#include "core/parser.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "lpts_parser.hpp"
#include "lpts_sql_scanner.hpp"

#include <regex>

namespace duckdb {

void SetOpenIvmInputDialect(ClientContext &context, SetScope scope, Value &parameter) {
	auto dialect = parameter.IsNull() ? SqlDialect::DUCKDB
	                                  : ParseSqlDialectSetting(parameter.ToString(), OPENIVM_INPUT_DIALECT_SETTING);
	// `parser_override` and `parse_function` are handed the extension info, not a ClientContext, so
	// mirror the resolved dialect there. The info is owned by the DBConfig, so the mirror stays
	// scoped to this database.
	for (auto &extension : ExtensionCallbackManager::Get(context).ParserExtensions()) {
		auto info = dynamic_cast<MaterializedViewParserExtensionInfo *>(extension.parser_info.get());
		if (info) {
			info->SetInputDialect(dialect);
		}
	}
}

static SqlDialect InputDialectFromInfo(ParserExtensionInfo *info) {
	auto materialized_view_info = dynamic_cast<MaterializedViewParserExtensionInfo *>(info);
	return materialized_view_info ? materialized_view_info->InputDialect() : SqlDialect::DUCKDB;
}

ParserOverrideResult MaterializedViewParserExtension::OverrideFunction(ParserExtensionInfo *info, const string &query,
                                                                       ParserOptions &options) {
	try {
		ParserOptions native_options = options;
		native_options.extensions = nullptr;
		vector<unique_ptr<SQLStatement>> statements;
		bool overridden = false;
		// Return lifecycle ASTs, not pragmas: DuckDB preprocesses all pragmas before
		// executing any native statement in this input. Extension statements bind
		// only when the driver reaches them, after preceding DDL/DML/BEGIN executes.
		for (auto &sql : SqlUtils::SplitSQLStatements(query)) {
			auto extension_result = ParseMaterializedViewStatement(sql, InputDialectFromInfo(info));
			if (extension_result.type == ParserExtensionResultType::PARSE_SUCCESSFUL) {
				auto extension = MaterializedViewParserExtension();
				auto statement =
				    make_uniq<ExtensionStatement>(std::move(extension), std::move(extension_result.parse_data));
				statement->query = sql;
				statements.push_back(std::move(statement));
				overridden = true;
				continue;
			}
			Parser parser(native_options);
			parser.ParseQuery(sql);
			for (auto &statement : parser.statements) {
				statements.push_back(std::move(statement));
			}
		}
		return overridden ? ParserOverrideResult(std::move(statements)) : ParserOverrideResult();
	} catch (std::exception &ex) {
		return ParserOverrideResult(ex);
	}
}

ParserExtensionParseResult MaterializedViewParserExtension::ParseFunction(ParserExtensionInfo *info,
                                                                          const string &query) {
	return ParseMaterializedViewStatement(query, InputDialectFromInfo(info));
}

ParserExtensionParseResult ParseMaterializedViewStatement(const string &query, SqlDialect input_dialect) {
	// Mask comments only in the lifecycle header. The AS body must reach the
	// native parser unchanged: removing a block comment between adjacent string
	// literals can turn invalid SQL into a valid concatenated literal.
	auto cleaned = query;
	string query_body;
	auto tokens = Parser::Tokenize(query);
	idx_t cursor = 0;
	for (idx_t i = 0; i <= tokens.size(); i++) {
		auto end = i < tokens.size() ? tokens[i].start : query.size();
		auto token = i > 0 ? &tokens[i - 1] : nullptr;
		bool string_constant = token && token->type == SimplifiedTokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT;
		bool escaped = string_constant && (query[token->start] == 'e' || query[token->start] == 'E');
		if (string_constant && query[cursor] == '$') {
			auto tag_end = query.find('$', cursor + 1);
			auto tag = query.substr(cursor, tag_end - cursor + 1);
			auto close = query.find(tag, tag_end + 1);
			cursor = close == string::npos ? query.size() : close + tag.size();
		}
		while (cursor < end) {
			if (query[cursor] == '\'' || query[cursor] == '"') {
				auto quote = query[cursor++];
				while (cursor < end) {
					auto c = query[cursor++];
					if (escaped && c == '\\' && cursor < end) {
						cursor++;
					} else if (c == quote) {
						if (cursor < end && query[cursor] == quote) {
							cursor++;
						} else {
							break;
						}
					}
				}
				continue;
			}
			auto comment_start = cursor;
			if (cursor + 1 < end && query[cursor] == '-' && query[cursor + 1] == '-') {
				while (cursor < end && query[cursor] != '\n' && query[cursor] != '\r') {
					cursor++;
				}
			} else if (cursor + 1 < end && query[cursor] == '/' && query[cursor + 1] == '*') {
				cursor += 2;
				idx_t depth = 1;
				while (cursor < end && depth > 0) {
					if (cursor + 1 < end && query[cursor] == '/' && query[cursor + 1] == '*') {
						depth++;
						cursor += 2;
					} else if (cursor + 1 < end && query[cursor] == '*' && query[cursor + 1] == '/') {
						depth--;
						cursor += 2;
					} else {
						cursor++;
					}
				}
			} else {
				cursor++;
				continue;
			}
			for (auto pos = comment_start; pos < cursor; pos++) {
				if (cleaned[pos] != '\n' && cleaned[pos] != '\r') {
					cleaned[pos] = ' ';
				}
			}
		}
		if (token && token->type == SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR && query[token->start] == ';') {
			cleaned[token->start] = ' ';
		}
		if (token && token->type == SimplifiedTokenType::SIMPLIFIED_TOKEN_KEYWORD &&
		    MatchesKeywordAt(cleaned, token->start, "as")) {
			query_body = query.substr(token->start + 2);
			cleaned.resize(token->start + 2);
			break;
		}
	}
	auto query_lower = SqlUtils::SQLToLowercase(cleaned);
	StringUtil::Trim(query_lower);
	// Collapse whitespace only for keyword detection, never in the AS query.
	auto header = query_lower;
	SqlUtils::RemoveRedundantWhitespaces(header);

	// Handle ALTER MATERIALIZED VIEW <name> SET REFRESH EVERY '<interval>' | SET REFRESH MANUAL
	if (StringUtil::StartsWith(header, "alter materialized view ")) {
		const string identifier = "(?:\"(?:[^\"]|\"\")*\"|[a-zA-Z_][a-zA-Z0-9_$]*)";
		const string qualified_identifier = identifier + "(?:\\s*\\.\\s*" + identifier + "){0,2}";
		std::regex alter_re("^alter\\s+materialized\\s+view\\s+(" + qualified_identifier +
		                        ")\\s+set\\s+refresh\\s+(every\\s+'([^']+)'|manual)$",
		                    std::regex::icase);
		std::smatch match;
		if (!std::regex_match(query_lower, match, alter_re)) {
			throw ParserException("Invalid ALTER MATERIALIZED VIEW syntax. "
			                      "Expected: ALTER MATERIALIZED VIEW <name> SET REFRESH EVERY '<interval>' "
			                      "or ALTER MATERIALIZED VIEW <name> SET REFRESH MANUAL");
		}
		string alter_view_name = match[1].str();
		auto name_components = SqlUtils::ParseQualifiedIdentifier(alter_view_name);
		if (name_components.empty()) {
			throw ParserException("Invalid materialized-view target '%s'", alter_view_name);
		}
		string refresh_type = StringUtil::Lower(match[2].str());
		string alter_value;
		if (refresh_type == "manual") {
			alter_value = "NULL";
		} else {
			int64_t interval = SqlUtils::ParseRefreshInterval(match[3].str());
			alter_value = to_string(interval);
		}
		// Pass the UPDATE SQL through MaterializedViewParseData; PlanFunction will execute it
		Parser alter_parser;
		alter_parser.ParseQuery("SELECT 1");
		auto parse_data =
		    make_uniq_base<ParserExtensionParseData, MaterializedViewParseData>(std::move(alter_parser.statements[0]));
		auto &materialized_view_data = dynamic_cast<MaterializedViewParseData &>(*parse_data);
		materialized_view_data.alter_sql = alter_value;
		materialized_view_data.target_name = alter_view_name;
		materialized_view_data.original_query = query;
		return ParserExtensionParseResult(std::move(parse_data));
	}

	if (!StringUtil::StartsWith(header, "create materialized view ") &&
	    !StringUtil::StartsWith(header, "create or replace materialized view ")) {
		return ParserExtensionParseResult();
	}

	OPENIVM_DEBUG_PRINT("[CREATE MV] Intercepted query: %s\n", query_lower.c_str());

	bool is_replace = StringUtil::StartsWith(header, "create or replace materialized view ");
	// Extract REFRESH EVERY clause before structural rewrite (strips it from the header)
	int64_t refresh_interval = SqlUtils::ExtractRefreshInterval(query_lower);
	OPENIVM_DEBUG_PRINT("[CREATE MV] Refresh interval: %lld seconds\n", (long long)refresh_interval);

	// Plan the view as CREATE TABLE AS. Rewrite only the header keywords; quoted names can
	// contain the same words. LPTS owns input syntax; the parsed AT clause carries the snapshot.
	idx_t name_start = 0;
	for (auto keyword : {"create", "or", "replace", "materialized", "view"}) {
		name_start = SkipWhitespace(query_lower, name_start);
		if (MatchesKeywordAt(query_lower, name_start, keyword)) {
			name_start += strlen(keyword);
		}
	}
	auto create_sql = NormalizeInputSqlToDuckDB(
	    "create table if not exists" + query_lower.substr(name_start) + query_body, input_dialect);
	auto as_pos = FindTopLevelAs(create_sql);
	auto view_query = as_pos == DConstants::INVALID_INDEX ? string() : create_sql.substr(as_pos + 2);
	// All other rewrites (DISTINCT, AVG, LEFT JOIN key, aggregate aliases) are done
	// at the plan level in PlanFunction via PlanRewrite + LPTS.
	OPENIVM_DEBUG_PRINT("[CREATE MV] After structural rewrite: %s\n", create_sql.c_str());

	Parser p;
	p.ParseQuery(create_sql);
	if (p.statements.size() != 1 || p.statements[0]->type != StatementType::CREATE_STATEMENT) {
		throw ParserException("Invalid CREATE MATERIALIZED VIEW statement");
	}
	auto &info = p.statements[0]->Cast<CreateStatement>().info->Cast<CreateTableInfo>();
	vector<string> target_parts;
	for (auto &part : {info.catalog, info.schema, info.table}) {
		if (!part.empty()) {
			target_parts.push_back(SqlUtils::QuoteIdentifier(part));
		}
	}

	auto parse_data = make_uniq_base<ParserExtensionParseData, MaterializedViewParseData>(std::move(p.statements[0]),
	                                                                                      refresh_interval);
	auto &materialized_view_data = dynamic_cast<MaterializedViewParseData &>(*parse_data);
	materialized_view_data.is_replace = is_replace;
	materialized_view_data.target_name = StringUtil::Join(target_parts, ".");
	materialized_view_data.view_query = std::move(view_query);
	materialized_view_data.original_query = query;
	return ParserExtensionParseResult(std::move(parse_data));
}

} // namespace duckdb
