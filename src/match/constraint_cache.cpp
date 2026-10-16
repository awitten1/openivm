#include "match/constraint_cache.hpp"

#include "core/openivm_constants.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {
namespace openivm {

vector<CachedConstraint> ConstraintCache::GetConstraints(ClientContext &context, const string &table_name) {
	vector<CachedConstraint> constraints;
	Connection con(*context.db);
	RefreshMetadata::UseSameLocation(context, con);
	auto result = con.Query("SELECT table_name, constraint_kind, columns_json, referenced_table, "
	                        "referenced_columns_json, is_trusted FROM " +
	                        string(CONSTRAINTS_CACHE_TABLE));
	if (result->HasError()) {
		return constraints;
	}
	for (idx_t r = 0; r < result->RowCount(); r++) {
		string stored_table = result->GetValue(0, r).ToString();
		if (!StringUtil::CIEquals(stored_table, table_name)) {
			continue;
		}
		CachedConstraint c;
		c.table_name = stored_table;
		c.kind = result->GetValue(1, r).ToString();
		c.columns = SqlUtils::ParseColumnList(result->GetValue(2, r).ToString());
		if (!result->GetValue(3, r).IsNull()) {
			c.referenced_table = result->GetValue(3, r).ToString();
		}
		if (!result->GetValue(4, r).IsNull()) {
			c.referenced_columns = SqlUtils::ParseColumnList(result->GetValue(4, r).ToString());
		}
		c.is_trusted = result->GetValue(5, r).IsNull() || result->GetValue(5, r).GetValue<bool>();
		if (!c.columns.empty()) {
			constraints.push_back(std::move(c));
		}
	}

	return constraints;
}

} // namespace openivm
} // namespace duckdb
