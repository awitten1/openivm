#include "duckdb.hpp"
#include "core/parser.hpp"
#include "core/parser_create_mv_helpers.hpp"
#include "core/parser_ddl.hpp"
#include "core/published_view.hpp"
#include "compile_facts.hpp"
#include "core/refresh_locks.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "rules/column_hider.hpp"

#include <chrono>
#include <future>
#include <iostream>
#include <thread>

namespace duckdb {

// Observe the actual gate wait, rather than assuming a launched worker reached
// DROP's ownership lookup. The deadline only bounds a broken test's wait.
class MutationGateTestAccess {
public:
	static void WaitForBlockedOwner(MutationGate &gate) {
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
		while (std::chrono::steady_clock::now() < deadline) {
			{
				std::lock_guard<mutex> guard(gate.lock);
				if (gate.waiting_owners != 0) {
					return;
				}
			}
			std::this_thread::yield();
		}
		throw InvalidInputException("Competing DROP did not wait on the mutation gate");
	}
};

} // namespace duckdb

using namespace duckdb;

static void Execute(Connection &con, const string &sql) {
	// Flush before execution so a coordinator timeout identifies the blocked
	// operation even when the query never returns an error or result.
	std::cerr << "START " << SqlUtils::SQLStatementPreview(sql) << std::endl;
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw InvalidInputException("Lifecycle SQL failed: %s\nSQL: %s", result->GetError(), sql);
	}
	std::cerr << "DONE " << SqlUtils::SQLStatementPreview(sql) << std::endl;
}

static void TestQuotedNamespaceNormalization() {
	const string sql = "CREATE MATERIALIZED VIEW \"Peer'Catalog\".\"Schema\"\"Name\".\"View.Name\" "
	                   "AS SELECT 'String''Value' AS \"Column'Name\";";
	const string expected = "create materialized view \"Peer'Catalog\".\"Schema\"\"Name\".\"View.Name\" "
	                        "as select 'String''Value' as \"Column'Name\";";
	if (SqlUtils::SQLToLowercase(sql) != expected) {
		throw InvalidInputException("CREATE normalization changed a quoted namespace or literal");
	}
}

static void TestQualifiedTableReferences() {
	// These two physical identities flatten to the same text if quoted dots are
	// mistaken for qualification. Only the maintenance table may receive a pin.
	const string data_table = "\"Lake\".\"Internal.Space\".\"maintenance\"";
	const string other_table = "\"Lake.Internal\".\"Space\".\"maintenance\"";
	const string reference = "lake.\"Internal.Space\".maintenance";
	const string query = "SELECT * FROM " + reference + " JOIN " + other_table + " USING(i)";
	const string pinned_table = data_table + " AT (VERSION => 7)";
	const string expected = "SELECT * FROM " + pinned_table + " JOIN " + other_table + " USING(i)";
	if (SqlUtils::FindTableReference(query, data_table) != reference ||
	    SqlUtils::CountTableReferences(query, data_table) != 1 ||
	    SqlUtils::ReplaceTableReferences(query, data_table, pinned_table) != expected ||
	    SqlUtils::ReplaceTableReferences(expected, data_table, pinned_table) != expected) {
		throw InvalidInputException("Publication pinning lost quoted catalog/schema identity");
	}
	const string escaped_table = SqlUtils::FullName("Lake\"Catalog", "Internal Space", "maintenance");
	const string escaped_query = "SELECT * FROM " + escaped_table;
	if (SqlUtils::ReplaceTableReferences(escaped_query, escaped_table, pinned_table) !=
	    "SELECT * FROM " + pinned_table) {
		throw InvalidInputException("Publication pinning did not match escaped identifiers");
	}
	// Unqualified names still match a qualified reference. Catalog callers quote
	// bare names containing punctuation before passing them to the SQL matcher.
	if (SqlUtils::FindTableReference(query, "maintenance") != reference ||
	    SqlUtils::FindTableReference("SELECT * FROM lake.\"Internal Space\".\"Table Name\"",
	                                 SqlUtils::QuoteIdentifier("Table Name")) !=
	        "lake.\"Internal Space\".\"Table Name\"") {
		throw InvalidInputException("Table-reference matching lost unqualified table names");
	}
	const string dotted_query = "SELECT * FROM lake.main.\"Table.Name\"";
	if (SqlUtils::FindTableReference(dotted_query, SqlUtils::QuoteIdentifier("Table.Name")) !=
	        "lake.main.\"Table.Name\"" ||
	    !SqlUtils::FindTableReference(dotted_query, "Table.Name").empty()) {
		throw InvalidInputException("Table-reference matching confused a quoted table name with qualification");
	}
}

// Unlike SQLLogicTest's restart, separate DuckDB instances with no supplied
// DBConfig cannot carry extension settings across the close/reopen boundary.
static void TestInternalSchemaReopen(const string &path) {
	std::cerr << "CASE internal schema reopen with fresh configuration" << std::endl;
	{
		DuckDB db(path);
		Connection owner(db);
		Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; "
		               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
		               "CREATE SCHEMA \"Source.Space\"; CREATE TABLE \"Source.Space\".items(i INTEGER); "
		               "INSERT INTO \"Source.Space\".items VALUES (1),(1),(2); "
		               "CREATE MATERIALIZED VIEW \"Source.Space\".original AS SELECT * FROM \"Source.Space\".items;");
	}
	{
		DuckDB db(path);
		Connection owner(db);
		Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true;");
		Execute(owner, "SELECT CASE WHEN current_setting('openivm_internal_schema')='' "
		               "AND current_setting('openivm_metadata_schema')='' "
		               "THEN true ELSE error('Reopen persisted placement settings') END;");
		Execute(owner,
		        "SET search_path='\"Control.Space\"'; "
		        "CREATE MATERIALIZED VIEW \"Source.Space\".after_reopen AS SELECT * FROM \"Source.Space\".items; "
		        "CREATE OR REPLACE MATERIALIZED VIEW \"Source.Space\".original "
		        "AS SELECT * FROM \"Source.Space\".items;");
		Execute(owner, "SELECT CASE WHEN "
		               "(SELECT count(*) FROM \"Control.Space\".openivm_views "
		               "WHERE view_sql_name='after_reopen' AND coalesce(internal_schema,'')='')=1 "
		               "AND (SELECT count(*) FROM \"Control.Space\".openivm_views "
		               "WHERE view_sql_name='original' AND internal_schema='Internal Space')=1 "
		               "AND (SELECT count(*) FROM duckdb_tables() t JOIN \"Control.Space\".openivm_views v "
		               "ON t.table_name='openivm_data_' || v.view_name "
		               "WHERE v.view_sql_name='after_reopen' AND t.schema_name='Source.Space')=1 "
		               "AND (SELECT count(*) FROM duckdb_tables() t JOIN \"Control.Space\".openivm_views v "
		               "ON t.table_name='openivm_data_' || v.view_name "
		               "WHERE v.view_sql_name='original' AND t.schema_name='Internal Space')=1 "
		               "AND (SELECT count(*) FROM \"Control.Space\".openivm_delta_tables "
		               "WHERE source_schema='Source.Space' AND delta_schema='Internal Space')=2 "
		               "AND (SELECT count(DISTINCT delta_table_name) FROM \"Control.Space\".openivm_delta_tables "
		               "WHERE source_schema='Source.Space')=1 "
		               "THEN true ELSE error('Reopen changed internal placement or duplicated a source delta') END;");
		Execute(owner, "INSERT INTO \"Source.Space\".items VALUES (1),(2),(3),(3); "
		               "DELETE FROM \"Source.Space\".items WHERE i=1; "
		               "UPDATE \"Source.Space\".items SET i=4 WHERE i=2; "
		               "INSERT INTO \"Source.Space\".items VALUES (1),(4),(5); "
		               "DELETE FROM \"Source.Space\".items WHERE i=5;");
		for (const auto &view : vector<string> {"original", "after_reopen"}) {
			auto qualified = SqlUtils::QuoteIdentifier("Source.Space") + "." + SqlUtils::QuoteIdentifier(view);
			Execute(owner, "PRAGMA refresh(" + Value(qualified).ToSQLString() + ");");
			Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + qualified +
			                   " EXCEPT ALL SELECT * FROM \"Source.Space\".items) "
			                   "OR EXISTS (SELECT * FROM \"Source.Space\".items EXCEPT ALL SELECT * FROM " +
			                   qualified + ") THEN error('Reopened MV bag mismatch') ELSE true END;");
		}
		Execute(owner, "DROP VIEW \"Source.Space\".original; DROP VIEW \"Source.Space\".after_reopen;");
		Execute(owner, "SELECT CASE WHEN "
		               "NOT EXISTS (SELECT 1 FROM \"Control.Space\".openivm_views) "
		               "AND NOT EXISTS (SELECT 1 FROM \"Control.Space\".openivm_delta_tables) "
		               "AND NOT EXISTS (SELECT 1 FROM duckdb_tables() "
		               "WHERE schema_name IN ('Source.Space','Internal Space') AND starts_with(table_name,'openivm_')) "
		               "THEN true ELSE error('Reopened namespace cleanup leaked') END;");
	}
}

static void TestPublication(bool explicit_transaction) {
	std::cerr << "CASE publication explicit_transaction=" << explicit_transaction << std::endl;
	DuckDB db(nullptr);
	Connection first(db), second(db), observer(db);
	Execute(first, "LOAD openivm; SET openivm_disable_daemon=true; "
	               "SET openivm_internal_schema='Internal Space'; "
	               "CREATE TABLE source(i INTEGER); INSERT INTO source VALUES (1), (1), (2);");
	Execute(second, "SET openivm_internal_schema='Internal Space';");
	// Stop exactly after planning has committed the physical source delta,
	// before the first view's ownership metadata is published.
	first.context->transaction.BeginTransaction();
	auto parsed = ParseMaterializedViewStatement("CREATE MATERIALIZED VIEW first_mv AS SELECT * FROM source;",
	                                             SqlDialect::DUCKDB);
	auto planned = MaterializedViewParserExtension::PlanFunction(nullptr, *first.context, std::move(parsed.parse_data));
	Execute(observer, "SELECT CASE WHEN (SELECT count(*) FROM duckdb_tables() WHERE schema_name='Internal Space' "
	                  "AND table_name='openivm_delta_6d61696e_source')=1 "
	                  "AND (SELECT count(*) FROM openivm_delta_tables)=0 "
	                  "THEN true ELSE error('Did not reach the source allocation/publication interval') END;");
	// Check exclusion synchronously, without relying on worker scheduling or sleeps.
	auto gate = RefreshLocks::AcquireGate(DatabaseInstance::GetDatabase(*first.context));
	auto second_owner = TransactionalMVLockState::Get(*second.context).GetMutationOwner();
	if (gate->TryLock(second_owner)) {
		gate->Unlock(second_owner);
		throw InvalidInputException("Source allocation did not retain the mutation gate through publication");
	}
	if (explicit_transaction) {
		// Establish a metadata snapshot before first publishes its ownership row.
		// Waiting for the mutation gate cannot advance this transaction's snapshot.
		Execute(second, "BEGIN;");
		Execute(second, "SELECT CASE WHEN count(*)=0 THEN true "
		                "ELSE error('Competing transaction already sees source ownership') END "
		                "FROM openivm_delta_tables;");
	}
	std::promise<void> started;
	auto ready = started.get_future();
	auto competing = std::async(std::launch::async, [&]() {
		started.set_value();
		Execute(second, "CREATE MATERIALIZED VIEW second_mv AS SELECT * FROM source;");
	});
	ready.get();
	try {
		ExecuteStagedDDL(*first.context, planned.parameters);
		first.context->transaction.Commit();
	} catch (const std::exception &) {
		if (first.context->transaction.HasActiveTransaction()) {
			first.context->transaction.Rollback(nullptr);
		}
		throw;
	}
	competing.get();
	if (explicit_transaction) {
		// Also retain transaction-local ownership when reconciling committed rows.
		Execute(second, "CREATE MATERIALIZED VIEW third_mv AS SELECT * FROM source; COMMIT;");
	}
	Execute(observer, "SELECT CASE WHEN (SELECT count(*) FROM duckdb_tables() WHERE schema_name='Internal Space' "
	                  "AND starts_with(table_name, 'openivm_delta_6d61696e_source'))=1 "
	                  "AND (SELECT count(DISTINCT delta_table_name) FROM openivm_delta_tables "
	                  "WHERE source_schema='main' AND table_name='openivm_delta_source')=1 "
	                  "THEN true ELSE error('Concurrent CREATEs allocated different source deltas') END;");
	Execute(observer, "INSERT INTO source VALUES (2),(3); DELETE FROM source WHERE i=1; "
	                  "UPDATE source SET i=4 WHERE i=2;");
	vector<string> views {"first_mv", "second_mv"};
	if (explicit_transaction) {
		views.push_back("third_mv");
	}
	for (const auto &view : views) {
		Execute(observer, "PRAGMA refresh('" + string(view) + "');");
		Execute(observer, "SELECT CASE WHEN EXISTS (SELECT * FROM " + string(view) +
		                      " EXCEPT ALL SELECT * FROM source) OR EXISTS (SELECT * FROM source EXCEPT ALL "
		                      "SELECT * FROM " +
		                      string(view) + ") THEN error('Concurrent MV bag mismatch') ELSE true END;");
	}
}

static void TestDropDuringPublication(bool explicit_transaction, bool staged_drop) {
	std::cerr << "CASE drop during publication explicit_transaction=" << explicit_transaction
	          << " staged_drop=" << staged_drop << std::endl;
	DuckDB db(nullptr);
	Connection creator(db), dropper(db), observer(db);
	Execute(creator, "LOAD openivm; SET openivm_disable_daemon=true; "
	                 "SET openivm_internal_schema='Internal Space'; "
	                 "CREATE TABLE source(i INTEGER); INSERT INTO source VALUES (1),(1),(2);");
	string dropped_view = "first_mv";
	if (staged_drop) {
		// A view in another native catalog exercises staged cleanup without a
		// DuckLake install: its shared source delta is in the host catalog.
		Execute(creator, "ATTACH ':memory:' AS other;");
		dropped_view = "other.main.first_mv";
	}
	Execute(creator, "CREATE MATERIALIZED VIEW " + dropped_view + " AS SELECT * FROM source;");
	creator.context->transaction.BeginTransaction();
	auto parsed = ParseMaterializedViewStatement("CREATE MATERIALIZED VIEW second_mv AS SELECT * FROM source;",
	                                             SqlDialect::DUCKDB);
	auto planned =
	    MaterializedViewParserExtension::PlanFunction(nullptr, *creator.context, std::move(parsed.parse_data));
	if (explicit_transaction) {
		Execute(dropper, "BEGIN;");
	}
	auto gate = RefreshLocks::AcquireGate(DatabaseInstance::GetDatabase(*creator.context));
	auto competing = std::async(std::launch::async, [&]() { Execute(dropper, "DROP VIEW " + dropped_view + ";"); });
	try {
		// Old DROP reaches this wait only AFTER deciding to delete the delta;
		// corrected DROP waits BEFORE reading any ownership metadata.
		MutationGateTestAccess::WaitForBlockedOwner(*gate);
		ExecuteStagedDDL(*creator.context, planned.parameters);
		creator.context->transaction.Commit();
	} catch (const std::exception &) {
		if (creator.context->transaction.HasActiveTransaction()) {
			creator.context->transaction.Rollback(nullptr);
		}
		competing.wait();
		if (dropper.context->transaction.HasActiveTransaction()) {
			dropper.context->transaction.Rollback(nullptr);
		}
		throw;
	}
	competing.get();
	auto observer_owner = TransactionalMVLockState::Get(*observer.context).GetMutationOwner();
	if (explicit_transaction) {
		if (gate->TryLock(observer_owner)) {
			gate->Unlock(observer_owner);
			throw InvalidInputException("DROP released the mutation gate before its transaction committed");
		}
		Execute(dropper, "COMMIT;");
	}
	if (!gate->TryLock(observer_owner)) {
		throw InvalidInputException("DROP retained the mutation gate after execution committed");
	}
	gate->Unlock(observer_owner);
	Execute(observer, "SELECT CASE WHEN count(*)=1 THEN true "
	                  "ELSE error('DROP removed the surviving CREATE source delta') END FROM duckdb_tables() "
	                  "WHERE schema_name='Internal Space' AND table_name='openivm_delta_6d61696e_source';");
	Execute(observer, "INSERT INTO source VALUES (1),(2),(3),(3); DELETE FROM source WHERE i=1; "
	                  "UPDATE source SET i=4 WHERE i=2; INSERT INTO source VALUES (4),(5); "
	                  "DELETE FROM source WHERE i=5;");
	Execute(observer, "SELECT CASE WHEN count(*)>0 THEN true "
	                  "ELSE error('Surviving source delta did not capture batched DML') END "
	                  "FROM \"Internal Space\".openivm_delta_6d61696e_source;");
	Execute(observer, "PRAGMA refresh('second_mv');");
	Execute(observer, "SELECT CASE WHEN EXISTS (SELECT * FROM second_mv EXCEPT ALL SELECT * FROM source) "
	                  "OR EXISTS (SELECT * FROM source EXCEPT ALL SELECT * FROM second_mv) "
	                  "THEN error('CREATE versus DROP MV bag mismatch') ELSE true END;");
}

// Lookup must finish after schema enumeration releases its lock, and preserve
// source identity when metadata, source and physical delta have different schemas.
static void TestSourceDeltaLookup() {
	std::cerr << "CASE source delta lookup across schemas" << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Source.Space\"; CREATE TABLE main.items(i INTEGER); "
	               "CREATE TABLE \"Source.Space\".items(i INTEGER);");
	Execute(owner, "CREATE MATERIALIZED VIEW main_mv AS SELECT * FROM main.items;");
	Execute(owner, "CREATE MATERIALIZED VIEW quoted_mv AS SELECT * FROM \"Source.Space\".items;");
	owner.context->RunFunctionInTransaction([&]() {
		for (const auto &schema : vector<string> {"main", "Source.Space"}) {
			auto delta = RefreshMetadata::FindSourceDelta(*owner.context, "memory", schema, "items");
			if (!delta || delta->schema.name != "Internal Space" ||
			    delta->name != RefreshMetadata::InternalSourceDeltaName(schema, "items")) {
				throw InvalidInputException("Source delta lookup lost its qualified physical identity");
			}
		}
		if (RefreshMetadata::FindSourceDelta(*owner.context, "memory", "Control.Space", "items")) {
			throw InvalidInputException("Source delta lookup reused another schema's same-named source");
		}
	});
	const vector<pair<string, string>> views = {{"main_mv", "main.items"}, {"quoted_mv", "\"Source.Space\".items"}};
	for (auto &view : views) {
		Execute(owner, "INSERT INTO " + view.second + " VALUES (1),(1),(2),(3); DELETE FROM " + view.second +
		                   " WHERE i=1; UPDATE " + view.second + " SET i=4 WHERE i=2; INSERT INTO " + view.second +
		                   " VALUES (1),(4),(5); DELETE FROM " + view.second + " WHERE i=5;");
		Execute(owner, "PRAGMA refresh('" + view.first + "');");
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view.first + " EXCEPT ALL SELECT * FROM " +
		                   view.second + ") OR EXISTS (SELECT * FROM " + view.second + " EXCEPT ALL SELECT * FROM " +
		                   view.first + ") THEN error('Source delta lookup MV bag mismatch') ELSE true END;");
	}
}

// Exercise initialization as well as discovery: upgrading a committed registry
// must not pin the caller's catalog snapshot before the helper's DDL commits.
// Explicit transactions instead own the upgrade and new schemas until commit.
static void TestSharedMetadataUpgrade(bool explicit_transaction, bool commit) {
	std::cerr << "CASE shared metadata upgrade explicit_transaction=" << explicit_transaction << " commit=" << commit
	          << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Control.Space\"; CREATE TABLE source(i INTEGER); "
	               "INSERT INTO source VALUES (1),(1),(2);");
	auto registry = SqlUtils::FullName("memory", "Control.Space", openivm::VIEWS_TABLE);
	Execute(owner, CreateMetadataTableSQL(openivm::VIEWS_TABLE, registry) + "; ALTER TABLE " + registry +
	                   " DROP COLUMN internal_schema;");
	if (explicit_transaction) {
		Execute(owner, "BEGIN;");
	}
	Execute(owner, "CREATE MATERIALIZED VIEW mv AS SELECT * FROM source;");
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	if (!commit) {
		Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true "
		               "ELSE error('Rolled-back initialization retained its schema or column') END FROM ("
		               "SELECT 1 FROM duckdb_schemas() WHERE schema_name='Internal Space' UNION ALL "
		               "SELECT 1 FROM information_schema.columns WHERE table_schema='Control.Space' "
		               "AND table_name='openivm_views' AND column_name='internal_schema');");
		// The same old registry must also upgrade after rollback, without manual repair.
		Execute(owner, "CREATE MATERIALIZED VIEW mv AS SELECT * FROM source;");
	}
	Execute(owner, "INSERT INTO source VALUES (1),(2),(3),(3); DELETE FROM source WHERE i=1; "
	               "UPDATE source SET i=4 WHERE i=2; INSERT INTO source VALUES (1),(4),(5); "
	               "DELETE FROM source WHERE i=5;");
	Execute(owner, "PRAGMA refresh('mv');");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM mv EXCEPT ALL SELECT * FROM source) "
	               "OR EXISTS (SELECT * FROM source EXCEPT ALL SELECT * FROM mv) "
	               "THEN error('Upgraded registry MV bag mismatch') ELSE true END;");
	Execute(owner, "DROP VIEW mv;");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Upgraded registry cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

// Source planning changes the helper's search path without changing its selected
// metadata location. Probe both durable ownership and transaction-local shadows.
static void TestDeltaOwnershipSearchPath(bool replay) {
	std::cerr << "CASE delta ownership search path replay=" << replay << std::endl;
	DuckDB db(nullptr);
	Connection owner(db), helper(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Source.Space\"; CREATE TABLE \"Source.Space\".source(i INTEGER); "
	               "INSERT INTO \"Source.Space\".source VALUES (1),(1),(2); "
	               "CREATE MATERIALIZED VIEW mv AS SELECT * FROM \"Source.Space\".source; "
	               "ATTACH ':memory:' AS \"Probe.Peer\"; CREATE SCHEMA \"Probe.Peer\".\"Source.Space\";");
	RefreshMetadata::UseCatalog(*owner.context, helper, "memory");
	auto key = RefreshMetadata(helper).FindViewKey("memory", "main", "mv");
	auto sources = RefreshMetadata(helper).GetDeltaSources(key, "memory", "main");
	if (sources.size() != 1) {
		throw InvalidInputException("Ownership search-path fixture must have one source");
	}
	if (replay) {
		Execute(owner, "BEGIN; CREATE MATERIALIZED VIEW sibling AS SELECT * FROM \"Source.Space\".source;");
		TransactionalMVMetadataState::Get(*owner.context).Apply(helper);
	}
	Execute(helper, "USE \"Probe.Peer\".\"Source.Space\";");
	RefreshMetadata::DeltaSource view_delta;
	view_delta.table_name = SqlUtils::DeltaName(key);
	view_delta.catalog_name = "memory";
	view_delta.schema_name = "Internal Space";
	if (!RefreshMetadata(helper).IsMaterializedViewDelta(view_delta) ||
	    RefreshMetadata(helper).IsMaterializedViewDelta(sources[0])) {
		throw InvalidInputException("Delta owner lookup followed the source search path");
	}
	// Inspect this helper's selected registry directly, as commit-time cleanup does.
	// In the replay case only the shadow contains the uncommitted sibling.
	auto registry = RefreshMetadata::MetadataTable(helper, openivm::DELTA_TABLES_TABLE);
	if (!RefreshMetadata::HasSourceDeltaConsumers(helper, sources[0]) ||
	    RefreshMetadata::HasSourceDeltaConsumers(helper, sources[0], nullptr, {{registry, Value(key).ToSQLString()}}) !=
	        replay) {
		throw InvalidInputException("Delta consumer lookup lost durable or replay ownership");
	}
	Execute(helper, "SELECT CASE WHEN current_database()='Probe.Peer' AND current_schema()='Source.Space' "
	                "THEN true ELSE error('Ownership inspection changed the helper search path') END;");
	if (replay) {
		Execute(owner, "ROLLBACK;");
	}
	Execute(owner, "INSERT INTO \"Source.Space\".source VALUES (1),(2),(3),(3); "
	               "DELETE FROM \"Source.Space\".source WHERE i=1; "
	               "UPDATE \"Source.Space\".source SET i=4 WHERE i=2;");
	Execute(owner, "PRAGMA refresh('mv');");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM mv EXCEPT ALL SELECT * FROM \"Source.Space\".source) "
	               "OR EXISTS (SELECT * FROM \"Source.Space\".source EXCEPT ALL SELECT * FROM mv) "
	               "THEN error('Ownership search-path survivor bag mismatch') ELSE true END;");
	Execute(owner, "DROP VIEW mv;");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Ownership search-path cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

// Scratch DROP must bind after its CREATE, using the caller's temporary catalog
// even when a persistent same-named table and a separate metadata schema exist.
static void TestDeferredScratchDrop(bool explicit_transaction, bool commit, bool executor_drop) {
	std::cerr << "CASE deferred scratch drop explicit_transaction=" << explicit_transaction << " commit=" << commit
	          << " executor_drop=" << executor_drop << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm;");
	Execute(owner, "SET openivm_disable_daemon=true; SET openivm_metadata_schema='Control.Space'; "
	               "SET openivm_internal_schema='Internal Space'; CREATE TABLE source(i INTEGER); "
	               "INSERT INTO source VALUES (1),(1),(2); CREATE TABLE \"Scratch;Name\"(i INTEGER); "
	               "INSERT INTO \"Scratch;Name\" VALUES (99); CREATE VIEW \"Scratch;View\" AS SELECT 99 AS i;");
	Execute(owner, "CREATE MATERIALIZED VIEW mv AS SELECT * FROM source;");
	Execute(owner, "USE memory.\"Control.Space\";");
	// Round-trip each physical DROP plan, including the direct executor route.
	// Query verification would execute a SELECT with side effects repeatedly;
	// serializer verification alone exercises the actual failing bind boundary.
	ClientConfig::GetConfig(*owner.context).verify_serializer = true;
	if (explicit_transaction) {
		Execute(owner, "BEGIN;");
	}
	auto drop = executor_drop ? "SELECT * FROM openivm_execute_drop_table('', '', 'Scratch;Name', false, false); "
	                          : "DROP TABLE \"Scratch;Name\"; ";
	Execute(owner, "CREATE TEMP TABLE \"Scratch;Name\" AS SELECT 7 AS i; "
	               "INSERT INTO \"Scratch;Name\" VALUES (8); " +
	                   string(drop) +
	                   "SELECT CASE WHEN count(*)=0 THEN true "
	                   "ELSE error('Deferred scratch DROP lost temporary identity') END "
	                   "FROM duckdb_tables() WHERE temporary AND table_name='Scratch;Name';");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM main.\"Scratch;Name\" EXCEPT ALL SELECT 99) "
	               "OR EXISTS (SELECT 99 EXCEPT ALL SELECT * FROM main.\"Scratch;Name\") "
	               "THEN error('Scratch DROP removed the persistent namesake') ELSE true END;");
	// Exercise the same resolved-target round trip for VIEW, with a persistent
	// namesake outside the active search path just as for TABLE above.
	auto view_drop = executor_drop ? "SELECT * FROM openivm_execute_drop_view('', '', 'Scratch;View', false, false); "
	                               : "DROP VIEW \"Scratch;View\"; ";
	Execute(owner, "CREATE TEMP VIEW \"Scratch;View\" AS SELECT 7 AS i; " + string(view_drop) +
	                   "SELECT CASE WHEN count(*)=0 THEN true "
	                   "ELSE error('Deferred scratch VIEW DROP lost temporary identity') END "
	                   "FROM duckdb_views() WHERE temporary AND view_name='Scratch;View';");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM main.\"Scratch;View\" EXCEPT ALL SELECT 99) "
	               "OR EXISTS (SELECT 99 EXCEPT ALL SELECT * FROM main.\"Scratch;View\") "
	               "THEN error('Scratch VIEW DROP removed the persistent namesake') ELSE true END;");
	Execute(owner, "DROP TABLE IF EXISTS \"Missing;Scratch\"; SELECT 1;");
	Execute(owner, "SELECT * FROM openivm_execute_drop_table('', '', 'Missing;Scratch', false, true);");
	Execute(owner, "SELECT * FROM openivm_execute_drop_view('', '', 'Missing;Scratch', false, true);");
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	Execute(owner, "INSERT INTO main.source VALUES (1),(2),(3),(3); DELETE FROM main.source WHERE i=1; "
	               "UPDATE main.source SET i=4 WHERE i=2; INSERT INTO main.source VALUES (1),(4),(5); "
	               "DELETE FROM main.source WHERE i=5;");
	Execute(owner, "PRAGMA refresh('main.mv');");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM main.mv EXCEPT ALL SELECT * FROM main.source) "
	               "OR EXISTS (SELECT * FROM main.source EXCEPT ALL SELECT * FROM main.mv) "
	               "THEN error('Deferred scratch DROP publication bag mismatch') ELSE true END;");
	Execute(owner, "DROP VIEW main.mv; SELECT 1;");
}

// Replay on a reused helper must follow catalog/schema identity, including when
// CREATE and CASCADE have both contributed mutations to the same transaction.
static void TestMetadataReplayLocations() {
	std::cerr << "CASE metadata replay locations" << std::endl;
	DuckDB db(nullptr);
	Connection owner(db), helper(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; "
	               "SET openivm_metadata_schema='Control.Space'; "
	               "SET openivm_internal_schema='Internal Space'; "
	               "CREATE TABLE a(i INTEGER); CREATE TABLE b(i INTEGER); "
	               "INSERT INTO a VALUES (1),(1),(2); INSERT INTO b VALUES (1),(1),(2); "
	               "CREATE MATERIALIZED VIEW mv1 AS SELECT a.i FROM a JOIN b USING(i); "
	               "CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM b; "
	               "ATTACH ':memory:' AS peer; CREATE TABLE peer.main.b(i INTEGER); "
	               "INSERT INTO peer.main.b VALUES (1),(1),(2); "
	               "CREATE MATERIALIZED VIEW peer.main.mv2 AS SELECT * FROM peer.main.b;");
	Execute(owner, "BEGIN; CREATE MATERIALIZED VIEW local_mv AS SELECT * FROM b; DROP TABLE a CASCADE;");
	auto &state = TransactionalMVMetadataState::Get(*owner.context);
	RefreshMetadata::UseCatalog(*owner.context, helper);
	auto primary = RefreshMetadata::CurrentLocation(helper).catalog_name;
	state.Apply(helper);
	Execute(helper, "SELECT CASE WHEN (SELECT count(*) FROM openivm_views WHERE view_sql_name='mv1')=0 "
	                "AND (SELECT count(*) FROM openivm_views WHERE view_sql_name='local_mv')=1 "
	                "AND (SELECT count(*) FROM openivm_views WHERE view_sql_name='mv2')=1 "
	                "THEN true ELSE error('Primary metadata replay lost CREATE or CASCADE ownership') END;");
	RefreshMetadata::UseCatalog(*owner.context, helper, "peer");
	state.Apply(helper);
	Execute(helper, "SELECT CASE WHEN count(*)=1 AND min(view_catalog)='peer' "
	                "THEN true ELSE error('Metadata replay leaked across catalogs') END FROM openivm_views;");
	RefreshMetadata::UseCatalog(*owner.context, helper, primary);
	state.Apply(helper);
	Execute(helper, "SELECT CASE WHEN (SELECT count(*) FROM openivm_views WHERE view_sql_name='mv1')=0 "
	                "AND (SELECT count(*) FROM openivm_views WHERE view_sql_name='local_mv')=1 "
	                "THEN true ELSE error('Metadata replay retained the previous catalog shadow') END;");
	Execute(owner, "ROLLBACK;");
	// Both catalogs' surviving views must still capture conflicting batches.
	for (const auto &source : vector<string> {"a", "b", "peer.main.b"}) {
		Execute(owner, "INSERT INTO " + source + " VALUES (1),(2),(3),(3); DELETE FROM " + source +
		                   " WHERE i=1; UPDATE " + source + " SET i=4 WHERE i=2; INSERT INTO " + source +
		                   " VALUES (1),(4),(5); DELETE FROM " + source + " WHERE i=5;");
	}
	const vector<pair<string, string>> views = {
	    {"mv1", "SELECT a.i FROM a JOIN b USING(i)"},
	    {"mv2", "SELECT * FROM b"},
	    {"peer.main.mv2", "SELECT * FROM peer.main.b"},
	};
	for (const auto &view : views) {
		Execute(owner, "PRAGMA refresh('" + view.first + "');");
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view.first + " EXCEPT ALL " + view.second +
		                   ") OR EXISTS (" + view.second + " EXCEPT ALL SELECT * FROM " + view.first +
		                   ") THEN error('Catalog replay rollback MV bag mismatch') ELSE true END;");
	}
}

static void TestCrossCatalogSharedDelta(bool dedicated_schema, bool commit) {
	std::cerr << "CASE cross-catalog shared delta dedicated_schema=" << dedicated_schema << " commit=" << commit
	          << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema=" +
	                   Value(dedicated_schema ? "Internal Space" : "").ToSQLString() + ";");
	Execute(owner, "CREATE SCHEMA \"Source.Space\"; CREATE TABLE \"Source.Space\".a(i INTEGER); "
	               "CREATE TABLE \"Source.Space\".b(i INTEGER); "
	               "INSERT INTO \"Source.Space\".a VALUES (1),(1),(2); "
	               "INSERT INTO \"Source.Space\".b VALUES (3),(3),(4); "
	               "CREATE MATERIALIZED VIEW main.sentinel_a AS SELECT * FROM \"Source.Space\".a; "
	               "CREATE MATERIALIZED VIEW main.sentinel_b AS SELECT * FROM \"Source.Space\".b; "
	               "ATTACH ':memory:' AS \"Consumer.Catalog\"; "
	               "CREATE MATERIALIZED VIEW \"Consumer.Catalog\".main.mv AS SELECT * FROM \"Source.Space\".b;");
	// Leave nonempty shared deltas before the new initial loads. Their watermarks
	// must still come from the caller's visible rows when source DDL is omitted.
	for (const auto &source : vector<string> {"a", "b"}) {
		auto source_table = SqlUtils::FullName("memory", "Source.Space", source);
		Execute(owner, "INSERT INTO " + source_table + " VALUES (1),(2),(3),(3); DELETE FROM " + source_table +
		                   " WHERE i=1; UPDATE " + source_table + " SET i=4 WHERE i=2;");
	}
	Execute(owner, "USE memory.\"Control.Space\"; BEGIN; "
	               "CREATE OR REPLACE MATERIALIZED VIEW \"Consumer.Catalog\".main.mv "
	               "AS SELECT * FROM \"Source.Space\".a; "
	               "CREATE MATERIALIZED VIEW \"Consumer.Catalog\".main.new_mv AS SELECT * FROM \"Source.Space\".a;");
	for (const auto &view : vector<string> {"mv", "new_mv"}) {
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM \"Consumer.Catalog\".main." + view +
		                   " EXCEPT ALL SELECT * FROM \"Source.Space\".a) OR EXISTS "
		                   "(SELECT * FROM \"Source.Space\".a EXCEPT ALL SELECT * FROM \"Consumer.Catalog\".main." +
		                   view + ") THEN error('Cross-catalog initial load bag mismatch') ELSE true END;");
	}
	Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	Execute(owner, "SELECT CASE WHEN count(*)=" + string(commit ? "1" : "0") +
	                   " THEN true ELSE error('Cross-catalog CREATE transaction outcome mismatch') END "
	                   "FROM duckdb_views() WHERE database_name='Consumer.Catalog' AND schema_name='main' "
	                   "AND view_name='new_mv';");
	for (const auto &source : vector<string> {"a", "b"}) {
		auto source_table = SqlUtils::FullName("memory", "Source.Space", source);
		Execute(owner, "INSERT INTO " + source_table + " VALUES (4),(5),(5),(6); DELETE FROM " + source_table +
		                   " WHERE i=4; UPDATE " + source_table + " SET i=7 WHERE i=5; INSERT INTO " + source_table +
		                   " VALUES (4),(7); DELETE FROM " + source_table + " WHERE i=6;");
	}
	vector<pair<string, string>> survivors = {
	    {"main.sentinel_a", "a"},
	    {"main.sentinel_b", "b"},
	    {"\"Consumer.Catalog\".main.mv", commit ? "a" : "b"},
	};
	if (commit) {
		survivors.emplace_back("\"Consumer.Catalog\".main.new_mv", "a");
	}
	for (const auto &view : survivors) {
		Execute(owner, "PRAGMA refresh(" + Value(view.first).ToSQLString() + ");");
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view.first +
		                   " EXCEPT ALL SELECT * FROM \"Source.Space\"." + view.second +
		                   ") OR EXISTS (SELECT * FROM \"Source.Space\"." + view.second + " EXCEPT ALL SELECT * FROM " +
		                   view.first +
		                   ") THEN error('Cross-catalog shared delta refresh bag mismatch') ELSE true END;");
	}
	for (const auto &view : survivors) {
		Execute(owner, "DROP VIEW " + view.first + ";");
	}
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Cross-catalog final cleanup leaked') END "
	               "FROM duckdb_tables() WHERE database_name='memory' "
	               "AND schema_name IN ('Source.Space','Internal Space') AND starts_with(table_name,'openivm_');");
}

static void RefreshAndCheckBag(Connection &owner, const string &view, const string &query) {
	Execute(owner, "PRAGMA refresh(" + Value(view).ToSQLString() + ");");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view + " EXCEPT ALL " + query + ") OR EXISTS (" + query +
	                   " EXCEPT ALL SELECT * FROM " + view +
	                   ") THEN error('Publication owner survivor bag mismatch') ELSE true END;");
}

// Removing the last child must not remove either of its parent's delta companions.
// Native DROP and staged cross-catalog DROP share the replacement cleanup path.
static void TestPublicationOwnerCleanup(bool cross_catalog, bool replace, bool explicit_transaction, bool commit) {
	std::cerr << "CASE publication owner cleanup cross_catalog=" << cross_catalog << " replace=" << replace
	          << " explicit_transaction=" << explicit_transaction << " commit=" << commit << std::endl;
	DuckDB db(nullptr);
	Connection owner(db), helper(db);
	const string child_catalog = cross_catalog ? "Consumer.Catalog" : "memory";
	const string parent = "memory.\"Parent.Space\".\"Parent;MV\"";
	const string child = SqlUtils::FullName(child_catalog, "Child.Space", "Child;MV");
	const string local_source = SqlUtils::FullName(child_catalog, "Child.Space", "source");
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Parent.Space\"; CREATE TABLE main.source(i INTEGER); "
	               "INSERT INTO main.source VALUES (1),(1),(2); CREATE MATERIALIZED VIEW " +
	                   parent + " AS SELECT * FROM memory.main.source;");
	if (cross_catalog) {
		Execute(owner, "ATTACH ':memory:' AS \"Consumer.Catalog\";");
	}
	Execute(owner, "CREATE SCHEMA " + SqlUtils::QuoteIdentifier(child_catalog) + ".\"Child.Space\"; CREATE TABLE " +
	                   local_source + "(i INTEGER); INSERT INTO " + local_source +
	                   " VALUES (7),(7),(8); CREATE MATERIALIZED VIEW " + child + " AS SELECT * FROM " + parent + ";");
	RefreshMetadata::UseCatalog(*owner.context, helper, "memory");
	auto key = RefreshMetadata(helper).FindViewKey("memory", "Parent.Space", "Parent;MV");
	if (key.empty()) {
		throw InvalidInputException("Publication owner fixture is missing parent metadata");
	}
	auto companions = "FROM duckdb_tables() WHERE database_name='memory' AND schema_name='Internal Space' "
	                  "AND table_name IN (" +
	                  Value(SqlUtils::DeltaName(key)).ToSQLString() + "," +
	                  Value(SqlUtils::DeltaName(PublishedViewName(key))).ToSQLString() + ")";
	auto check_companions = [&]() {
		Execute(owner, "SELECT CASE WHEN count(*)=2 THEN true ELSE error('Child cleanup removed parent delta') END " +
		                   companions + ";");
	};
	check_companions();
	// Use a search path that cannot resolve the parent's user-facing schema.
	Execute(owner, "USE " + SqlUtils::QuoteIdentifier(child_catalog) + ".\"Control.Space\";");
	if (explicit_transaction) {
		Execute(owner, "BEGIN;");
	}
	Execute(owner, replace ? "CREATE OR REPLACE MATERIALIZED VIEW " + child + " AS SELECT * FROM " + local_source + ";"
	                       : "SELECT 'before;drop'; /* quoted child; */ DROP VIEW " + child + "; SELECT 1;");
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	check_companions();
	for (const auto &source : vector<string> {"memory.main.source", local_source}) {
		Execute(owner, "INSERT INTO " + source + " VALUES (1),(2),(3),(3); DELETE FROM " + source +
		                   " WHERE i=1; UPDATE " + source + " SET i=4 WHERE i=2; INSERT INTO " + source +
		                   " VALUES (1),(4),(5); DELETE FROM " + source + " WHERE i=5;");
	}
	RefreshAndCheckBag(owner, parent, "SELECT * FROM memory.main.source");
	if (replace || !commit) {
		RefreshAndCheckBag(owner, child, "SELECT * FROM " + (replace && commit ? local_source : parent));
		Execute(owner, "DROP VIEW " + child + ";");
		check_companions();
		Execute(owner, "INSERT INTO memory.main.source VALUES (4),(4),(6); DELETE FROM memory.main.source WHERE i=4; "
		               "UPDATE memory.main.source SET i=7 WHERE i=6;");
		RefreshAndCheckBag(owner, parent, "SELECT * FROM memory.main.source");
	}
	Execute(owner, "DROP VIEW " + parent + ";");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Parent DROP retained its companions') END " +
	                   companions + ";");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Publication owner cleanup leaked state') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

static void TestCrossCatalogPublicationConsumers(bool commit) {
	std::cerr << "CASE cross-catalog publication consumers replacement_commit=" << commit << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	const string source = "memory.\"Source.Space\".items";
	const string replacement = "memory.\"Other.Source\".items";
	const string parent = "memory.\"Parent.Space\".\"Parent;MV\"";
	const string fast = "\"Consumer.Catalog\".\"Child.Space\".\"Fast;MV\"";
	const string slow = "\"Consumer.Catalog\".\"Other.Child\".\"Slow;MV\"";
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Source.Space\"; CREATE SCHEMA \"Other.Source\"; "
	               "CREATE SCHEMA \"Parent.Space\"; CREATE TABLE " +
	                   source + "(i INTEGER); CREATE TABLE " + replacement + "(i INTEGER); INSERT INTO " + source +
	                   " VALUES (1),(1),(2); INSERT INTO " + replacement +
	                   " VALUES (20),(20),(30); CREATE MATERIALIZED VIEW " + parent + " AS SELECT * FROM " + source +
	                   "; ATTACH ':memory:' AS \"Consumer.Catalog\"; "
	                   "CREATE SCHEMA \"Consumer.Catalog\".\"Child.Space\"; "
	                   "CREATE SCHEMA \"Consumer.Catalog\".\"Other.Child\"; CREATE MATERIALIZED VIEW " +
	                   fast + " AS SELECT * FROM " + parent + "; CREATE MATERIALIZED VIEW " + slow +
	                   " AS SELECT * FROM " + parent + "; USE \"Consumer.Catalog\".\"Control.Space\";");
	// No consumer is registered in the parent's catalog. Cover both publication
	// programs, keeping one child behind so cleanup must retain its earlier rows.
	Execute(owner, "INSERT INTO " + source + " VALUES (9),(9);");
	RefreshAndCheckBag(owner, parent, "SELECT * FROM " + source);
	RefreshAndCheckBag(owner, fast, "SELECT * FROM " + parent);
	Execute(owner, "INSERT INTO " + source + " VALUES (1),(2),(3),(3); DELETE FROM " + source +
	                   " WHERE i=1 OR i=9; UPDATE " + source + " SET i=4 WHERE i=2; INSERT INTO " + source +
	                   " VALUES (1),(4),(5); DELETE FROM " + source + " WHERE i=5;");
	RefreshAndCheckBag(owner, parent, "SELECT * FROM " + source);
	RefreshAndCheckBag(owner, fast, "SELECT * FROM " + parent);
	RefreshAndCheckBag(owner, slow, "SELECT * FROM " + parent);
	// Replacement must retain the parent's publication and emit its signed diff
	// for consumers in another metadata catalog. Rollback must retain the old source.
	Execute(owner, "BEGIN; CREATE OR REPLACE MATERIALIZED VIEW " + parent + " AS SELECT * FROM " + replacement + ";");
	Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	const auto &active_source = commit ? replacement : source;
	RefreshAndCheckBag(owner, fast, "SELECT * FROM " + active_source);
	RefreshAndCheckBag(owner, slow, "SELECT * FROM " + active_source);
	Execute(owner, "INSERT INTO " + active_source + " VALUES (1),(2),(3),(3); DELETE FROM " + active_source +
	                   " WHERE i=1; UPDATE " + active_source + " SET i=4 WHERE i=2; INSERT INTO " + active_source +
	                   " VALUES (1),(4),(5); DELETE FROM " + active_source + " WHERE i=5;");
	RefreshAndCheckBag(owner, parent, "SELECT * FROM " + active_source);
	RefreshAndCheckBag(owner, fast, "SELECT * FROM " + parent);
	RefreshAndCheckBag(owner, slow, "SELECT * FROM " + parent);
	Execute(owner, "DROP VIEW " + fast + "; DROP VIEW " + slow + "; DROP VIEW " + parent + ";");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Cross-catalog publication cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

static void TestPublicationOwnerReplay(bool commit) {
	std::cerr << "CASE publication owner replay commit=" << commit << std::endl;
	DuckDB db(nullptr);
	Connection owner(db), helper(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Parent.Space\"; CREATE TABLE main.source(i INTEGER); "
	               "INSERT INTO main.source VALUES (1),(1),(2); ATTACH ':memory:' AS \"Consumer.Catalog\"; "
	               "CREATE TABLE \"Consumer.Catalog\".main.source(i INTEGER); "
	               "CREATE MATERIALIZED VIEW \"Consumer.Catalog\".main.sentinel "
	               "AS SELECT * FROM \"Consumer.Catalog\".main.source;");
	RefreshMetadata::UseCatalog(*owner.context, helper, "Consumer.Catalog");
	Execute(owner, "BEGIN; CREATE MATERIALIZED VIEW \"Parent.Space\".\"Parent;MV\" AS SELECT * FROM main.source;");
	auto keys =
	    owner.Query("SELECT view_name FROM memory.\"Control.Space\".openivm_views WHERE view_sql_name='Parent;MV'");
	if (keys->HasError() || keys->RowCount() != 1) {
		throw InvalidInputException("Publication replay fixture is missing transaction-local parent metadata");
	}
	RefreshMetadata::DeltaSource source;
	source.table_name = SqlUtils::DeltaName(PublishedViewName(keys->GetValue(0, 0).ToString()));
	source.catalog_name = "memory";
	source.schema_name = "Internal Space";
	auto &state = TransactionalMVMetadataState::Get(*owner.context);
	if (RefreshMetadata(helper).IsMaterializedViewDelta(source) ||
	    !RefreshMetadata(helper).IsMaterializedViewDelta(source, &state)) {
		throw InvalidInputException("Publication owner lookup missed the transaction's new metadata location");
	}
	// Qualified source keys and a different selected registry must keep the same owner.
	source.table_name = SqlUtils::FullName(source.catalog_name, source.schema_name, source.table_name);
	if (!RefreshMetadata(helper).IsMaterializedViewDelta(source, &state)) {
		throw InvalidInputException("Publication owner replay lost qualified source identity");
	}
	Execute(owner, "CREATE MATERIALIZED VIEW main.\"Child;MV\" AS SELECT * FROM \"Parent.Space\".\"Parent;MV\"; "
	               "DROP VIEW main.\"Child;MV\";");
	Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	if (RefreshMetadata(helper).IsMaterializedViewDelta(source) != commit) {
		throw InvalidInputException("Publication owner lookup retained rolled-back or lost committed ownership");
	}
	if (!commit) {
		Execute(owner, "CREATE MATERIALIZED VIEW \"Parent.Space\".\"Parent;MV\" AS SELECT * FROM main.source;");
	}
	Execute(owner, "INSERT INTO main.source VALUES (1),(2),(3),(3); DELETE FROM main.source WHERE i=1; "
	               "UPDATE main.source SET i=4 WHERE i=2;");
	RefreshAndCheckBag(owner, "memory.\"Parent.Space\".\"Parent;MV\"", "SELECT * FROM memory.main.source");
	Execute(owner, "BEGIN; DROP VIEW \"Parent.Space\".\"Parent;MV\";");
	if (!RefreshMetadata(helper).IsMaterializedViewDelta(source) ||
	    RefreshMetadata(helper).IsMaterializedViewDelta(source, &state)) {
		throw InvalidInputException("Publication owner replay retained a transaction-local DROP");
	}
	Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	if (!commit) {
		Execute(owner, "INSERT INTO main.source VALUES (4),(4),(6); DELETE FROM main.source WHERE i=4; "
		               "UPDATE main.source SET i=7 WHERE i=6;");
		RefreshAndCheckBag(owner, "memory.\"Parent.Space\".\"Parent;MV\"", "SELECT * FROM memory.main.source");
		Execute(owner, "DROP VIEW \"Parent.Space\".\"Parent;MV\";");
	}
	if (RefreshMetadata(helper).IsMaterializedViewDelta(source)) {
		throw InvalidInputException("Publication owner lookup retained a dropped parent");
	}
	Execute(helper, "SELECT CASE WHEN current_database()='Consumer.Catalog' AND current_schema()='Control.Space' "
	                "THEN true ELSE error('Publication owner replay changed the selected metadata location') END;");
	Execute(owner, "INSERT INTO \"Consumer.Catalog\".main.source VALUES (1),(2),(3),(3); "
	               "DELETE FROM \"Consumer.Catalog\".main.source WHERE i=1; "
	               "UPDATE \"Consumer.Catalog\".main.source SET i=4 WHERE i=2;");
	RefreshAndCheckBag(owner, "\"Consumer.Catalog\".main.sentinel", "SELECT * FROM \"Consumer.Catalog\".main.source");
	Execute(owner, "DROP VIEW \"Consumer.Catalog\".main.sentinel;");
}

static void CheckSourceWatermarkReplay(Connection &owner, DatabaseInstance &db) {
	Connection helper(db);
	RefreshMetadata::UseCatalog(*owner.context, helper, "memory");
	const string columns = "SELECT d.table_name, d.last_update, d.last_refresh_ts FROM ";
	const string predicate = " v ON d.view_name=v.view_name WHERE v.view_sql_name='mv2' "
	                         "AND d.source_schema='Source.Space'";
	auto durable = owner.Query(columns + RefreshMetadata::MetadataTable(helper, openivm::DELTA_TABLES_TABLE) +
	                           " d JOIN " + RefreshMetadata::MetadataTable(helper, openivm::VIEWS_TABLE) + predicate);
	if (durable->HasError() || durable->RowCount() != 1) {
		throw InvalidInputException("Source watermark fixture must have one caller-visible source row");
	}
	// The replacement's new physical delta is uncommitted. Replay must retain
	// exactly the watermark evaluated by the caller without reading that table.
	TransactionalMVMetadataState::Get(*owner.context).Apply(helper);
	string expected = "SELECT " + durable->GetValue(0, 0).ToSQLString() + " AS table_name, " +
	                  durable->GetValue(1, 0).ToSQLString() + "::TIMESTAMP AS last_update, " +
	                  durable->GetValue(2, 0).ToSQLString() + "::TIMESTAMP AS last_refresh_ts";
	auto replayed = columns + "openivm_delta_tables d JOIN openivm_views" + predicate;
	Execute(helper, "SELECT CASE WHEN EXISTS (" + replayed + " EXCEPT ALL " + expected + ") OR EXISTS (" + expected +
	                    " EXCEPT ALL " + replayed +
	                    ") THEN error('Source watermark replay changed the caller value') ELSE true END;");
}

// Keep an unrelated, later-sorted metadata catalog selected at the end of source
// enumeration. Ownership in memory must include every earlier local mutation,
// regardless of which location Apply() last shadowed on the compiler helper.
static void TestSourceCascadeConsumerLocations(const string &mutation, bool commit, bool batched_drop,
                                               bool explicit_transaction) {
	std::cerr << "CASE source cascade mutation=" << mutation << " commit=" << commit << " batched_drop=" << batched_drop
	          << " explicit_transaction=" << explicit_transaction << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Source.Space\"; CREATE TABLE \"Source.Space\".a(i INTEGER); "
	               "CREATE TABLE \"Source.Space\".b(i INTEGER); CREATE TABLE \"Source.Space\".c(i INTEGER); "
	               "INSERT INTO \"Source.Space\".a VALUES (1),(1),(2); "
	               "INSERT INTO \"Source.Space\".b VALUES (1),(1),(2); "
	               "INSERT INTO \"Source.Space\".c VALUES (1),(1),(2); "
	               "CREATE MATERIALIZED VIEW mv1 AS SELECT a.i FROM \"Source.Space\".a a "
	               "JOIN \"Source.Space\".b b USING(i); "
	               "ATTACH ':memory:' AS \"zz Replay Peer\"; "
	               "CREATE TABLE \"zz Replay Peer\".main.b(i INTEGER); "
	               "INSERT INTO \"zz Replay Peer\".main.b VALUES (1),(1),(2); "
	               "CREATE MATERIALIZED VIEW \"zz Replay Peer\".main.mv1 "
	               "AS SELECT * FROM \"zz Replay Peer\".main.b;");
	// Verify placement before any transaction replay or cleanup. A CREATE that
	// folds the quoted catalog can bind successfully while publishing ownership
	// in memory instead of the peer's native metadata catalog.
	Execute(owner, "SELECT CASE WHEN count(*)=1 AND min(view_catalog)='zz Replay Peer' "
	               "THEN true ELSE error('Quoted peer metadata was published in the wrong catalog') END "
	               "FROM \"zz Replay Peer\".\"Control.Space\".openivm_views WHERE view_sql_name='mv1';");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true "
	               "ELSE error('Peer ownership leaked into the source metadata catalog') END "
	               "FROM memory.\"Control.Space\".openivm_views WHERE lower(view_catalog)='zz replay peer';");
	{
		Connection helper(db);
		RefreshMetadata::UseCatalog(*owner.context, helper, "zz replay peer");
		auto location = RefreshMetadata::CurrentLocation(helper);
		if (location.catalog_name != "zz Replay Peer" || location.schema_name != "Control.Space") {
			throw InvalidInputException("Case-insensitive catalog lookup changed metadata ownership");
		}
	}
	bool added = mutation == "create";
	bool removed = mutation == "drop";
	// Retain a nonempty delta for mv1. A new consumer must replay MAX(delta)+1,
	// not substitute its creation time or the helper's transaction timestamp.
	Execute(owner, "INSERT INTO \"Source.Space\".b VALUES (6),(6);");
	if (!added) {
		Execute(owner, "CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM \"Source.Space\".b;");
	}
	if (explicit_transaction) {
		Execute(owner, "BEGIN;");
	}
	if (added) {
		Execute(owner, "CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM \"Source.Space\".b;");
	} else if (removed) {
		Execute(owner, "DROP VIEW mv2;");
	} else {
		Execute(owner, "CREATE OR REPLACE MATERIALIZED VIEW mv2 AS SELECT * FROM \"Source.Space\".c;");
		if (explicit_transaction) {
			CheckSourceWatermarkReplay(owner, *db.instance);
		}
		if (mutation == "replace") {
			Execute(owner, "CREATE OR REPLACE MATERIALIZED VIEW mv2 AS SELECT * FROM \"Source.Space\".b;");
			if (explicit_transaction) {
				CheckSourceWatermarkReplay(owner, *db.instance);
			}
			Execute(owner, "CREATE OR REPLACE MATERIALIZED VIEW mv2 AS SELECT * FROM \"Source.Space\".c;");
		}
		if (mutation == "recreate") {
			Execute(owner, "DROP VIEW mv2; CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM \"Source.Space\".b;");
		}
	}
	if (explicit_transaction && !removed) {
		CheckSourceWatermarkReplay(owner, *db.instance);
	}
	Execute(owner, "DROP TABLE \"Source.Space\".a CASCADE;" + string(batched_drop ? " SELECT 1;" : ""));
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	bool owns_b = !commit || added || mutation == "recreate";
	Execute(owner, "SELECT CASE WHEN count(*)=" + string(owns_b ? "1" : "0") +
	                   " THEN true ELSE error('Source CASCADE lost local ownership or leaked a removed delta') END "
	                   "FROM duckdb_tables() WHERE database_name='memory' AND schema_name='Internal Space' "
	                   "AND table_name='" +
	                   RefreshMetadata::InternalSourceDeltaName("Source.Space", "b") + "';");
	vector<pair<string, string>> surviving = {
	    {"\"zz Replay Peer\".main.mv1", "SELECT * FROM \"zz Replay Peer\".main.b"},
	};
	if (!commit) {
		surviving.push_back({"mv1", "SELECT a.i FROM \"Source.Space\".a a JOIN \"Source.Space\".b b USING(i)"});
	}
	if ((!commit && !added) || (commit && !removed)) {
		surviving.push_back({"mv2", commit && mutation == "replace" ? "SELECT * FROM \"Source.Space\".c"
		                                                            : "SELECT * FROM \"Source.Space\".b"});
	}
	vector<string> sources = {"\"Source.Space\".b", "\"Source.Space\".c", "\"zz Replay Peer\".main.b"};
	if (!commit) {
		sources.push_back("\"Source.Space\".a");
	}
	for (auto &source : sources) {
		Execute(owner, "INSERT INTO " + source + " VALUES (1),(2),(3),(3); DELETE FROM " + source +
		                   " WHERE i=1; UPDATE " + source + " SET i=4 WHERE i=2; INSERT INTO " + source +
		                   " VALUES (1),(4),(5); DELETE FROM " + source + " WHERE i=5; UPDATE " + source +
		                   " SET i=2 WHERE i=4;");
	}
	for (auto &view : surviving) {
		Execute(owner, "PRAGMA refresh('" + view.first + "');");
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view.first + " EXCEPT ALL " + view.second +
		                   ") OR EXISTS (" + view.second + " EXCEPT ALL SELECT * FROM " + view.first +
		                   ") THEN error('Cross-location source CASCADE survivor bag mismatch') ELSE true END;");
	}
	if (commit && !removed) {
		Execute(owner, "DROP VIEW mv2;");
	}
	if (!commit) {
		Execute(owner, "DROP VIEW mv1;");
		if (!added) {
			Execute(owner, "DROP VIEW mv2;");
		}
	}
	Execute(owner, "DROP VIEW \"zz Replay Peer\".main.mv1;");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Final consumer delta cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

// Legacy view keys may repeat across metadata locations. Exclusions must name
// their metadata table even when the excluded key also appears elsewhere.
static void TestStagedCascadeConsumerIdentity() {
	std::cerr << "CASE staged cascade consumer identity" << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE TABLE a(i INTEGER); CREATE TABLE b(i INTEGER); INSERT INTO a VALUES (1),(1),(2); "
	               "INSERT INTO b VALUES (1),(1),(2); "
	               "CREATE MATERIALIZED VIEW mv1 AS SELECT a.i FROM a JOIN b USING(i); "
	               "ATTACH ':memory:' AS peer; CREATE MATERIALIZED VIEW peer.main.mv1 AS SELECT * FROM memory.main.b;");
	Connection helper(db);
	RefreshMetadata::UseCatalog(*owner.context, helper, "memory");
	auto primary_table = RefreshMetadata::MetadataTable(helper, openivm::DELTA_TABLES_TABLE);
	auto primary_key = RefreshMetadata(helper).FindViewKey("memory", "main", "mv1");
	auto sources = RefreshMetadata(helper).GetDeltaSources(primary_key, "memory", "main");
	RefreshMetadata::UseCatalog(*owner.context, helper, "peer");
	auto peer_key = RefreshMetadata(helper).FindViewKey("peer", "main", "mv1");
	// Include the peer key in the primary exclusion list: it must not exclude
	// the peer row. This also exercises legacy short-key collisions without
	// changing persisted physical identities to manufacture a legacy database.
	bool checked_b = false;
	for (auto &source : sources) {
		if (RefreshMetadata::SourceTableName(source.table_name, source.catalog_name, source.schema_name) !=
		    SqlUtils::DeltaName("b")) {
			continue;
		}
		checked_b = true;
		if (!RefreshMetadata::HasSourceDeltaConsumers(
		        helper, source, nullptr,
		        {{primary_table, Value(primary_key).ToSQLString() + "," + Value(peer_key).ToSQLString()}})) {
			throw InvalidInputException("Source ownership exclusions leaked into another metadata catalog");
		}
	}
	if (!checked_b) {
		throw InvalidInputException("Cross-catalog consumer fixture is missing source b ownership");
	}
	Execute(owner, "DROP TABLE a CASCADE; "
	               "INSERT INTO b VALUES (1),(2),(3),(3); DELETE FROM b WHERE i=1; UPDATE b SET i=4 WHERE i=2; "
	               "INSERT INTO b VALUES (1),(4),(5); DELETE FROM b WHERE i=5; UPDATE b SET i=2 WHERE i=4;");
	Execute(owner, "PRAGMA refresh('peer.main.mv1');");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM peer.main.mv1 EXCEPT ALL SELECT * FROM b) "
	               "OR EXISTS (SELECT * FROM b EXCEPT ALL SELECT * FROM peer.main.mv1) "
	               "THEN error('Staged CASCADE excluded another catalog consumer') ELSE true END;");
	Execute(owner, "DROP VIEW peer.main.mv1;");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Staged final consumer delta cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

// Native batches prepare each DROP after earlier statements have executed. Cover
// full view cleanup, a committed sibling, final consumers, and rollback without
// relying on optimizer or result-order settings to execute maintenance steps.
static void TestBatchedViewDrop(bool explicit_transaction, bool commit, bool disable_optimizer) {
	std::cerr << "CASE batched view drop explicit_transaction=" << explicit_transaction << " commit=" << commit
	          << " disable_optimizer=" << disable_optimizer << std::endl;
	DuckDB db(nullptr);
	Connection owner(db), helper(db);
	Execute(owner, "LOAD openivm;");
	Execute(owner, "SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "SET openivm_stateful_auxstate=true; SET preserve_insertion_order=false; SET threads=4;");
	Execute(owner, "CREATE SCHEMA \"Source.Space\"; CREATE SCHEMA \"Public.Space\"; CREATE SCHEMA \"Other.Public\"; "
	               "CREATE TABLE \"Source.Space\".items(g INTEGER, x INTEGER); "
	               "INSERT INTO \"Source.Space\".items VALUES (1,10),(1,10),(2,20);");
	Execute(owner, "CREATE MATERIALIZED VIEW \"Public.Space\".\"Same.MV\" "
	               "AS SELECT g, COUNT(DISTINCT x) AS n FROM \"Source.Space\".items GROUP BY g;");
	Execute(owner, "CREATE MATERIALIZED VIEW \"Other.Public\".\"Same.MV\" AS SELECT * FROM \"Source.Space\".items;");
	RefreshMetadata::UseCatalog(*owner.context, helper, "memory");
	auto view_key = RefreshMetadata(helper).FindViewKey("memory", "Public.Space", "Same.MV");
	if (view_key.empty() || RefreshMetadata(helper).GetAuxiliaryTables(view_key).empty()) {
		throw InvalidInputException("Batched view DROP fixture must own auxiliary state");
	}
	auto metadata_table = RefreshMetadata::MetadataTable(helper, openivm::VIEWS_TABLE);
	string owned_names;
	vector<string> owned_tables = {IncrementalTableNames::DataTableName(view_key), PublishedViewName(view_key),
	                               SqlUtils::DeltaName(view_key), SqlUtils::DeltaName(PublishedViewName(view_key))};
	for (auto &name : RefreshMetadata(helper).GetAuxiliaryTables(view_key)) {
		owned_tables.push_back(name);
	}
	for (auto &name : owned_tables) {
		if (!owned_names.empty()) {
			owned_names += ",";
		}
		owned_names += Value(name).ToSQLString();
	}
	auto owned_count = "(SELECT count(*) FROM duckdb_tables() WHERE schema_name='Internal Space' "
	                   "AND table_name IN (" +
	                   owned_names + "))";
	Execute(owner, "SELECT CASE WHEN " + owned_count +
	                   ">=3 THEN true "
	                   "ELSE error('Batched DROP fixture has no backing/publication/auxiliary tables') END;");
	if (disable_optimizer) {
		Execute(owner, "PRAGMA disable_optimizer;");
	}
	if (explicit_transaction) {
		Execute(owner, "BEGIN;");
	}
	// A preceding statement must finish before DROP looks up ownership. Both
	// mutations and trailing queries must remain in their original batch order.
	Execute(owner, string(disable_optimizer ? "SELECT 1; " : "INSERT INTO \"Source.Space\".items VALUES (3,30); ") +
	                   "DROP VIEW \"Public.Space\".\"Same.MV\"; SELECT 1;");
	if (disable_optimizer) {
		Execute(owner, "PRAGMA enable_optimizer;");
	}
	Execute(owner, "SELECT CASE WHEN " + owned_count +
	                   "=0 AND "
	                   "(SELECT count(*) FROM " +
	                   metadata_table + " WHERE view_name=" + Value(view_key).ToSQLString() +
	                   ")=0 THEN true ELSE error('Batched view DROP leaked metadata or maintenance tables') END;");
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	bool retained = explicit_transaction && !commit;
	Execute(owner, "SELECT CASE WHEN (SELECT count(*) FROM " + metadata_table +
	                   " WHERE view_name=" + Value(view_key).ToSQLString() + ")=" + string(retained ? "1" : "0") +
	                   " THEN true ELSE error('Batched DROP commit/rollback lost registry identity') END;");
	Execute(owner, "INSERT INTO \"Source.Space\".items VALUES (1,10),(2,20),(3,30),(3,30); "
	               "DELETE FROM \"Source.Space\".items WHERE x=10; "
	               "UPDATE \"Source.Space\".items SET x=40 WHERE x=20; "
	               "INSERT INTO \"Source.Space\".items VALUES (1,10),(4,40),(5,50); "
	               "DELETE FROM \"Source.Space\".items WHERE x=50; "
	               "UPDATE \"Source.Space\".items SET x=20 WHERE x=40;");
	vector<pair<string, string>> survivors = {
	    {"\"Other.Public\".\"Same.MV\"", "SELECT * FROM \"Source.Space\".items"},
	};
	if (retained) {
		survivors.push_back(
		    {"\"Public.Space\".\"Same.MV\"", "SELECT g, COUNT(DISTINCT x) FROM \"Source.Space\".items GROUP BY g"});
	}
	for (auto &view : survivors) {
		Execute(owner, "PRAGMA refresh('" + view.first + "');");
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view.first + " EXCEPT ALL " + view.second +
		                   ") OR EXISTS (" + view.second + " EXCEPT ALL SELECT * FROM " + view.first +
		                   ") THEN error('Batched view DROP survivor bag mismatch') ELSE true END;");
	}
	if (retained) {
		Execute(owner, "BEGIN; DROP VIEW \"Public.Space\".\"Same.MV\"; "
		               "DROP VIEW \"Other.Public\".\"Same.MV\"; COMMIT;");
	} else {
		Execute(owner, "DROP VIEW \"Other.Public\".\"Same.MV\"; SELECT 1;");
	}
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Batched final consumer cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

static void TestMixedLifecycleBatch(bool explicit_transaction, bool commit) {
	std::cerr << "CASE mixed lifecycle explicit_transaction=" << explicit_transaction << " commit=" << commit
	          << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal;Space'; "
	               "CREATE SCHEMA \"Public;Space\"; CREATE TABLE source(i INTEGER); "
	               "INSERT INTO source VALUES (1),(1),(2);");
	const string view = "\"Public;Space\".\"Mixed;MV\"";
	// DuckDB tokenizes newline-concatenated literals as one token. Comments
	// between their segments must be removed without erasing either segment.
	const string base_query = "SELECT i, 'a'\n'b' AS label, 'c' /* nested /* ; */ comment */\n'd' AS joined, "
	                          "'value;--literal' AS comment_label, $$dollar;/*literal*/$$ AS dollar_label FROM source";
	Execute(owner, "CREATE MATERIALIZED VIEW " + view + " REFRESH EVERY '1 hour' AS " + base_query + ";");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view + " EXCEPT ALL " + base_query + ") OR EXISTS (" +
	                   base_query + " EXCEPT ALL SELECT * FROM " + view +
	                   ") THEN error('Scheduled lifecycle changed literal contents') ELSE true END;");
	Execute(owner, "CREATE MATERIALIZED VIEW sibling AS SELECT * FROM source;");
	if (explicit_transaction) {
		Execute(owner, "BEGIN;");
	}
	// Native statement binding follows execution order. The second DROP must
	// consume ownership from a CREATE executed in this same batch.
	Execute(owner, "SELECT $$native;literal$$, 'create materialized view in a literal'; "
	               "/* outer ; /* nested ; */ comment */ DROP VIEW " +
	                   view + "; -- delimiter ; inside a comment\n CREATE MATERIALIZED VIEW " + view + " AS " +
	                   base_query + "; INSERT INTO source VALUES (3),(3); DROP VIEW " + view +
	                   "; CREATE /* keyword comment ; */ OR REPLACE MATERIALIZED VIEW " + view + " AS " + base_query +
	                   "; SELECT CASE WHEN count(*)=5 THEN true ELSE error('Mixed batch reordered DML') END FROM " +
	                   view + ";");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view + " EXCEPT ALL " + base_query + ") OR EXISTS (" +
	                   base_query + " EXCEPT ALL SELECT * FROM " + view +
	                   ") THEN error('Mixed lifecycle initial load bag mismatch') ELSE true END;");
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	Execute(owner, "INSERT INTO source VALUES (1),(2),(3),(3); DELETE FROM source WHERE i=1; "
	               "UPDATE source SET i=4 WHERE i=2; INSERT INTO source VALUES (1),(5),(5); "
	               "DELETE FROM source WHERE i=5; UPDATE source SET i=2 WHERE i=4;");
	for (auto &survivor : vector<pair<string, string>> {{view, base_query}, {"sibling", "SELECT * FROM source"}}) {
		Execute(owner, "PRAGMA refresh('" + survivor.first + "');");
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + survivor.first + " EXCEPT ALL " + survivor.second +
		                   ") OR EXISTS (" + survivor.second + " EXCEPT ALL SELECT * FROM " + survivor.first +
		                   ") THEN error('Mixed lifecycle survivor bag mismatch') ELSE true END;");
	}
	Execute(owner, "DROP VIEW " + view + "; DROP VIEW sibling; SELECT 1;");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Mixed lifecycle final cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal;Space';");
}

static void TestSequentialCreateBatch(bool explicit_transaction, bool commit) {
	std::cerr << "CASE sequential CREATE batch explicit_transaction=" << explicit_transaction << " commit=" << commit
	          << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "PRAGMA enable_profiling='no_output'; PRAGMA enable_verification;");
	const auto metrics = ClientConfig::GetConfig(*owner.context).profiler_settings;
	// BEGIN, schema/table creation, DML, and MV creation are all in one input.
	// No source exists when DuckDB preprocesses it. Each replacement must see
	// the caller's preceding DROP and intervening mutations.
	Execute(owner,
	        string(explicit_transaction ? "BEGIN; " : "") +
	            "CREATE SCHEMA \"Public;Space\"; CREATE TABLE \"Public;Space\".source(i INTEGER); "
	            "INSERT INTO \"Public;Space\".source VALUES (1),(1),(2); "
	            "CREATE MATERIALIZED VIEW \"Public;Space\".mv AS SELECT * FROM \"Public;Space\".source; "
	            "DROP VIEW \"Public;Space\".mv; INSERT INTO \"Public;Space\".source VALUES (3),(3); "
	            "CREATE MATERIALIZED VIEW \"Public;Space\".mv AS SELECT * FROM \"Public;Space\".source; "
	            "CREATE OR REPLACE MATERIALIZED VIEW \"Public;Space\".mv AS SELECT * FROM \"Public;Space\".source;");
	if (ClientConfig::GetConfig(*owner.context).profiler_settings != metrics) {
		throw InvalidInputException("Lifecycle execution did not restore profiling metrics");
	}
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM \"Public;Space\".mv EXCEPT ALL "
	               "SELECT * FROM \"Public;Space\".source) OR EXISTS (SELECT * FROM \"Public;Space\".source "
	               "EXCEPT ALL SELECT * FROM \"Public;Space\".mv) "
	               "THEN error('Sequential CREATE initial load mismatch') ELSE true END;");
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	if (explicit_transaction && !commit) {
		Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Sequential CREATE rollback leaked objects') "
		               "END FROM duckdb_tables() WHERE schema_name IN ('Public;Space','Internal Space');");
		return;
	}
	Execute(owner, "INSERT INTO \"Public;Space\".source VALUES (1),(2),(3),(3); "
	               "DELETE FROM \"Public;Space\".source WHERE i=1; "
	               "UPDATE \"Public;Space\".source SET i=4 WHERE i=2;");
	Execute(owner, "PRAGMA refresh('\"Public;Space\".mv');");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM \"Public;Space\".mv EXCEPT ALL "
	               "SELECT * FROM \"Public;Space\".source) OR EXISTS (SELECT * FROM \"Public;Space\".source "
	               "EXCEPT ALL SELECT * FROM \"Public;Space\".mv) "
	               "THEN error('Sequential CREATE refresh bag mismatch') ELSE true END;");
	Execute(owner, "DROP VIEW \"Public;Space\".mv; SELECT CASE WHEN count(*)=0 THEN true "
	               "ELSE error('Sequential CREATE final cleanup leaked') END FROM duckdb_tables() "
	               "WHERE schema_name='Internal Space';");
}

static void TestLateLifecycleBindFailure(bool explicit_transaction) {
	std::cerr << "CASE late lifecycle bind failure explicit_transaction=" << explicit_transaction << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; CREATE TABLE source(i INTEGER); "
	               "INSERT INTO source VALUES (1),(1),(2); CREATE MATERIALIZED VIEW mv AS SELECT * FROM source; "
	               "PRAGMA enable_profiling='no_output';");
	const auto metrics = ClientConfig::GetConfig(*owner.context).profiler_settings;
	if (explicit_transaction) {
		Execute(owner, "BEGIN;");
	}
	// Exercise the same owning program abstraction with a deterministic *late*
	// binder failure, after successful schema/table creation and captured DML.
	// The public logical-statement API avoids introducing a test-only SQL function.
	auto binder = Binder::CreateBinder(*owner.context);
	auto plan =
	    BindLifecycleProgram(*owner.context, *binder,
	                         "CREATE SCHEMA \"Late;Schema\"; "
	                         "CREATE TABLE \"Late;Schema\".scratch AS SELECT i, count(*) FROM source GROUP BY i; "
	                         "CREATE TABLE \"Late;Schema\".recursive AS WITH RECURSIVE r(i) AS "
	                         "(VALUES (1) UNION ALL SELECT i+1 FROM r WHERE i<3) SELECT * FROM r; "
	                         "INSERT INTO source VALUES (7); SELECT missing_lifecycle_column FROM source;");
	plan->ResolveOperatorTypes();
	auto statement = make_uniq<LogicalPlanStatement>(std::move(plan));
	statement->query = "OpenIVM late lifecycle bind failure regression";
	auto result = owner.Query(std::move(statement));
	if (!result->HasError() || result->GetError().find("missing_lifecycle_column") == string::npos) {
		throw InvalidInputException("Lifecycle fixture did not reach its intended late binder failure");
	}
	if (ClientConfig::GetConfig(*owner.context).profiler_settings != metrics) {
		throw InvalidInputException("Failed lifecycle execution did not restore profiling metrics");
	}
	if (explicit_transaction) {
		// A binder error normally permits COMMIT. Partial lifecycle writes must
		// instead invalidate the transaction, so COMMIT rolls all of them back.
		Execute(owner, "COMMIT;");
	}
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM source EXCEPT ALL SELECT * FROM (VALUES (1),(1),(2))) "
	               "OR EXISTS (SELECT * FROM (VALUES (1),(1),(2)) EXCEPT ALL SELECT * FROM source) "
	               "OR EXISTS (SELECT 1 FROM duckdb_schemas() WHERE schema_name='Late;Schema') "
	               "THEN error('Failed lifecycle committed partial writes') ELSE true END;");
	Execute(owner, "INSERT INTO source VALUES (1),(2),(3),(3); DELETE FROM source WHERE i=1; "
	               "UPDATE source SET i=4 WHERE i=2;");
	Execute(owner, "PRAGMA refresh('mv');");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM mv EXCEPT ALL SELECT * FROM source) "
	               "OR EXISTS (SELECT * FROM source EXCEPT ALL SELECT * FROM mv) "
	               "THEN error('Failed lifecycle damaged delta capture') ELSE true END;");
	Execute(owner, "DROP VIEW mv;");
}

int main(int argc, char **argv) {
	if (argc != 2) {
		std::cerr << "Usage: source_delta_publication_test <fresh-database-path>" << std::endl;
		return 2;
	}
	try {
		TestQuotedNamespaceNormalization();
		TestQualifiedTableReferences();
		TestInternalSchemaReopen(argv[1]);
		TestSourceDeltaLookup();
		TestSharedMetadataUpgrade(false, true);
		TestSharedMetadataUpgrade(true, true);
		TestSharedMetadataUpgrade(true, false);
		for (bool executor_drop : {false, true}) {
			TestDeferredScratchDrop(false, true, executor_drop);
			TestDeferredScratchDrop(true, true, executor_drop);
			TestDeferredScratchDrop(true, false, executor_drop);
		}
		TestSequentialCreateBatch(false, true);
		TestSequentialCreateBatch(true, true);
		TestSequentialCreateBatch(true, false);
		TestLateLifecycleBindFailure(false);
		TestLateLifecycleBindFailure(true);
		TestMetadataReplayLocations();
		for (bool dedicated_schema : {false, true}) {
			TestCrossCatalogSharedDelta(dedicated_schema, false);
			TestCrossCatalogSharedDelta(dedicated_schema, true);
		}
		for (bool cross_catalog : {false, true}) {
			TestPublicationOwnerCleanup(cross_catalog, false, false, true);
			TestPublicationOwnerCleanup(cross_catalog, true, true, false);
			TestPublicationOwnerCleanup(cross_catalog, true, true, true);
		}
		TestPublicationOwnerCleanup(false, false, true, false);
		TestPublicationOwnerCleanup(false, false, true, true);
		TestCrossCatalogPublicationConsumers(false);
		TestCrossCatalogPublicationConsumers(true);
		TestPublicationOwnerReplay(false);
		TestPublicationOwnerReplay(true);
		TestDeltaOwnershipSearchPath(false);
		TestDeltaOwnershipSearchPath(true);
		for (const auto &mutation : vector<string> {"create", "drop", "replace", "recreate"}) {
			for (bool batched_drop : {false, true}) {
				TestSourceCascadeConsumerLocations(mutation, false, batched_drop, true);
				TestSourceCascadeConsumerLocations(mutation, true, batched_drop, true);
				TestSourceCascadeConsumerLocations(mutation, true, batched_drop, false);
			}
		}
		TestStagedCascadeConsumerIdentity();
		for (bool disable_optimizer : {false, true}) {
			TestBatchedViewDrop(false, true, disable_optimizer);
			TestBatchedViewDrop(true, true, disable_optimizer);
			TestBatchedViewDrop(true, false, disable_optimizer);
		}
		TestPublication(false);
		TestPublication(true);
		TestDropDuringPublication(false, false);
		TestDropDuringPublication(true, false);
		TestDropDuringPublication(false, true);
		TestMixedLifecycleBatch(false, true);
		TestMixedLifecycleBatch(true, true);
		TestMixedLifecycleBatch(true, false);
		std::cout << "PASS" << std::endl;
	} catch (const std::exception &ex) {
		std::cerr << ex.what() << std::endl;
		return 1;
	}
}
