#include "duckdb.hpp"
#include "core/parser.hpp"
#include "core/parser_ddl.hpp"
#include "core/refresh_locks.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/main/client_context.hpp"

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
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw InvalidInputException(result->GetError());
	}
}

static void TestPublication(bool explicit_transaction) {
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

// Replay on a reused helper must follow catalog/schema identity, including when
// CREATE and CASCADE have both contributed mutations to the same transaction.
static void TestMetadataReplayLocations() {
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

// Keep an unrelated, later-sorted metadata catalog selected at the end of source
// enumeration. Ownership in memory must include every earlier local mutation,
// regardless of which location Apply() last shadowed on the compiler helper.
static void TestSourceCascadeConsumerLocations(const string &mutation, bool commit) {
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
	bool added = mutation == "create";
	bool removed = mutation == "drop";
	if (!added) {
		Execute(owner, "CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM \"Source.Space\".b;");
	}
	Execute(owner, "BEGIN;");
	if (added) {
		Execute(owner, "CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM \"Source.Space\".b;");
	} else if (removed) {
		Execute(owner, "DROP VIEW mv2;");
	} else {
		Execute(owner, "CREATE OR REPLACE MATERIALIZED VIEW mv2 AS SELECT * FROM \"Source.Space\".c;");
		if (mutation == "recreate") {
			Execute(owner, "DROP VIEW mv2; CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM \"Source.Space\".b;");
		}
	}
	Execute(owner, "DROP TABLE \"Source.Space\".a CASCADE;");
	Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
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

int main() {
	try {
		TestMetadataReplayLocations();
		for (const auto &mutation : vector<string> {"create", "drop", "replace", "recreate"}) {
			TestSourceCascadeConsumerLocations(mutation, false);
			TestSourceCascadeConsumerLocations(mutation, true);
		}
		TestStagedCascadeConsumerIdentity();
		TestPublication(false);
		TestPublication(true);
		TestDropDuringPublication(false, false);
		TestDropDuringPublication(true, false);
		TestDropDuringPublication(false, true);
		std::cout << "PASS" << std::endl;
	} catch (const std::exception &ex) {
		std::cerr << ex.what() << std::endl;
		return 1;
	}
}
