#include "duckdb.hpp"
#include "core/parser.hpp"
#include "core/parser_ddl.hpp"
#include "core/refresh_locks.hpp"
#include "duckdb/main/client_context.hpp"

#include <future>
#include <iostream>

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

int main() {
	try {
		TestPublication(false);
		TestPublication(true);
		std::cout << "PASS" << std::endl;
	} catch (const std::exception &ex) {
		std::cerr << ex.what() << std::endl;
		return 1;
	}
}
