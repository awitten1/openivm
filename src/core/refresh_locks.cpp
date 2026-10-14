#include "core/refresh_locks.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/printer.hpp"

#include <cstdio>

namespace duckdb {

void MutationGate::Lock(const void *owner) {
	std::unique_lock<mutex> guard(lock);
	if (active_owner == owner) {
		depth++;
		return;
	}
	waiting_owners++;
	condition.wait(guard, [&]() { return active_owner == nullptr; });
	waiting_owners--;
	active_owner = owner;
	depth = 1;
}

void MutationGate::Unlock(const void *owner) {
	std::lock_guard<mutex> guard(lock);
	if (active_owner != owner || depth == 0) {
		// Report accounting errors in release builds without unlocking another owner's gate.
		fprintf(stderr,
		        "[openivm] mutation gate release mismatch: owner=%p active_owner=%p depth=%llu. "
		        "The gate stays held; subsequent OpenIVM mutations on this database will block.\n",
		        owner, active_owner, static_cast<unsigned long long>(depth));
		fflush(stderr);
		D_ASSERT(false);
		return;
	}
	depth--;
	if (depth == 0) {
		active_owner = nullptr;
		condition.notify_one();
	}
}

bool MutationGate::TryLock(const void *owner) {
	std::lock_guard<mutex> guard(lock);
	if (active_owner && active_owner != owner) {
		return false;
	}
	active_owner = owner;
	depth++;
	return true;
}

shared_ptr<MutationGate> RefreshLocks::AcquireGate(DatabaseInstance &db) {
	return db.GetObjectCache().GetOrCreate<MutationGate>(MutationGate::ObjectType());
}

MutationLockGuard::MutationLockGuard(ClientContext &context)
    : MutationLockGuard(DatabaseInstance::GetDatabase(context),
                        TransactionalMVLockState::Get(context).GetMutationOwner()) {
}

const void *TransactionalMVLockState::GetMutationOwner() {
	lock_guard<mutex> guard(state_lock);
	return mutation_owner;
}

TransactionalMVLockState::TransactionalMVLockState(ClientContext &context) : owner(context), mutation_owner(&context) {
}

TransactionalMVLockState &TransactionalMVLockState::Get(ClientContext &context) {
	return *context.registered_state->GetOrCreate<TransactionalMVLockState>("openivm_transactional_mv_locks", context);
}

void TransactionalMVLockState::AcquireMutationLock() {
	// Parallel delta-capture workers share this state. Losing a guard here leaks its gate depth.
	lock_guard<mutex> guard(state_lock);
	if (!mutation_guard) {
		mutation_guard = make_uniq<MutationLockGuard>(DatabaseInstance::GetDatabase(owner), mutation_owner);
		OPENIVM_DEBUG_PRINT("[LOCK] acquired database mutation lock owner=%p\n", static_cast<void *>(&owner));
	}
}

void TransactionalMVLockState::SetMutationOwner(const void *owner_token) {
	lock_guard<mutex> guard(state_lock);
	if (mutation_guard) {
		throw InternalException("OpenIVM cannot change mutation ownership after acquiring the mutation lock");
	}
	mutation_owner = owner_token;
}

void TransactionalMVLockState::DeferDeltaCleanup(vector<string> statements) {
	lock_guard<mutex> guard(state_lock);
	for (auto &statement : statements) {
		if (std::find(deferred_delta_cleanup.begin(), deferred_delta_cleanup.end(), statement) ==
		    deferred_delta_cleanup.end()) {
			deferred_delta_cleanup.push_back(std::move(statement));
		}
	}
}

void TransactionalMVLockState::TransactionCommit(MetaTransaction &transaction, ClientContext &context) {
	vector<string> cleanup;
	vector<RefreshMetadata::DeltaSource> source_cleanup;
	{
		lock_guard<mutex> guard(state_lock);
		cleanup.swap(deferred_delta_cleanup);
		source_cleanup.swap(deferred_source_delta_cleanup);
	}
	if (!cleanup.empty() || !source_cleanup.empty()) {
		// The caller is already committed. Housekeeping failures must never report
		// a failed commit or roll back a successfully refreshed MV. Retained deltas
		// are excluded by the committed watermark and retried on a later refresh.
		try {
			Connection con(*context.db);
			Get(*con.context).SetMutationOwner(GetMutationOwner());
			// A later CREATE in the same transaction may have reused a delta that
			// replacement scheduled for cleanup. Check committed ownership again.
			for (auto &source : source_cleanup) {
				if (!RefreshMetadata::HasSourceDeltaConsumers(con, source)) {
					cleanup.push_back("DROP TABLE IF EXISTS " + SqlUtils::FullName(source.catalog_name,
					                                                               source.delta_schema_name,
					                                                               source.delta_table_name));
				}
			}
			for (auto &sql : cleanup) {
				auto result = con.Query(sql);
				if (result->HasError()) {
					Printer::Print("OpenIVM transaction committed; external delta cleanup deferred: " +
					               result->GetError());
				}
			}
		} catch (const std::exception &ex) {
			Printer::Print(string("OpenIVM transaction committed; external delta cleanup deferred: ") + ex.what());
		}
	}
	Release();
}

void TransactionalMVLockState::DeferUnusedSourceDeltaCleanup(const RefreshMetadata::DeltaSource &source) {
	lock_guard<mutex> guard(state_lock);
	deferred_source_delta_cleanup.push_back(source);
}

void TransactionalMVLockState::RecordSourceDeltaDrop(const string &catalog, const string &schema, const string &table) {
	lock_guard<mutex> guard(state_lock);
	dropped_source_deltas.insert(StringUtil::Lower(SqlUtils::FullName(catalog, schema, table)));
}

bool TransactionalMVLockState::HasDroppedSourceDelta(const string &catalog, const string &schema, const string &table) {
	lock_guard<mutex> guard(state_lock);
	return dropped_source_deltas.count(StringUtil::Lower(SqlUtils::FullName(catalog, schema, table))) != 0;
}

optional_ptr<TableCatalogEntry>
TransactionalMVLockState::FindDeferredSourceDelta(const string &catalog, const string &schema, const string &table) {
	lock_guard<mutex> guard(state_lock);
	for (auto &source : deferred_source_delta_cleanup) {
		if (StringUtil::CIEquals(catalog, source.catalog_name) && StringUtil::CIEquals(schema, source.schema_name) &&
		    StringUtil::CIEquals(RefreshMetadata::SourceTableName(source.table_name, catalog, schema),
		                         SqlUtils::DeltaName(table))) {
			// Ownership was removed by an earlier replacement, but this transaction
			// still reserves the physical delta until its post-commit cleanup check.
			return Catalog::GetEntry<TableCatalogEntry>(owner, catalog, source.delta_schema_name,
			                                            source.delta_table_name, OnEntryNotFound::RETURN_NULL);
		}
	}
	return nullptr;
}

void TransactionalMVLockState::TransactionRollback(MetaTransaction &transaction, ClientContext &context) {
	Release();
}

void TransactionalMVLockState::Release() {
	lock_guard<mutex> guard(state_lock);
	OPENIVM_DEBUG_PRINT("[LOCK] release database mutation lock owner=%p\n", static_cast<void *>(&owner));
	deferred_delta_cleanup.clear();
	deferred_source_delta_cleanup.clear();
	dropped_source_deltas.clear();
	mutation_guard.reset();
}

} // namespace duckdb
