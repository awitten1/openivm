# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the quack extension, this should probably be the goal too.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or 
```bash
make test_debug
```

The native `source_delta_publication_test` integration executable pauses CREATE
after source-delta allocation and before ownership publication, then checks a
competing connection in both autocommit and an explicit transaction whose metadata
snapshot predates ownership publication, shared placement, transaction-local
ownership reuse, and refresh bag equality. `make test` builds and runs it through
`test_release_internal`; `make test_source_delta_publication` runs it on its own.
Together with `test/sql/internal_schema.test` and
`test/sql/internal_schema_ducklake.test`, it covers namespace lifecycle regressions.
It also checks source-delta lookup with separate control, source and maintenance
schemas, including same-named sources and a missing owner. Catalog enumeration
must release its schema lock before resolving table entries. Batched conflicting
writes must still refresh both views to bidirectional bag equality.
Metadata-upgrade cases cover the first CREATE over a legacy registry in autocommit,
explicit commit, and rollback followed by retry. They check rollback of the new
schema and metadata column, refresh bag equality, and final-consumer cleanup.
Scratch-table and scratch-view cleanup also checks native DROP and the physical DROP executor
after CREATE in one batch, with a persistent namesake, quoted names, a separate
control schema, autocommit, explicit commit and rollback. Serializer verification
is enabled for both DROP executor functions, including absent `IF EXISTS` targets;
deserialization must preserve the native binder's resolved temporary identity.
Each case then refreshes
the surviving MV after conflicting batched writes and checks both EXCEPT ALL directions.

It also pauses a CREATE that reuses a committed view's source delta, waits until a
competing DROP actually blocks on the mutation gate, then publishes the new view
before allowing DROP to proceed. Native autocommit, explicit-transaction, and
cross-catalog staged cleanup must preserve the surviving delta, capture batched
INSERT/DELETE/UPDATE operations, and refresh to full bag equality in both directions.

`internal_schema.test` also replaces and drops one of two committed, independent
views sharing a source within an explicit transaction. With separate metadata and
maintenance schemas, both commit and rollback must preserve the sibling's delta
and capture batched conflicting writes. Dropping the final owners in one
transaction must then remove that delta.

The lifecycle executable also checks repeated replacement and DROP/recreate before
source CASCADE, with commit and rollback. Transaction-local metadata replay must
preserve the caller's exact source watermark, including nonempty shared deltas and
new physical deltas that helper connections cannot yet see.

The namespace lifecycle fixtures also cover a qualified source-table CASCADE
followed by final-consumer DROP or replacement in the same transaction. Commit
must remove the physical source delta; rollback and independent siblings must
retain ownership and refresh correctly after conflicting batches. The native
integration executable reuses one replay helper across two metadata catalogs and
back, checking that CREATE/CASCADE mutations and temporary shadows retain their
catalog/schema identities.

The same native executable places an unrelated metadata catalog after the source
catalog in enumeration order. It checks transaction-local CREATE, DROP,
replacement, and DROP/recreate before source CASCADE, with both commit and
rollback, plus every mutation in autocommit with direct and batched CASCADE.
Ownership probes also move a helper to another quoted catalog/schema after
selecting metadata, checking both durable rows and an uncommitted replay sibling
without changing the helper's search path.
Each surviving MV receives conflicting batched INSERT/DELETE/UPDATE
and a bidirectional `EXCEPT ALL` comparison; final-consumer cleanup must remove
all maintenance tables. A cross-catalog source consumer and an exclusion list containing its key
also verify that direct CASCADE exclusions and staged final DROP retain the
consumer's catalog/schema identity.

Before that matrix starts, quoted mixed-case catalog placement is checked in
both native metadata registries. CREATE normalization must preserve quoted
identifiers (including escaped quotes and embedded apostrophes); selecting the
same catalog with different casing must still find its own metadata schema.

Native batch cleanup also runs the source CASCADE ownership matrix with a trailing
statement. Batched view DROP checks quoted, same-named views, auxiliary-state and
publication cleanup, shared-source survival, commit/rollback, and final-consumer
cleanup. It exercises post-bind routing with the optimizer disabled and result
order preservation disabled. Surviving views receive conflicting batched DML and
are checked with bidirectional `EXCEPT ALL` after each refresh.

Mixed DROP/CREATE batches return extension statement ASTs to DuckDB's native
query driver. Each lifecycle compiles only after preceding native statements
execute; generated SQL then binds and executes one step at a time on the caller
context and active executor. This covers source tables and schemas created in the
same input, including `BEGIN`, replacement, native verification and profiling,
quoted semicolons, dollar-quoted literals, nested and keyword comments, intervening
DML, explicit commit and rollback.
Metadata replay supplies helper-local ownership visibility without running physical
programs ahead of their native statement position.

The fixture also injects a late binder failure after successful lifecycle writes.
Autocommit rolls back the statement; an explicit transaction must be invalidated
so COMMIT cannot retain partial writes. Profiling metrics must be restored on
success and failure, and surviving MVs must refresh to bidirectional bag equality.
