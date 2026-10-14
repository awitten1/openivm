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
ownership reuse, and refresh bag equality. Run it from a
fresh pinned build alongside `test/sql/internal_schema.test` and
`test/sql/internal_schema_ducklake.test` for namespace lifecycle regression coverage.

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
