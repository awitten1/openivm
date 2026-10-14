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
competing connection, shared placement, and refresh bag equality. Run it from a
fresh pinned build alongside `test/sql/internal_schema.test` and
`test/sql/internal_schema_ducklake.test` for namespace lifecycle regression coverage.
