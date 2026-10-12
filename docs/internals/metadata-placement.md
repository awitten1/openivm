# Metadata and internal-object placement

OpenIVM keeps two kinds of objects. Control metadata describes views, their
sources, watermarks and refresh history. Internal maintenance objects hold the
data that refresh reads and writes. They follow separate placement rules.

## Control metadata

The control-metadata tables are `openivm_views`, `openivm_delta_tables`,
`openivm_refresh_hooks`, `openivm_refresh_history`, `openivm_refresh_profile`,
`openivm_mv_dependencies`, `openivm_match_log` and `openivm_constraints_cache`.

**Catalog.** A view in a native DuckDB catalog keeps its metadata in that catalog.
A view in an external catalog, such as DuckLake, keeps its metadata in the native
frontend database. DuckDB cannot commit one transaction across a native catalog
and DuckLake, so OpenIVM commits native metadata separately from DuckLake data.

**Schema.** Within that catalog, `openivm_metadata_schema` selects the schema:

```sql
SET openivm_metadata_schema = 'openivm';
CREATE MATERIALIZED VIEW product_sales AS
    SELECT product_name, SUM(amount) AS total FROM orders GROUP BY product_name;

SELECT view_sql_name, view_catalog, view_schema FROM openivm.openivm_views;
```

| Setting value | Behavior |
|---|---|
| empty (default) | Each catalog keeps its metadata in the schema that already holds it. New metadata uses `main`. |
| a schema name | New metadata is created in that schema. Existing metadata in another schema is moved there automatically. |

The setting is not stored in the database. After reopening, OpenIVM finds the
schema that holds `openivm_views` in each catalog, so later sessions and the
refresh daemon need no setting. If two schemas of one catalog contain
`openivm_views`, OpenIVM reports an error; set `openivm_metadata_schema` to the
schema to use.

### Moving existing metadata

Do not move OpenIVM tables by hand. When `openivm_metadata_schema` names a schema
that does not yet hold a catalog's metadata, the next `CREATE MATERIALIZED VIEW`
or refresh in that catalog moves all control-metadata tables into it in one
transaction. Metadata tables created before the first view, such as the constraint
cache, move with them. Until then, OpenIVM reads the metadata where it is.

The move runs outside explicit transactions. Inside `BEGIN ... COMMIT`, OpenIVM
keeps using the existing metadata schema. If a catalog has no metadata yet, the
first materialized view in the transaction creates the configured schema, and
`ROLLBACK` removes it with the view.

## Internal maintenance objects

Internal objects never move with the metadata schema:

| Object | Location |
|---|---|
| User-facing materialized view | The requested catalog and schema |
| Backing table `openivm_data_<key>` | The MV's catalog and schema |
| Publication table `openivm_visible_<key>` and its delta | The MV's catalog and schema |
| MV delta table `openivm_delta_<key>` | The MV's catalog and schema; for DuckLake MVs, the DuckLake schema |
| Auxiliary state (DISTINCT counts, semi/anti match counts) | The MV's catalog and schema |
| Unique index on grouped backing tables | The backing table |
| Native source delta `openivm_delta_<table>` | The source table's catalog and schema |
| DuckLake source changes | DuckLake snapshots; no delta table |

`<key>` is an internal view key derived from the view's catalog, schema and name.
Same-named views in different schemas or catalogs therefore have separate internal
objects, and `openivm_delta_tables` records each source's catalog and schema.
Source deltas are named after their source table and stay beside it, so
same-named sources in different schemas keep separate deltas.

Inspect actual locations with:

```sql
SELECT database_name, schema_name, table_name
FROM duckdb_tables()
WHERE starts_with(table_name, 'openivm_')
ORDER BY database_name, schema_name, table_name;
```
