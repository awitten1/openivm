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
transaction. Metadata tables that `main` holds outside a view registry, such as the
constraint cache created at `LOAD` or before a first view created inside a
transaction, move with them, including declared RELY foreign keys. Until then,
OpenIVM reads the metadata where it is.

The move runs outside explicit transactions. Inside `BEGIN ... COMMIT`, OpenIVM
keeps using the existing metadata schema. If a catalog has no metadata yet, the
first materialized view in the transaction creates the configured schema, and
`ROLLBACK` removes it with the view.

## Internal maintenance objects

Internal objects never move with the metadata schema, and never cross catalogs.
`openivm_internal_schema` selects a dedicated schema for them inside each data
catalog:

```sql
SET openivm_internal_schema = 'openivm_internal';
CREATE MATERIALIZED VIEW sales.product_totals AS
    SELECT product, SUM(amount) AS total FROM sales.orders GROUP BY product;
-- sales.product_totals stays in schema sales; its tables go to openivm_internal.
```

| Object | Unset (default) | `openivm_internal_schema = 'I'` |
|---|---|---|
| User-facing materialized view | The requested catalog and schema | Unchanged |
| Backing table `openivm_data_<key>` | The MV's catalog and schema | `I` in the MV's catalog |
| Publication table `openivm_visible_<key>` and its delta | The MV's catalog and schema | `I` in the MV's catalog |
| MV delta table `openivm_delta_<key>` | The MV's catalog and schema | `I` in the MV's catalog |
| Auxiliary state (DISTINCT counts, semi/anti match counts) | The MV's catalog and schema | `I` in the MV's catalog |
| Unique index on grouped backing tables | The backing table | The backing table |
| Native source delta | `openivm_delta_<table>` beside the source | `openivm_delta_<hex(schema)>_<table>` in `I` of the source's catalog |
| DuckLake source changes | DuckLake snapshots; no delta table | Unchanged |

For a DuckLake MV, `I` is a schema of the DuckLake catalog, created in the lake's
own transaction. Native objects are created in the native catalog's transaction,
so an explicit transaction's `ROLLBACK` removes them and a newly created `I`.

`<key>` is an internal view key derived from the view's catalog, schema and name,
so same-named views in different schemas or catalogs have separate internal
objects. A source delta in an internal schema encodes its source schema in hex,
so same-named sources of different schemas never collide there.
`openivm_delta_tables` records each source's catalog and schema and, for moved
deltas, the internal schema (`delta_schema`). `openivm_views.internal_schema`
records where a view's internal tables live.

**Existing objects.** The setting applies to objects created while it is set.
OpenIVM finds existing tables where they are, through `openivm_views.internal_schema`
and `openivm_delta_tables.delta_schema` (empty means beside the view or source), so
nothing has to be moved by hand:

- A source delta is shared by every view over that source. A new view reuses the
  existing delta wherever it lives; one source never gets a second delta.
- `CREATE OR REPLACE` keeps the replaced view's internal location, so views built
  on it keep reading the same tables. Drop and recreate a view to move it.
- Deltas of OpenIVM's own tables (a parent MV's publication) stay beside them.

An MV in an external catalog other than DuckLake keeps its internal objects in the
`main` schema of the native frontend database, never in the metadata schema.

Inspect actual locations with:

```sql
SELECT database_name, schema_name, table_name
FROM duckdb_tables()
WHERE starts_with(table_name, 'openivm_')
ORDER BY database_name, schema_name, table_name;
```
