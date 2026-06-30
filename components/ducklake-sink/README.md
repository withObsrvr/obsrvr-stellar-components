# ducklake-sink

Consumes `stellar.ledger.batch.v1` events and writes partitioned, DuckDB-loadable JSONL files plus a `schema.sql` helper. This is the first analytical materialization step before native DuckLake catalog writes.

Environment:

- `DUCKDB_EXPORT_DIR`, default `duckdb-ledger-batches`
- `PORT`, default `:50052`
- `HEALTH_PORT`, default `8089`

Output layout:

```text
duckdb-ledger-batches/
  schema.sql
  network=<network>/
    ledger_range=<range>/
      ledger_<sequence>.jsonl
```
