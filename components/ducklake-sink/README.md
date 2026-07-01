# ducklake-sink

Consumes `stellar.ledger.batch.v1` events and writes them into a DuckLake catalog through embedded DuckDB.

Environment:

- `DUCKLAKE_CATALOG_PATH`, default `ducklake/stellar.ducklake`
- `DUCKLAKE_DATA_PATH`, default `ducklake/data`
- `DUCKLAKE_ATTACH_NAME`, default `stellar_lake`
- `PORT`, default `:50052`
- `HEALTH_PORT`, default `8089`

Tables:

```text
ledger_batches
bronze_rows
```

`ledger_batches` stores one row per processed ledger and the full protobuf JSON payload. `bronze_rows` stores the full extractor surface as one row per bronze row. Replaying the same network and ledger sequence deletes and reinserts those rows in one DuckLake transaction.
