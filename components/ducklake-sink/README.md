# ducklake-sink

Consumes `stellar.ledger.batch.v1` events and writes them into a DuckLake catalog.

The sink supports two modes:

- `DUCKLAKE_MODE=embedded`, default: attach DuckLake directly from this process.
- `DUCKLAKE_MODE=quack`: send write SQL to a `quack-ducklake-server` that owns the DuckLake attachment.

Environment:

- `DUCKLAKE_MODE`, default `embedded`
- `DUCKLAKE_CATALOG_PATH`, default `ducklake/stellar.ducklake`
- `DUCKLAKE_DATA_PATH`, default `ducklake/data`
- `DUCKLAKE_ATTACH_NAME`, default `stellar_lake`
- `QUACK_URI`, default `quack:127.0.0.1:9494`
- `QUACK_TOKEN`, required when `DUCKLAKE_MODE=quack`
- `QUACK_REMOTE_DB`, default `remote_lake`
- `PORT`, default `:50052`
- `HEALTH_PORT`, default `8089`

Envelope tables:

```text
ledger_batches
bronze_rows
```

Typed bronze tables are also materialized under the `bronze` schema using the same table names as `stellar-history-loader`, including:

```text
bronze.ledgers_row_v2
bronze.transactions_row_v2
bronze.operations_row_v2
bronze.effects_row_v1
bronze.trades_row_v1
bronze.accounts_snapshot_v1
bronze.trustlines_snapshot_v1
bronze.contract_events_stream_v1
bronze.contract_data_snapshot_v1
bronze.token_transfers_stream_v1
```

`ledger_batches` stores one row per processed ledger and the full protobuf JSON payload. `bronze_rows` stores the full extractor surface as one row per bronze row. The typed `bronze.*` tables provide history-loader-compatible analytical tables. Replaying the same ledger deletes and reinserts both the envelope rows and typed rows in one DuckLake transaction.
