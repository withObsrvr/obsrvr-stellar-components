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
- `QUACK_DISABLE_SSL`, default `false`; set only for an explicitly insecure dev server
- `DUCKLAKE_REMOTE_TIMEOUT`, default `30s`
- `PORT`, default `:50052`
- `HEALTH_PORT`, default `8089`; serves `/healthz`

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

`ledger_batches` stores one row per processed ledger and the full protobuf JSON
payload. `bronze_rows` stores the full extractor surface as one row per bronze
row. `ingest_watermarks` stores one row per committed ledger batch and is
written in the same transaction/script as the batch. The typed `bronze.*` tables
provide history-loader-compatible analytical tables. Replaying the same ledger
deletes and reinserts the envelope rows, typed rows, and watermark in one
DuckLake transaction.

DuckLake schema changes are tracked in `schema_migrations`. The bootstrap
`bronze_schema.sql` is migration `001`, and future ordered migrations can evolve
existing catalogs instead of relying on `CREATE TABLE IF NOT EXISTS` no-ops.

Gap check:

```sql
WITH bounds AS (
  SELECT
    min(ledger_sequence) AS min_seq,
    max(ledger_sequence) AS max_seq
  FROM ingest_watermarks
  WHERE network_passphrase = '<network passphrase>'
),
expected AS (
  SELECT range AS ledger_sequence
  FROM bounds, range(CAST(min_seq AS BIGINT), CAST(max_seq AS BIGINT) + 1)
)
SELECT expected.ledger_sequence
FROM expected
LEFT JOIN ingest_watermarks USING (ledger_sequence)
WHERE ingest_watermarks.ledger_sequence IS NULL
ORDER BY expected.ledger_sequence;
```

The sink records the first observed `network_passphrase` in `catalog_metadata`
and refuses later batches from a different network. Use one catalog/database per
network.

The `/healthz` endpoint reports the latest write ledger, write age, and last
write error. A failed write normally exits the process after bounded retries, so
the endpoint is mainly for liveness and post-restart recency checks.
