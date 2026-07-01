# ducklake-replica-sync

Synchronizes changed ledgers from a primary DuckLake catalog into a target
serving DuckLake catalog.

This component is separate from `index-materializer`. It uses DuckLake
snapshots/change feed to discover which source ledgers changed, then rebuilds
those ledgers in the target table from the current primary table contents.

## Configuration

- `QUACK_URI`, default `quack:127.0.0.1:9494`
- `QUACK_TOKEN`, required
- `QUACK_REMOTE_DB`, default `remote_lake`
- `QUACK_DISABLE_SSL`, default `true`
- `SOURCE_CATALOG`, default `stellar_lake`
- `SOURCE_TABLES`, required comma-separated list like
  `bronze.transactions_row_v2,bronze.contract_events_stream_v1`
- `REPLICA_NAME`, default `serving_replica`
- `START_SNAPSHOT`, default `0`
- `TARGET_MODE`, default `embedded`
- `TARGET_DUCKLAKE_CATALOG_PATH`, default `ducklake/serving.ducklake`
- `TARGET_DUCKLAKE_DATA_PATH`, default `ducklake/serving-data`
- `TARGET_ATTACH_NAME`, default `serving_lake`
- `TARGET_QUACK_URI`, required when `TARGET_MODE=quack`
- `TARGET_QUACK_TOKEN`, required when `TARGET_MODE=quack`
- `TARGET_QUACK_REMOTE_DB`, default `target_lake`
- `TARGET_QUACK_DISABLE_SSL`, default `QUACK_DISABLE_SSL`
- `LEDGER_BATCH_SIZE`, default `1000`

Optional ledger column overrides:

- `LEDGER_COLUMN`, default `ledger_sequence`
- `LEDGER_COLUMN_OVERRIDES`, comma-separated `table=column` pairs

Example:

```bash
QUACK_TOKEN=dev_secret \
SOURCE_TABLES=bronze.transactions_row_v2,bronze.contract_events_stream_v1 \
TARGET_DUCKLAKE_CATALOG_PATH=/tmp/serving.ducklake \
TARGET_DUCKLAKE_DATA_PATH=/tmp/serving-data \
bin/ducklake-replica-sync
```

Production read-replica mode should use a target Quack server that owns the
serving DuckLake attachment:

```bash
QUACK_URI=quack:primary-quack:9494 \
QUACK_TOKEN=primary_secret \
TARGET_MODE=quack \
TARGET_QUACK_URI=quack:replica-quack:9494 \
TARGET_QUACK_TOKEN=replica_secret \
SOURCE_TABLES=bronze.transactions_row_v2,bronze.contract_events_stream_v1 \
bin/ducklake-replica-sync
```

In `TARGET_MODE=quack`, the sync component sends the target rebuild transaction
to the target Quack server. The target server attaches the primary Quack endpoint
inside that server-side script, pulls current primary rows, rebuilds target
ledgers, and commits the checkpoint. This keeps the target DuckLake attachment
owned by the read-replica Quack server while replication is running.

Changed ledgers are rebuilt in bounded batches controlled by
`LEDGER_BATCH_SIZE`. The table checkpoint advances only after all batches for
that table have completed.

## Semantics

For each source table, the component:

1. reads the latest primary DuckLake snapshot
2. reads the table checkpoint from `replica.sync_checkpoints` in the target
3. calls `table_changes(source_table, last_snapshot + 1, current_snapshot)`
4. extracts changed ledger sequences
5. deletes those ledgers from the target table
6. inserts current primary rows for those ledgers into the target table
7. advances the checkpoint only after the target transaction commits

Derived target tables are rebuildable serving state. The primary DuckLake
catalog remains authoritative.
