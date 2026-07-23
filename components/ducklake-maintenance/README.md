# ducklake-maintenance

Periodically runs DuckLake maintenance through the Quack server that owns the
lake attachment:

1. `ducklake_flush_inlined_data` — materializes inlined rows (small inserts
   stored in the catalog database) into Parquet files.
2. `ducklake_merge_adjacent_files` — compacts adjacent small Parquet files.
3. `ducklake_expire_snapshots` — expires snapshots older than the retention
   window.

Together with `DUCKLAKE_INLINE_ROW_LIMIT` on `quack-ducklake-server`, this
enables the inline-first write path: per-ledger commits are catalog-only
(no Parquet on the hot path), and this component materializes healthy Parquet
files off the write path.

It intentionally does not delete expired files
(`ducklake_cleanup_old_files` / `ducklake_delete_orphaned_files`) — storage
reclamation interacts with time travel and replica checkpoints and needs its
own operation.

Environment:

- `QUACK_URI`, default `quack:127.0.0.1:9494`
- `QUACK_TOKEN`, required
- `QUACK_REMOTE_DB`, default `remote_lake`
- `QUACK_DISABLE_SSL`, default `false`
- `DUCKLAKE_ATTACH_NAME`, default `stellar_lake`
- `MAINTENANCE_INTERVAL`, default `5m`
- `SNAPSHOT_RETENTION`, default `48h`; `0` disables snapshot expiry
- `MERGE_ADJACENT_FILES`, default `true`
- `RUN_ONCE`, default `false`; run one cycle and exit (harness/verification)
- `MAINTENANCE_REMOTE_TIMEOUT`, default `5m`
- `HEALTH_PORT`, default `8090`; serves `/healthz`

Constraint: `SNAPSHOT_RETENTION` must exceed `ducklake-replica-sync`'s
worst-case checkpoint lag. If a replica checkpoint falls behind the oldest
retained snapshot, that table falls back to a full resync.

A failing statement inside a cycle is logged and reported via `/healthz`, but
the loop keeps running and later statements in the cycle still execute — a
transient flush failure must not stop snapshot expiry, and a Quack outage must
not kill the component.
