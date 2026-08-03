# quack-ducklake-server

Owns a DuckDB process with a DuckLake catalog attached, then exposes that
process through the Quack remote protocol.

This component is infrastructure for shared DuckLake access. It should run
beside ingestion, index materializers, and query APIs rather than inside
`stellar-query-api` or `obsrvr-gateway`.

## Configuration

- `DUCKLAKE_CATALOG_PATH`, default `ducklake/stellar.ducklake`
- `DUCKLAKE_DATA_PATH`, default `ducklake/data`
- `DUCKLAKE_ATTACH_NAME`, default `stellar_lake`
- `DUCKLAKE_METADATA_ATTACH_NAME`, default
  `__ducklake_metadata_<DUCKLAKE_ATTACH_NAME>`; the hidden DuckDB metadata
  database that owns the file-backed catalog WAL. Explicit checkpoints target
  this database, not the DuckLake attachment's maintenance checkpoint.
- `DUCKLAKE_INLINE_ROW_LIMIT`, default `1024`; applied idempotently at
  startup via `set_option('data_inlining_row_limit', …)`. Inserts below this
  row count are inlined into the catalog instead of writing Parquet.
  Inlined commits cost ~0.18ms/row (measured), so the limit tiers writes:
  small tables inline, large tables take the fast Parquet path, and
  `ducklake-maintenance` merges the resulting files. Measured commit latency
  per mainnet ledger: `20000` → ~1.7s (all inlined), `1024` → ~0.55s
  (~1 file/ledger), `256` → ~85ms (~7 files/ledger). `0` disables inlining;
  a negative value leaves the catalog's persisted setting untouched.
- `QUACK_URI`, default `quack:127.0.0.1:9494`
- `QUACK_TOKEN`, required
- `QUACK_HEALTH_ADDR`, default `:8088`; serves `/healthz` and `/metrics`
- `QUACK_ALLOW_OTHER_HOSTNAME`, default `false`
- `QUACK_DISABLE_SSL`, default `false`
- `QUACK_INSECURE`, default `false`; required before plaintext,
  allow-other-hostname, external-access, or `disabled_filesystems=none` mode
  can start. When true, the server also uses plaintext Quack.
- `QUACK_ENABLE_EXTERNAL_ACCESS`, default `false`
- `QUACK_DISABLED_FILESYSTEMS`, default `LocalFileSystem`; set to `none` only
  for intentionally file-backed local DuckLake catalogs.
- `QUACK_LOCK_CONFIGURATION`, default `true`
- `QUACK_MEMORY_LIMIT`, default `4GB`
- `QUACK_DUCKDB_THREADS`, default `4`
- `DUCKDB_CHECKPOINT_THRESHOLD`, optional DuckDB WAL auto-checkpoint threshold;
  unset uses DuckDB's `16 MiB` default. The default puts catalog checkpoints on
  ingest commits and causes multi-second tail spikes during sustained backfill.
  A measured `1GB` profile reduced 650-ledger sink latency from p95 `400ms`,
  p99 `592ms`, max `2.504s` to p95 `369ms`, p99 `418ms`, max `918ms`.
  Increasing it defers rather than eliminates checkpoint work and permits a
  larger WAL, so it is an interim latency control rather than a hard-SLO proof.
- `CHECKPOINT_ENABLED`, default `false`; enables the coordinated manual
  checkpoint primitive and authenticated `POST /admin/checkpoint` endpoint.
- `CHECKPOINT_TIMEOUT`, default `30s`; bounds manual checkpoint execution.
- `CHECKPOINT_ADMIN_TOKEN`, required when checkpointing is enabled. Send it as
  `Authorization: Bearer <token>`. Keep it separate from public health/metrics
  access and inject it through Nomad secrets rather than a jobspec literal.
- `CHECKPOINT_CONTROLLER_ENABLED`, default `false`; enables the periodic
  soft-idle/hard-limit scheduler. It requires `CHECKPOINT_ENABLED=true`.
- `CHECKPOINT_SOFT_WAL_BYTES`, default `67108864` (64 MiB), and
  `CHECKPOINT_HARD_WAL_BYTES`, default `536870912` (512 MiB), are the measured
  experiment candidates, not production policy.
- `CHECKPOINT_POLL_INTERVAL`, default `1s`, and `CHECKPOINT_IDLE_DURATION`,
  default `2s`, control scheduler polling and the minimum post-ingest idle
  period. The controller remains disabled until the cadence gate passes.
- `QUACK_DUCKDB_PATH`, optional DuckDB local database path
- `INGEST_PORT`, default empty (disabled); serves `BronzeIngestService` — a
  gRPC stream that commits ledger batches in-process, one DuckLake
  transaction per ledger with per-ledger acks, watermark-gated delete-skip
  for fresh ledgers, and replay-on-uncertainty after failures. Rows stage
  through native memory tables via the DuckDB Appender and land via
  `INSERT..SELECT`, so data inlining applies. Authenticated with
  `QUACK_TOKEN` via `x-ingest-token` metadata. For sub-400ms commits pair
  with `DUCKLAKE_INLINE_ROW_LIMIT=256` and a 1–5 minute
  `ducklake-maintenance` interval.

## Telemetry

`/metrics` uses a process-local Prometheus registry. It exposes bounded-label
histograms for `decode`, `staging`, `preface`, `transfer`, `commit`, `cleanup`,
and total server receive-to-ack latency, plus batch/retry/budget/in-flight
metrics. Catalog and `<catalog>.wal` byte gauges read file size at scrape time.
Manual checkpoints use a dedicated DuckDB connection and the same writer
coordinator as ingest. If ingest owns the coordinator, the endpoint returns
HTTP 409 and records an `ingest_active` deferral rather than racing the ledger
transaction. Successful and failed executions update checkpoint duration, inflight state,
result, retry-backoff, and last-success metrics. Manual requests make at most
three attempts with bounded exponential backoff. A failed execution persists
checkpoint error state and makes `/healthz` return 503 until a later successful
checkpoint clears the failure.

The initial rules and Grafana dashboard live under `deploy/monitoring/`. The
Nomad server template registers `quack-ducklake-metrics` on the health port so
Prometheus does not try to scrape the Quack protocol port.

## Local Example

```bash
QUACK_TOKEN=dev_secret \
QUACK_INSECURE=true \
QUACK_ENABLE_EXTERNAL_ACCESS=true \
QUACK_DISABLED_FILESYSTEMS=none \
DUCKLAKE_CATALOG_PATH=/tmp/stellar.ducklake \
DUCKLAKE_DATA_PATH=/tmp/stellar-data \
bin/quack-ducklake-server
```

Clients can run SQL inside the server process:

```sql
LOAD quack;
ATTACH 'quack:127.0.0.1:9494' AS remote (
  TOKEN 'dev_secret',
  DISABLE_SSL true
);

SELECT * FROM remote.query(
  'SELECT count(*) FROM stellar_lake.bronze.transactions_row_v2'
);
```

Production startup fails closed when plaintext transport or relaxed filesystem
access is requested without `QUACK_INSECURE=true`. The server also pins DuckDB
to one connection, sets memory/thread limits, exposes `/healthz`, and locks
configuration after Quack starts.

Current Quack beta limitation: strict `enable_external_access=false` plus
`disabled_filesystems=LocalFileSystem` prevents local `ducklake:` catalogs from
serving through Quack. The chaos harness therefore opts into
`QUACK_ENABLE_EXTERNAL_ACCESS=true` and `QUACK_DISABLED_FILESYSTEMS=none` for
file-backed local catalogs. Treat that as an explicit residual risk and prefer
isolated hosts/storage until Quack supports a narrower allowlist.
