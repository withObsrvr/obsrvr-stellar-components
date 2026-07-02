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
- `QUACK_URI`, default `quack:127.0.0.1:9494`
- `QUACK_TOKEN`, required
- `QUACK_HEALTH_ADDR`, default `:8088`; serves `/healthz`
- `QUACK_ALLOW_OTHER_HOSTNAME`, default `false`
- `QUACK_DISABLE_SSL`, default `false`
- `QUACK_INSECURE`, default `false`; required before plaintext or
  allow-other-hostname mode can start. When true, the server also uses
  plaintext Quack.
- `QUACK_ENABLE_EXTERNAL_ACCESS`, default `false`
- `QUACK_DISABLED_FILESYSTEMS`, default `LocalFileSystem`; set to `none` only
  for intentionally file-backed local DuckLake catalogs.
- `QUACK_LOCK_CONFIGURATION`, default `true`
- `QUACK_MEMORY_LIMIT`, default `4GB`
- `QUACK_DUCKDB_THREADS`, default `4`
- `QUACK_DUCKDB_PATH`, optional DuckDB local database path

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

Production startup fails closed when plaintext mode is requested without
`QUACK_INSECURE=true`. The server also pins DuckDB to one connection, sets
memory/thread limits, exposes `/healthz`, and locks configuration after Quack
starts.

Current Quack beta limitation: strict `enable_external_access=false` plus
`disabled_filesystems=LocalFileSystem` prevents local `ducklake:` catalogs from
serving through Quack. The chaos harness therefore opts into
`QUACK_ENABLE_EXTERNAL_ACCESS=true` and `QUACK_DISABLED_FILESYSTEMS=none` for
file-backed local catalogs. Treat that as an explicit residual risk and prefer
isolated hosts/storage until Quack supports a narrower allowlist.
