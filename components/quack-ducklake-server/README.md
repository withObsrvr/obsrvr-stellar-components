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
- `QUACK_ALLOW_OTHER_HOSTNAME`, default `true`
- `QUACK_DISABLE_SSL`, default `true`
- `QUACK_DUCKDB_PATH`, optional DuckDB local database path

## Local Example

```bash
QUACK_TOKEN=dev_secret \
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
