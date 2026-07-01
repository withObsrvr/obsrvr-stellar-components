# Quack DuckLake Architecture

`quack-ducklake-server` is the owner of the DuckDB process that attaches the
DuckLake catalog. Other components connect to it through the Quack remote
protocol instead of attaching the DuckLake catalog independently.

## Placement

The server should run as lake infrastructure beside ingestion, index
materialization, and query services.

It should not live inside `stellar-query-api` or `obsrvr-gateway`:

- `stellar-query-api` should remain an HTTP API over lake/query surfaces.
- `obsrvr-gateway` should remain the public gateway, auth, routing, metering,
  and product boundary.
- `quack-ducklake-server` should own DuckLake attachment, DuckDB extensions,
  object-store/catalog credentials, and remote SQL execution.

## Initial Runtime Shape

```text
raw-ledger-source
  -> stellar-ledger-processor
  -> ducklake-sink
       DUCKLAKE_MODE=quack
       QUACK_URI=quack:quack-ducklake-server:9494

index-materializer
  INDEX_NAME=tx_hash_index
  -> server-side INSERT ... SELECT through Quack

index-materializer
  INDEX_NAME=contract_events_index
  -> server-side INSERT ... SELECT through Quack

stellar-query-api / obsrvr-gateway
  -> query through Quack or purpose-built API readers
```

## Why

This centralizes DuckLake access in one process. It avoids every sink,
transformer, and API process racing to attach the same DuckLake catalog and
keeps materialization work close to the data.

The index materialization model should prefer server-side SQL:

```sql
INSERT INTO index.some_index
SELECT ...
FROM bronze.some_table
WHERE ledger_sequence > $start
  AND ledger_sequence <= $end;
```

This replaces row-moving transformer services with compact orchestration:

1. choose ledger range
2. run SQL through Quack
3. checkpoint the range
4. report health and lag

## Caveat

Quack is currently beta. Treat it as an internal lake access layer first. Keep
the embedded DuckLake sink mode available as a fallback until production
behavior is proven under concurrent ingestion, materialization, maintenance,
and query load.
