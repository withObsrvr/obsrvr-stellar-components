# Nomad Migration

Current target replacement:

```text
stellar-live-source-datalake -> stellar-postgres-ingester
```

New shape:

```text
raw-ledger-source@0.2.2
  -> stellar-ledger-processor
  -> postgres-sink
```

After the Postgres path is proven, add fan-out:

```text
raw-ledger-source@0.2.2
  -> stellar-ledger-processor
    -> postgres-sink
    -> ducklake-sink
```

## Migration Gates

1. Local `raw-ledger-source -> stellar-ledger-processor -> jsonl-sink` run writes at least one `stellar.ledger.batch.v1` payload.
2. Replaying the same ledger produces the same row IDs.
3. Postgres sink upserts ledgers, transactions, and operations idempotently.
4. Testnet Nomad deployment replaces the old direct ingester path.
5. Analytical fan-out is enabled after Postgres replacement is stable.

## Required Environment

`stellar-ledger-processor`:

- `NETWORK_PASSPHRASE`
- `PORT`
- `HEALTH_PORT`
- `ENABLE_FLOWCTL`
- `FLOWCTL_ENDPOINT`

`postgres-sink`:

- `POSTGRES_DSN`
- `PORT`
- `HEALTH_PORT`
- `ENABLE_FLOWCTL`
- `FLOWCTL_ENDPOINT`

`ducklake-sink`:

- `DUCKDB_EXPORT_DIR`
- `PORT`
- `HEALTH_PORT`
- `ENABLE_FLOWCTL`
- `FLOWCTL_ENDPOINT`
