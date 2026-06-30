# postgres-sink

Consumes `stellar.ledger.batch.v1` events and idempotently materializes the first canonical tables into Postgres:

- `stellar_ledgers`
- `stellar_transactions`
- `stellar_operations`

Environment:

- `POSTGRES_DSN`, default `postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable`
- `PORT`, default `:50052`
- `HEALTH_PORT`, default `8089`

The sink intentionally does not parse Stellar XDR. It writes rows from `LedgerBatch`.
