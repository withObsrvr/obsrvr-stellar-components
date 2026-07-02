# postgres-sink

Consumes `stellar.ledger.batch.v1` events and idempotently materializes canonical tables into Postgres:

- `stellar_ledgers`
- `stellar_transactions`
- `stellar_operations`
- `stellar_bronze_rows`

`stellar_bronze_rows` stores the full `stellar-extract` bronze surface as JSONB keyed by source table name, ledger, and deterministic row ID.

Replay semantics:

- canonical ledgers, transactions, and operations upsert on natural keys and
  refresh every non-key column
- bronze rows are replaced by `(network_passphrase, ledger_sequence,
  table_name)` before the replayed rows are inserted
- `id` is stored as a deterministic contract field, not as the conflict target
  for canonical tables

Environment:

- `POSTGRES_DSN`, required
- `POSTGRES_MAX_OPEN_CONNS`, default `8`
- `POSTGRES_MAX_IDLE_CONNS`, default `POSTGRES_MAX_OPEN_CONNS`
- `POSTGRES_CONN_MAX_LIFETIME`, default `30m`
- `PORT`, default `:50052`
- `HEALTH_PORT`, default `8089`

Startup fails if `POSTGRES_DSN` is empty or if `PingContext` cannot reach the
database. The sink intentionally does not parse Stellar XDR. It writes rows from
`LedgerBatch`.
