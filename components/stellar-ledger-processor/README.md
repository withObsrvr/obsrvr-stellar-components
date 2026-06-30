# stellar-ledger-processor

Consumes `stellar.ledger.v1` events from `raw-ledger-source@0.2.2`, decodes `stellar.v1.RawLedger`, parses `LedgerCloseMeta` XDR, and emits `stellar.ledger.batch.v1` protobuf payloads.

The batch includes compatibility rows for ledgers, transactions, and operations, plus `bronze_rows` for the full `stellar-extract` surface used by the legacy history loader and Postgres ingester.

Required environment:

- `NETWORK_PASSPHRASE`

Optional environment:

- `PORT`, default `:50051`
- `HEALTH_PORT`, default `8088`
- `ENABLE_FLOWCTL`, default `false`
- `FLOWCTL_ENDPOINT`
- `COMPONENT_ID`
