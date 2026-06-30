# obsrvr-stellar-components

Reusable Stellar flowctl components that compose as:

```text
raw-ledger-source@0.2.2 -> stellar-ledger-processor -> sinks
```

This repository is Nix flake based. Use the flake for the pinned Go and protobuf toolchain:

```bash
nix develop
make proto
make test
make build
```

## Components

- `stellar-ledger-processor`: consumes `stellar.ledger.v1`, emits `stellar.ledger.batch.v1`.
- `jsonl-sink`: writes normalized ledger batches as protobuf JSONL fixtures.
- `postgres-sink`: idempotently writes ledgers, transactions, and operations to Postgres.
- `ducklake-sink`: writes DuckDB-loadable partitioned JSONL as the first DuckLake materialization step.

## Contracts

The canonical normalized batch event is:

```text
Event.Type: stellar.ledger.batch.v1
Payload:    stellar.components.v1.LedgerBatch
```

The raw ledger source stays external:

```text
raw-ledger-source@0.2.2
  Event.Type: stellar.ledger.v1
  Payload:    stellar.v1.RawLedger
```

See:

- `docs/rebuild-plan.md`
- `docs/architecture.md`
- `docs/event-contracts.md`
- `docs/nomad-migration.md`
