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
- `ledger-fixture-recorder`: converts JSONL batches into hashed,
  length-delimited protobuf fixture corpora, with bounded reordering for the
  concurrent JSONL sink delivery path.
- `ingest-replay`: replays fixture corpora directly to the ingest RPC with
  deterministic live/future/catch-up cadence gates or bounded saturated
  micro-batch backfill.
- `ducklake-backfill-worker`: materializes one verified historical fixture
  range into immutable, hashed Parquet shard files without attaching to the
  shared catalog.
- `postgres-sink`: idempotently writes ledgers, transactions, and operations to Postgres.
- `ducklake-sink`: writes normalized ledger batches into a DuckLake catalog with history-loader-compatible typed bronze tables.
- `ducklake-gatekeeper`: verifies snapshot-pinned transformation proposals and atomically promotes accepted Silver output with provenance.

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

- `docs/current-state.md` — canonical topology and production-gate status
- `docs/parallel-file-backfill-implementation-plan.md` — proposed scalable
  full-history Bronze/Silver backfill and cutover path
- `docs/parallel-file-backfill-evidence-2026-08-05.md` — first real-ledger
  file-worker correctness, throughput, and memory evidence
- `docs/quickstart.md`
- `docs/rebuild-plan.md`
- `docs/architecture.md`
- `docs/event-contracts.md`
- `docs/flow-ops-bridge.md`
- `docs/nomad-migration.md`
