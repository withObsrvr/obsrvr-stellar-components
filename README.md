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
- `ducklake-backfill-worker`: streams one verified historical range into
  rolled, immutable, hashed Parquet parts without attaching to the shared
  catalog. `--writer=arrow-parquet` uses bounded Arrow row groups with no
  DuckDB staging database; `--writer=duckdb-appender` is the parity oracle and
  rollback. `--source=ledger-stream` reads raw XDR directly from the configured
  SDK stream, performs one extraction decode, and sends contract events from
  typed extraction directly to Arrow without `LedgerBatch`, row JSON,
  reflection, or generic SQL-value conversion. Use
  `make test-file-backfill-benchmark` with `BACKFILL_SOURCE`, an exact range,
  and `BACKFILL_CONCURRENCY` to record aggregate throughput and phase evidence.
- `postgres-sink`: idempotently writes ledgers, transactions, and operations to Postgres.
- `ducklake-sink`: writes normalized ledger batches into a DuckLake catalog with history-loader-compatible typed bronze tables.
- `ducklake-gatekeeper`: verifies snapshot-pinned transformation proposals and atomically promotes accepted Silver output with provenance.

The direct archive benchmark keeps the source layout explicit:

```bash
BACKFILL_SOURCE=ledger-stream \
BACKFILL_LEDGER_START=62080000 \
BACKFILL_LEDGER_END=62080119 \
BACKFILL_CONCURRENCY=4 \
BACKFILL_WRITER=arrow-parquet \
BACKEND_TYPE=ARCHIVE \
ARCHIVE_STORAGE_TYPE=S3 \
ARCHIVE_BUCKET_NAME=aws-public-blockchain \
ARCHIVE_PATH=v1.1/stellar/ledgers/pubnet \
AWS_REGION=us-east-2 \
LEDGERS_PER_FILE=1 \
FILES_PER_PARTITION=64000 \
BUFFER_SIZE=1000 \
NUM_WORKERS=50 \
NETWORK_PASSPHRASE='Public Global Stellar Network ; September 2015' \
make test-file-backfill-benchmark
```

The Obsrvr GCS archive uses a separate bucket and object prefix:

```bash
ARCHIVE_STORAGE_TYPE=GCS \
ARCHIVE_BUCKET_NAME=obsrvr-stellar-ledger-data-pubnet-data \
ARCHIVE_PATH=landing/ledgers/pubnet \
LEDGERS_PER_FILE=1 \
FILES_PER_PARTITION=64000
```

Use `BACKFILL_COMPRESSION=snappy` for the measured throughput profile or keep
the default `zstd` when minimizing persisted bytes is more important.

Raw Arrow workers can overlap source acquisition and extraction without
changing artifact order. The measured single-host profile is:

```bash
BACKFILL_WRITER=arrow-parquet \
BACKFILL_COMPRESSION=snappy \
BACKFILL_EXTRACT_WORKERS=4 \
BACKFILL_MAX_INFLIGHT_LEDGERS=8 \
BACKFILL_DECODE_WORKERS=2 \
BACKFILL_PARQUET_WRITERS=2 \
BACKFILL_MAX_PENDING_ROW_GROUPS=4
```

The source XDR is borrowed, so the pipeline copies it before the next archive
read. `BACKFILL_MAX_INFLIGHT_LEDGERS` bounds those owned copies, decoded
ledgers, and out-of-order results together. See
[`docs/bounded-arrow-pipeline-evidence-2026-08-05.md`](docs/bounded-arrow-pipeline-evidence-2026-08-05.md)
for the extraction configuration knee and
[`docs/parallel-arrow-writer-evidence-2026-08-05.md`](docs/parallel-arrow-writer-evidence-2026-08-05.md)
for the bounded writer result.

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
- `docs/parallel-arrow-writer-evidence-2026-08-05.md` — bounded parallel
  row-group encoding, determinism, and host-scaling evidence
- `docs/quickstart.md`
- `docs/rebuild-plan.md`
- `docs/architecture.md`
- `docs/event-contracts.md`
- `docs/flow-ops-bridge.md`
- `docs/nomad-migration.md`
