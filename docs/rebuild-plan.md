# obsrvr-stellar-components rebuild plan

## Purpose

Rebuild `obsrvr-stellar-components` as the home for reusable Stellar flowctl components that compose as:

```text
Source -> Processor -> Sink
```

The repo should replace source-coupled monoliths like `stellar-postgres-ingester` with independently deployable components that can fan out to multiple sinks.

## Primary direction

Use `raw-ledger-source` from `stellar-raw-ledger-origin` as the canonical raw ledger source.

```text
raw-ledger-source@0.2.2
  emits Event.Type = stellar.ledger.v1
  payload = protobuf stellar.v1.RawLedger
```

This repo should start with processors and sinks, not another raw ledger source.

## Reference repos

Use these as references:

- `/home/tillman/Documents/stellar-raw-ledger-origin`
  - flowctl source packaging
  - component metadata/image layout
  - raw ledger event contract
- `/home/tillman/Documents/nebu-processor-registry`
  - component-per-directory structure
  - proto-first processor design
  - schema docs and processor metadata patterns
  - reusable Stellar extraction logic examples
- `/home/tillman/Documents/ttp-processor-demo/obsrvr-lake/stellar-postgres-ingester`
  - existing Postgres writer and extraction behavior to migrate away from monolithic coupling
- `/home/tillman/Documents/infra/environments/prod/latitude/obsrvr-lake-testnet`
  - current Nomad deployment shape and operational constraints

Flowctl-specific references to keep close while implementing:

- `/home/tillman/Documents/flowctl`
  - authoritative runner/resolver behavior
  - component image and metadata rules
  - local process-driver and registry/image-path E2E behavior
  - especially:
    - `docs/component-image-spec.md`
    - `docs/building-components.md`
    - `internal/components/resolver.go`
    - `internal/components/metadata.go`
    - `internal/components/translator.go`
    - `internal/runner/pipeline_runner.go`
    - `internal/runner/stream_orchestrator.go`
    - `docker/contract-events-processor/metadata.json`
    - `docker/duckdb-consumer/metadata.json`
- `/home/tillman/Documents/flowctl-sdk`
  - canonical Go runtime wrappers for sources, processors, and consumers/sinks
  - examples for component process behavior and event payload handling
  - especially:
    - `pkg/source`
    - `pkg/processor`
    - `pkg/consumer`
    - `examples/contract-events-processor`
    - `examples/duckdb-consumer`
    - `examples/contract-events-pipeline/contract-events-pipeline.yaml`
- `/home/tillman/Documents/flow-proto`
  - authoritative protobuf definitions for flowctl envelopes/events
  - raw ledger payload type reference
  - especially:
    - `proto/flowctl/v1`
    - `proto/stellar/v1/raw_ledger.proto`

Avoid using the old archived `obsrvr-stellar-components` Arrow Flight implementation as the base. It had useful ideas, but it was stale and not aligned with the current flowctl event contract.

## Architectural principles

1. **Source stays raw**
   - `raw-ledger-source` only acquires ledger data.
   - It should not know about Postgres, DuckLake, Bronze tables, or product-specific projections.

2. **Processors own Stellar semantics**
   - Decode `stellar.v1.RawLedger`.
   - Parse `LedgerCloseMeta` XDR.
   - Extract normalized/canonical rows or derived events.

3. **Sinks only materialize**
   - Sinks should not parse Stellar XDR if avoidable.
   - They should consume typed normalized batches/events and write to storage.

4. **Fan-out is first-class**
   - One processor output should be consumable by multiple sinks:

   ```text
   raw-ledger-source
     -> stellar-ledger-processor
       -> postgres-sink
       -> ducklake-sink
       -> jsonl/debug-sink
   ```

5. **Proto-first contracts**
   - Define schemas in `.proto` before implementation.
   - Generate Go types.
   - Version event types and payloads explicitly.

6. **Ledger-bounded idempotency**
   - Prefer one output batch per ledger, or per bounded ledger range.
   - Sinks should be able to replay safely.

## Proposed event model

Prefer batch events as the primary output from the first processor.

Input:

```text
Event.Type: stellar.ledger.v1
Payload:    stellar.v1.RawLedger
```

Primary processor output:

```text
Event.Type: stellar.ledger.batch.v1
Payload:    stellar.components.v1.LedgerBatch
```

Example schema shape:

```protobuf
message LedgerBatch {
  string network_passphrase = 1;
  uint32 ledger_sequence = 2;
  int64 closed_at_unix = 3;
  string schema_version = 4;
  string extraction_version = 5;

  repeated LedgerRow ledgers = 10;
  repeated TransactionRow transactions = 11;
  repeated OperationRow operations = 12;
  repeated ContractEventRow contract_events = 13;
  repeated ContractInvocationRow contract_invocations = 14;
  repeated TokenTransferRow token_transfers = 15;
  repeated AccountEffectRow account_effects = 16;
}
```

Why batch-first:

- fewer events than one-event-per-row
- natural ledger commit boundary
- easier checkpointing and replay
- better fit for Postgres, DuckDB, DuckLake, Parquet, and object-storage sinks
- preserves cross-table consistency for a ledger

A later `row-fanout-processor` can explode batches into individual row events if needed.

## Proposed repository layout

```text
obsrvr-stellar-components/
  README.md
  docs/
    rebuild-plan.md
    architecture.md
    event-contracts.md
    nomad-migration.md

  proto/
    stellar/components/v1/
      ledger_batch.proto

  gen/
    go/
      ... generated protobuf code ...

  pkg/
    ledgerdecode/       # RawLedger -> LedgerCloseMeta helpers
    normalize/          # LedgerCloseMeta -> LedgerBatch extraction
    scval/              # shared ScVal conversion helpers
    ids/                # TOID / stable id helpers
    checkpoints/        # shared checkpoint abstractions if needed

  components/
    stellar-ledger-processor/
      cmd/component/
      metadata.json
      Dockerfile.flowctl
      README.md
      tests/

    postgres-sink/
      cmd/component/
      metadata.json
      Dockerfile.flowctl
      README.md
      migrations/
      tests/

    ducklake-sink/
      cmd/component/
      metadata.json
      Dockerfile.flowctl
      README.md
      tests/

    jsonl-sink/
      cmd/component/
      metadata.json
      Dockerfile.flowctl
      README.md

  pipelines/
    local-postgres.yaml
    local-ducklake.yaml
    fanout-postgres-ducklake.yaml

  scripts/
    lint.sh
    test-local-pipeline.sh
    generate-proto.sh
```

## Component roadmap

### 1. `stellar-ledger-processor`

Consumes raw ledger events and emits normalized ledger batches.

Responsibilities:

- consume `stellar.ledger.v1`
- unmarshal `stellar.v1.RawLedger`
- decode `LedgerCloseMeta` XDR
- extract canonical row batches
- emit `stellar.ledger.batch.v1`

Initial rows to support:

- ledgers
- transactions
- operations
- contract events
- contract invocations
- token transfers

Useful references:

- `nebu-processor-registry/processors/contract-events`
- `nebu-processor-registry/processors/token-transfer`
- `nebu-processor-registry/processors/contract-invocation`
- `obsrvr-lake/stellar-postgres-ingester/go/extractors*.go`

### 2. `jsonl-sink`

Build this early as a debugging sink.

Responsibilities:

- consume `stellar.ledger.batch.v1`
- write each batch or row as JSONL
- provide easy local inspection and golden test fixtures

### 3. `postgres-sink`

Replacement path for `stellar-postgres-ingester` storage behavior.

Responsibilities:

- consume `stellar.ledger.batch.v1`
- write normalized rows to Postgres
- support idempotent replay
- batch inserts/COPY where possible
- maintain checkpoint/watermark if needed

Migration target:

- current Nomad setup in `obsrvr-lake-testnet`
- current DB tables from `stellar-postgres-ingester`

### 4. `ducklake-sink`

New analytical materialization target.

Responsibilities:

- consume `stellar.ledger.batch.v1`
- write to DuckDB/DuckLake
- support partitioning by network and ledger range
- support replay/idempotency

### 5. Optional future processors

- `contract-event-processor`
- `token-transfer-processor`
- `invocation-graph-processor`
- `row-fanout-processor`
- `projection-processor`

These can consume normalized batches and emit more specialized derived streams.

## Goal-oriented delivery plan

The rebuild should be tracked as a set of system capabilities, not only as an ordered task list. Each goal below has an outcome, supporting implementation work, non-goals, and acceptance criteria.

### Goal 1: Establish the canonical ledger batch contract

Outcome: downstream components can rely on one versioned protobuf event for normalized Stellar ledger data.

Supporting implementation work:

- Bootstrap the repo with README, `.gitignore`, Go module/workspace strategy, and Makefile targets for `test`, `build`, `proto`, and `lint`.
- Create `proto/stellar/components/v1/ledger_batch.proto`.
- Decide exact row fields for the first cut.
- Generate Go code.
- Document event types in `docs/event-contracts.md`.

Non-goals:

- Do not build a replacement raw ledger source.
- Do not introduce row-level event types before the batch contract is proven.

Acceptance criteria:

- `stellar.components.v1.LedgerBatch` is defined in protobuf.
- The chosen event type is documented consistently as `stellar.ledger.batch.v1`.
- Generated Go types compile.
- Sample payload fixtures unmarshal cleanly.

### Goal 2: Prove the local raw-to-batch pipeline

Outcome: a local flow can read raw ledger events, normalize them, and write inspectable output.

Target flow:

```text
raw-ledger-source@0.2.2
  -> stellar-ledger-processor
  -> jsonl-sink
```

Supporting implementation work:

- Implement shared decode/extract packages:
  - `stellar.v1.RawLedger` -> `xdr.LedgerCloseMeta`
  - `xdr.LedgerCloseMeta` -> `LedgerBatch`
- Build `stellar-ledger-processor` with:
  - input type `stellar.ledger.v1`
  - output type `stellar.ledger.batch.v1`
  - flowctl processor wrapper
- Build `jsonl-sink` for local inspection and fixture generation.
- Add unit tests with small fixtures.
- Add golden output tests for JSONL.
- Add a local process-driver E2E with `raw-ledger-source` and `jsonl-sink`.

Non-goals:

- Do not couple JSONL output to Postgres or DuckDB schemas.
- Do not put Stellar XDR parsing in sinks.

Acceptance criteria:

- The source emits `stellar.ledger.v1`.
- The processor emits `stellar.ledger.batch.v1`.
- The sink receives and writes at least one ledger batch.
- Event payloads unmarshal cleanly.
- Replaying the same ledger produces stable ids/output.

### Goal 3: Replace the monolithic Postgres ingester

Outcome: `raw-ledger-source -> stellar-ledger-processor -> postgres-sink` can replace the current source-coupled `stellar-postgres-ingester` path.

Target flow:

```text
raw-ledger-source
  -> stellar-ledger-processor
  -> postgres-sink
```

Supporting implementation work:

- Build `postgres-sink`.
- Port only sink behavior from `stellar-postgres-ingester`.
- Keep extraction and Stellar semantics in the processor and shared packages.
- Validate against the existing testnet schema.
- Support idempotent replay.
- Use batch inserts or COPY where possible.
- Maintain sink checkpoint/watermark state where needed.
- Expose processor progress/watermark state.

Non-goals:

- Do not preserve the monolithic source-plus-writer coupling.
- Do not make Postgres-specific fields leak back into the canonical event contract unless they are genuinely canonical ledger fields.

Acceptance criteria:

- Postgres tables are compatible enough for this to act as a drop-in replacement.
- The sink consumes `stellar.ledger.batch.v1`.
- Replaying a ledger does not duplicate rows or corrupt state.
- The new Nomad shape replaces `stellar-live-source-datalake -> stellar-postgres-ingester` in testnet.

### Goal 4: Add analytical fan-out

Outcome: the same normalized ledger batch stream can feed an analytical materialization target without changing the source or processor.

Target flow:

```text
raw-ledger-source
  -> stellar-ledger-processor
    -> postgres-sink
    -> ducklake-sink
```

Supporting implementation work:

- Build `ducklake-sink`.
- Start with ledger batches written to DuckDB tables.
- Support partitioning by network and ledger range.
- Support replay/idempotency.
- Evolve from DuckDB into DuckLake catalog/materialization after the first target works.

Non-goals:

- Do not block the Postgres replacement on DuckLake-specific catalog design.
- Do not add a second processor path for analytics until the shared batch output proves insufficient.

Acceptance criteria:

- `ducklake-sink` consumes `stellar.ledger.batch.v1`.
- Ledger batches materialize into DuckDB tables.
- The same processor output can feed Postgres and DuckDB/DuckLake sinks.
- Replay behavior remains ledger-bounded and idempotent.

### Goal 5: Make packaging, deployment, and regression safety production-grade

Outcome: every component can be built, tested, packaged, deployed, and replayed repeatably.

Supporting implementation work:

- Add `Dockerfile.flowctl` and `metadata.json` for each component.
- Follow the component image layout proven in `stellar-raw-ledger-origin`:
  - binary at `/app/component`
  - metadata at `/metadata.json`
  - `metadata.json` declares type, input/output types, and env mapping
- Add fixture replay for known ledgers.
- Add process-driver E2E tests.
- Add registry/image-path E2E tests.
- Add idempotent sink replay tests.
- Document the Nomad migration in `docs/nomad-migration.md`.

Non-goals:

- Do not optimize image publishing before the component contracts and local flows pass.
- Do not broaden the migration beyond the testnet replacement path until replay/idempotency is proven.

Acceptance criteria:

- Each deployable component has a valid `metadata.json`.
- Each deployable component has a working `Dockerfile.flowctl`.
- Test, build, proto generation, and lint commands are available from the repo root.
- Process-driver E2E passes locally.
- Replay and idempotency tests pass for processor output and sinks.

## Resolved design decisions

1. Exact name of normalized batch event type:
   - `stellar.normalized.ledger_batch.v1`
   - `stellar.canonical.ledger_batch.v1`
   - `stellar.ledger.batch.v1`
   answer: `stellar.ledger.batch.v1`

2. Should `LedgerBatch` include all table-like rows in one payload, or split high-volume domains into multiple batch event types?
    answer: single ledger-bounded batch

3. How closely should initial Postgres schemas match the existing `stellar-postgres-ingester` tables?
    answer: the new Postgres path should be a drop-in replacement.

4. Should checkpoints live in sinks only, or should processors also expose progress/watermark state?
    answer: processors should also expose progress

5. Resolved: `ducklake-sink` writes directly to a DuckLake catalog through embedded DuckDB. It is not a DuckDB/Parquet staging sink.
    answer: start with DuckDB first.
