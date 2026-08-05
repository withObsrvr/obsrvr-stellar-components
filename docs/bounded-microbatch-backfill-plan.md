# Bounded Micro-Batch Backfill Implementation Plan

**Date:** 2026-08-04
**Status:** Proposed; implementation has not started
**Scope:** `BronzeIngestService`, `quack-ducklake-server`, `ducklake-sink`,
Flow consumer delivery, checkpoint coordination, replay tooling, and backfill
operations

## Decision summary

Live ingest and historical backfill will remain two explicit operating modes.

- **Live** keeps the current contract: one ledger in flight, one ledger per
  DuckLake transaction, and one acknowledgement after that ledger is durable.
- **Backfill** uses one ordered, bounded micro-batch in flight. Multiple
  contiguous ledgers stage together and commit in one DuckLake transaction.
  The server acknowledges the committed ledger range, not each ledger
  independently.
- Both modes use one server-owned DuckDB writer. Opening more concurrent write
  streams is not the scaling mechanism.
- Backfill boundaries are limited simultaneously by ledger count, encoded
  bytes, decoded rows, and age. Count alone is unsafe because Stellar ledger
  sizes vary substantially.
- Live and backfill are mutually exclusive for a catalog in the first release.
  Cutover changes the server from backfill admission to live admission after a
  final parity and checkpoint gate.
- Fixed candidate sizes are benchmarked first. Adaptive batch sizing is a
  follow-up only if fixed bounds cannot meet the throughput and pause targets.

This is an additive design. The existing live RPC and its latency semantics do
not change.

## Why a separate mode is required

The 2026-08-04 direct fixture run sent 1,000 recent pubnet ledgers through the
existing one-ledger-in-flight path in `308.818s`, or approximately `3.238`
ledgers per second. Extrapolating that sink-only rate to ledger `63,804,680`
gives:

```text
63,804,680 * 308.818s / 1,000 = 19,704,034s = 228.1 days
```

If the target keeps advancing at one ledger every five seconds, the same rate
would take approximately 243 days to catch the moving tip. This is not a
production forecast: early ledgers are generally smaller, while a real run
also pays archive-fetch and processor costs omitted by direct fixture replay.
It does show that the current transaction grain cannot be the full-history
plan.

That timing was collected with DuckDB 1.5.4. It is the comparison baseline,
not a result to carry forward: every micro-batch candidate must be rerun with
the DuckDB 1.5.5-compatible engine, Go bindings, DuckLake, and Quack set before
limits are selected.

The 1,000-ledger catalog also advanced to approximately snapshot 1,008. A
full-history run at one transaction per ledger would therefore create on the
order of 63.8 million DuckLake snapshots before snapshot expiry. Micro-batching
must improve both write throughput and snapshot amplification.

## Goals

1. Preserve ledger-level logical correctness while amortizing staging,
   transfer, commit, and catalog work across a bounded range.
2. Bound memory, unacknowledged replay scope, transaction duration, WAL growth,
   checkpoint pauses, and shutdown time.
3. Resume safely after a client crash, server crash, lost acknowledgement,
   uncertain commit, or checkpoint interruption.
4. Produce a deterministic parity comparison with the existing one-ledger
   path over the same fixture corpus.
5. Make a clean backfill-to-live cutover possible without dual writers or a
   catalog rewrite.
6. Select batch limits from retained measurements using a current, compatible
   DuckDB, DuckLake, Quack, and `duckdb-go` set.

## Non-goals for the first release

- Parallel DuckLake writers against one catalog
- Concurrent live and historical writes into the same catalog
- Adaptive batch sizing before fixed candidates are understood
- Out-of-order or sparse-range backfill
- Replacing the live `<400ms` arrival-to-queryable contract
- Treating external Quack writes as coordinated with ingest
- Running a serving replica continuously during initial full-history loading

## Correctness invariants

The implementation must retain these invariants:

1. A ledger's typed rows, `ledger_batches` row, and `ingest_watermarks` row are
   committed atomically.
2. A micro-batch is also atomic: either every ledger in its declared range is
   visible or none is.
3. Acknowledgements are sent only after commit.
4. The input range is strictly increasing, contiguous, and from one Stellar
   network.
5. A committed-but-unacknowledged micro-batch can be retried without duplicate
   rows or watermarks.
6. No checkpoint or internally coordinated maintenance operation overlaps an
   ingest transaction.
7. Resource limits are enforced by the server from observed data, not trusted
   client declarations.
8. The backfill source has a fixed start, fixed target end, and durable source
   identity. A moving tip is handled only after switching to live mode.

## Current constraints that implementation must address

### Single writer

`quack-ducklake-server` owns a dedicated ingest `sql.Conn` and serializes it
with the checkpoint controller through `writerCoordinator`. Multiple
outstanding RPCs would still queue behind that writer and could increase memory
and lock contention without increasing DuckLake write concurrency.

### Native staging

The fast path decodes a ledger into `memory.bronze` tables using the DuckDB
Appender, then transfers each populated table with `INSERT ... SELECT`. A
DuckDB transaction may write to only one attached database, so all native
staging for a micro-batch must complete before its DuckLake transaction begins.
This makes byte and row limits mandatory.

### Flow delivery is not currently bounded

The vendored Flow consumer receives an event and immediately starts an
unbounded goroutine for its handler. The sink then serializes those handlers
with a local one-slot channel. Under a saturated full-history source, this can
accumulate blocked goroutines and retained event payloads. The SDK also sends
its end-of-stream response without first waiting for all handlers to finish.

Backfill cannot be production-safe until the Flow delivery layer:

- acquires a bounded permit before receiving or dispatching more work
- waits for all accepted handlers before reporting stream completion
- returns accurate success/failure counts after those handlers finish
- supports an ordered maximum-in-flight setting large enough to assemble one
  micro-batch

The preferred fix is a Flow SDK release with bounded ordered delivery. A local
specialized consumer is an acceptable temporary implementation only if the SDK
change cannot land in time; it must implement the same contract and must not
silently fall back to unbounded fan-out.

### Decode concurrency

`bronze.DecodeTypedRows` currently starts up to eight workers per ledger.
Calling it concurrently for an entire micro-batch would multiply goroutines and
memory. Backfill needs a shared worker budget across the range rather than a
nested worker pool for every ledger.

## Target architecture

```text
archive source, fixed [start, end]
  -> stellar-ledger-processor
  -> bounded ordered Flow delivery
  -> ducklake-sink (INGEST_PROFILE=backfill)
       -> micro-batch assembler
          bounds: ledgers + protobuf bytes + bronze rows + age
       -> IngestLedgerMicroBatches stream
          one explicitly framed range in flight
          -> quack-ducklake-server (INGEST_PROFILE=backfill)
               -> bounded decode worker pool
               -> native memory staging
               -> one range-replacement DuckLake transaction
                    typed Bronze rows
                    ledger metadata + watermarks
                    micro-batch commit receipt
               -> committed-range acknowledgement
               -> checkpoint/maintenance admission between ranges

cutover:
  stop backfill at fixed target
  -> parity + gaps + receipt checks
  -> coordinated metadata checkpoint
  -> switch INGEST_PROFILE=live
  -> resume existing one-ledger RPC at target + 1
```

## Protocol design

Do not weaken or reinterpret `IngestLedgerBatches`. Add a new bidirectional RPC
to `BronzeIngestService`, tentatively named `IngestLedgerMicroBatches`.

Each micro-batch is explicitly framed by the client:

```protobuf
message IngestMicroBatchBegin {
  string micro_batch_id = 1;
  uint32 ledger_start = 2;
  uint32 ledger_end = 3;
  uint32 ledger_count = 4;
  uint64 encoded_bytes = 5;
  uint64 bronze_rows = 6;
  string payload_sha256 = 7;
}

message IngestMicroBatchRequest {
  oneof payload {
    IngestMicroBatchBegin begin = 1;
    LedgerBatch batch = 2;
    IngestMicroBatchCommit commit = 3;
  }
}

message IngestMicroBatchCommit {}

message IngestMicroBatchAck {
  string micro_batch_id = 1;
  uint32 ledger_start = 2;
  uint32 ledger_end = 3;
  uint32 ledger_count = 4;
  bool replayed = 5;
  bool deduplicated = 6;
}
```

The final names can change during implementation, but these semantics should
not:

- `begin` declares one immutable range and its digest.
- exactly the declared contiguous ledger messages follow
- `commit` closes the frame; no second frame starts before its acknowledgement
- the server recomputes count, bounds, byte size, row count, network, and digest
- malformed, oversized, mixed-network, duplicate, or non-contiguous frames are
  rejected before a DuckLake transaction starts
- an empty catalog may begin at the job's declared start; after that, a new
  frame must begin at the committed high watermark plus one, except for an
  identical receipt retry
- the stream may carry sequential frames, but the initial window is exactly
  one unacknowledged micro-batch

`micro_batch_id` should be derived from the network passphrase, ledger bounds,
and SHA-256 of length-delimited deterministic protobuf encodings. A retry must
reuse the same identifier and payload.

Individual `LedgerBatch` messages remain below the existing per-message gRPC
limit. The protocol deliberately streams ledger messages instead of creating a
single hundreds-of-megabytes repeated-protobuf request.

## Server execution model

### Admission

Add `INGEST_PROFILE=live|backfill`, defaulting to `live`.

- In `live`, the existing RPC is admitted and the backfill RPC fails closed.
- In `backfill`, one backfill stream owns catalog ingest admission and the live
  RPC fails with `FailedPrecondition` or `ResourceExhausted`.
- A second backfill stream is rejected rather than queued indefinitely.
- Generic Quack writes remain outside this guarantee and must be operationally
  disabled during the first production backfill.

This also fixes the current possibility of multiple ingest streams racing the
server's in-memory high watermark.

### Bounded preparation

The server validates the frame as it arrives and accounts for actual:

- ledger count
- deterministic protobuf bytes
- Bronze row count
- oldest buffered ledger age
- total buffered memory estimate

Decoding uses one server-wide worker pool with a configured maximum. Prepared
results retain ledger order. Table names are sorted before staging and transfer
so statement order is stable across runs.

Only CPU decoding may overlap a checkpoint. Native staging and the DuckLake
transaction use the dedicated ingest connection and writer coordinator.

### Transaction algorithm

For one accepted frame:

1. Check for an existing identical commit receipt. If present, return a
   deduplicated acknowledgement without rewriting data.
2. Decode every ledger using the bounded worker pool.
3. Union the populated typed-table specifications across the range.
4. Acquire the writer coordinator.
5. Clear those native staging tables once.
6. Append every decoded row for the range into its `memory.bronze` table.
7. Begin one DuckLake transaction.
8. Verify the catalog network.
9. If the range may already exist or a prior commit was uncertain, delete the
   entire declared ledger range from metadata, watermarks, typed tables, and
   any overlapping micro-batch receipt.
10. Insert one `ledger_batches` row and one watermark per ledger.
11. Transfer each populated typed table once with `INSERT ... SELECT`.
12. Insert the micro-batch commit receipt.
13. Commit once.
14. Update the in-memory watermark only after commit succeeds.
15. Clear staging best-effort; the next frame always clears defensively.
16. Release the writer coordinator.
17. Send one range acknowledgement.

Add range-oriented helpers in `pkg/bronze` rather than looping the existing
single-ledger delete helper. Every delete must use the identical inclusive
`[ledger_start, ledger_end]` boundary.

The first implementation should optimize the fresh, append-only full-history
case while retaining range replacement for uncertain retries. It should reject
arbitrary sparse corrections; those remain a separate replay operation.

### Commit receipts

Add an ordered migration for a table shaped like:

```sql
CREATE TABLE bronze.ingest_microbatch_commits (
  network_passphrase VARCHAR NOT NULL,
  micro_batch_id VARCHAR NOT NULL,
  ledger_start UBIGINT NOT NULL,
  ledger_end UBIGINT NOT NULL,
  ledger_count UINTEGER NOT NULL,
  payload_sha256 VARCHAR NOT NULL,
  committed_at TIMESTAMP NOT NULL
);
```

The receipt is written in the same transaction as the range. It is not a
second checkpoint and must not be persisted by the client as a substitute for
catalog state.

If the commit succeeds but the acknowledgement is lost, a retry finds the
identical receipt and can acknowledge without rewriting. Reusing an identifier
with different bounds or digest is a hard error. Receipt retention must cover
the complete active backfill and its cutover verification.

## Client and Flow integration

The backfill sink owns a range assembler. Each Flow handler submits a ledger
and blocks until the range containing it is durably acknowledged. The assembler
orders the submissions by ledger sequence and flushes on the first reached
bound:

```text
target ledger count
maximum encoded protobuf bytes
maximum Bronze rows
maximum batch age
source end / graceful drain
```

The client computes the deterministic digest, sends one explicit frame, and
retries that exact frame after a stream failure or uncertain acknowledgement.
It must never regroup an uncertain frame because the server receipt and replay
scope are defined by the original identifier.

The Flow maximum-in-flight value must be at least the target ledger count but
no larger than the sink's total queue capacity. The permit is acquired before
the SDK receives or dispatches the next event, creating real transport
backpressure rather than a growing set of blocked goroutines.

Live mode retains maximum in flight `1` and the current per-ledger client. No
backfill buffer exists in the live process.

## Checkpoint and maintenance behavior

Saturated backfill has no idle window. Checkpoint work must therefore become an
explicit admission pause between micro-batch transactions.

The current `TryLock` scheduler can repeatedly lose to a saturated writer. Add
a priority handoff for the hard limit:

1. The WAL poller observes the hard limit or the ingest loop observes it after
   a commit.
2. The coordinator marks a checkpoint pending before the next range can acquire
   writer admission.
3. The current range completes and is acknowledged.
4. Health reports degraded/backfill-paused.
5. The metadata checkpoint runs with its existing timeout and retry policy.
6. Admission resumes only after success; persistent failure remains unhealthy
   and stops new commits.

At the soft limit during saturation, record a `backfill_saturated` deferral and
continue until either a natural gap appears or the hard boundary is reached.
The DuckDB automatic threshold stays above the hard controller limit as the
emergency fallback.

External `ducklake-maintenance` currently writes through Quack outside the
writer coordinator. The first full backfill must not run it concurrently by
assumption. Choose one of these from measurements:

- route a bounded maintenance operation through server-owned admission between
  micro-batches; preferred for production
- pause backfill explicitly, run maintenance, then resume

Benchmark both no-maintenance throughput and the selected bounded-maintenance
cadence. Snapshot expiry and physical cleanup remain disabled until backfill
parity is retained and downstream snapshot consumers have established new
checkpoints.

## Configuration candidates

These are experiment inputs, not final defaults:

```text
INGEST_PROFILE=live|backfill                 # default live
BACKFILL_TARGET_LEDGERS_PER_COMMIT=25        # client request
BACKFILL_MAX_LEDGERS_PER_COMMIT=100          # server cap
BACKFILL_MAX_ENCODED_BYTES=256MiB             # server-enforced actual bytes
BACKFILL_MAX_BRONZE_ROWS=500000               # server-enforced actual rows
BACKFILL_MEMORY_BUDGET=2GiB                   # Go-side frame/preparation cap
BACKFILL_MAX_BATCH_AGE=2s                     # flush a partial range
BACKFILL_MAX_QUEUED_MICROBATCHES=2            # collection + active frame
BACKFILL_DECODE_WORKERS=8                     # total, not per ledger
BACKFILL_COMMIT_TIMEOUT=2m
BACKFILL_FLOW_MAX_INFLIGHT=100
```

Configuration validation must reject:

- backfill limits that exceed the process memory budget
- queue capacity smaller than a target micro-batch
- live and backfill admission enabled together
- a DuckDB automatic checkpoint threshold at or below the explicit hard limit
- non-positive count, byte, row, age, worker, or timeout limits

The retained report must include every effective value. A server may clamp a
client target to its cap, but it must return and log the effective bounds rather
than silently changing them.

`BACKFILL_MEMORY_BUDGET` is separate from DuckDB's `QUACK_MEMORY_LIMIT`. The
Nomad allocation must cover both budgets plus runtime and extension headroom;
neither limit may be inferred to protect the other.

## Telemetry

Keep labels bounded; ledger sequences and micro-batch identifiers belong in
logs and reports, not Prometheus labels.

Server metrics:

```text
obsrvr_ducklake_backfill_microbatches_total{result,replayed,deduplicated}
obsrvr_ducklake_backfill_ledgers_total{result}
obsrvr_ducklake_backfill_rows_total
obsrvr_ducklake_backfill_encoded_bytes_total
obsrvr_ducklake_backfill_phase_seconds{
  phase="decode|staging|preface|transfer|commit|cleanup|total"
}
obsrvr_ducklake_backfill_microbatch_ledgers
obsrvr_ducklake_backfill_microbatch_bytes
obsrvr_ducklake_backfill_microbatch_rows
obsrvr_ducklake_backfill_buffered_ledgers
obsrvr_ducklake_backfill_buffered_bytes
obsrvr_ducklake_backfill_last_ledger
obsrvr_ducklake_backfill_retries_total{reason}
obsrvr_ducklake_backfill_admission_rejected_total{reason}
obsrvr_ducklake_backfill_checkpoint_pause_seconds{result}
```

Client metrics:

```text
obsrvr_ducklake_backfill_rpc_round_trip_seconds
obsrvr_ducklake_backfill_assembly_seconds
obsrvr_ducklake_backfill_queue_depth
obsrvr_ducklake_backfill_range_retries_total
```

Every benchmark report should also record:

- ledgers/second, Bronze rows/second, encoded MiB/second
- transaction and checkpoint duration percentiles
- snapshots created and ledgers per snapshot
- catalog/WAL bytes and Parquet file counts
- peak RSS and configured DuckDB memory limit
- time spent fetching, processing, queued, staging, committing, checkpointing,
  and maintaining
- projected fixed-tip and moving-tip completion time
- exact DuckDB, `duckdb-go`, DuckLake, and Quack versions/build hashes

## Test plan

### Unit tests

- deterministic framing digest and identifier
- count/byte/row/age flush boundaries
- contiguous ordering and bounded reorder behavior
- mixed-network, gap, duplicate, and oversized-frame rejection
- range SQL uses identical inclusive bounds for every table
- commit receipt match, conflict, and deduplication
- uncertain commit reuses the original frame
- bounded decode worker count
- one active stream and live/backfill mutual exclusion
- hard-limit checkpoint priority prevents writer barging
- graceful drain flushes the final partial range
- Flow EOF waits for accepted handlers and reports their final results

### Local integration matrix

Use the same hash-verified 1,000-ledger pubnet fixture corpus for every candidate.
Run at least:

```text
ledger target:  1, 5, 10, 25, 50, 100
byte cap:       64MiB, 128MiB, 256MiB, 512MiB
inline limit:   0, 256, 1024
checkpoint:     disabled, soft deferral + hard pause
maintenance:    disabled, selected bounded cadence
```

Prune combinations that hit a byte or memory cap; record the effective batch
distribution rather than pretending the requested count was reached.

For every retained candidate:

1. ingest into a fresh catalog
2. compare all logical tables with the one-ledger baseline using `EXCEPT ALL`
   in both directions
3. verify watermark count, min, max, and zero gaps
4. verify one metadata row per ledger and no duplicate typed keys where the
   source contract requires uniqueness
5. verify receipt count and exact range coverage
6. measure snapshots created and file amplification
7. resume with the next range

### Chaos matrix

Kill or disconnect at each boundary:

1. while receiving a frame
2. during decode
3. during native staging
4. after transaction begin but before commit
5. during commit
6. after commit but before acknowledgement
7. during a hard-limit checkpoint pause
8. during bounded maintenance
9. during graceful drain of the final partial range

Every case must restart, resend only the bounded uncertain range, converge to
the baseline, retain zero gaps/partial ledgers, and continue with the next
range.

### Representative-history sampling

The recent 1,000-ledger fixture is useful for a high-density test but is not a
weighted full-history model. Capture smaller hash-verified samples from early,
middle, and recent mainnet eras. Use their actual ledger-size distribution to
produce a weighted completion forecast. Report both archive/processor/sink
end-to-end throughput and direct RPC throughput so the true bottleneck is
visible.

## Initial acceptance gates

Correctness gates are hard requirements:

```text
watermark gaps:                       0
logical parity differences:           0
partial ledger commits:               0
partial micro-batch commits:           0
duplicate rows after uncertain retry: 0
successful resume after every fault:  required
peak memory above configured bound:    0
```

Starting performance targets:

```text
minimum direct-ingest throughput:      10 ledgers/s
goal direct-ingest throughput:         50 ledgers/s or better
snapshot amplification reduction:     at least 10x vs one-ledger commits
hard-limit recovery:                   <30s
candidate p99 micro-batch transaction: <5s
candidate max range receive-to-ack:     <30s
candidate hard checkpoint pause:       <3s
unacknowledged replay scope:           <= one effective micro-batch
```

Ten ledgers per second still implies about 73.8 days for a fixed 63.8-million
ledger target; 50 ledgers per second implies about 14.8 days. If the bounded
native-staging design cannot approach the goal without exceeding memory or
transaction bounds, the next experiment is file-backed staging for larger
transactions, not unbounded in-memory batching.

The benchmark selects final settings. The candidate values in this document do
not become production defaults merely because correctness passes.

## Rollout and cutover

1. Deploy the additive server RPC disabled, with `INGEST_PROFILE=live`; prove
   no live latency or correctness regression.
2. Run direct fixture benchmarks against a disposable catalog and select the
   count/byte/row bounds.
3. Land bounded Flow delivery and the backfill sink client; repeat the same
   corpus end to end.
4. Run a multi-era, multi-thousand-ledger shadow backfill with checkpoint and
   bounded-maintenance pauses.
5. Start the real backfill into a new primary catalog with a fixed target end.
   Do not serve it and do not run a concurrent live writer.
6. Periodically retain progress, resource, checkpoint, and parity reports.
7. Near cutover, choose ledger `C`, stop the old live writer after `C`, and
   finish the new catalog exactly through `C`.
8. Run full watermark, range, table-parity, receipt-coverage, and network gates.
9. Run a coordinated metadata checkpoint and record recovery evidence.
10. Restart the new server with `INGEST_PROFILE=live`, point the existing live
    sink at it, and begin with `C + 1`.
11. Establish or rebuild the serving replica from the new primary only after
    primary cutover succeeds.
12. Keep the old catalog read-only for rollback until the observation window
    and serving parity gates pass.

Rollback before step 10 is simply abandoning the new catalog. After step 10,
stop admission, record the last acknowledged ledger, and either resume the old
writer from that boundary or replay the bounded new range into the old primary.

## Incremental delivery

### PR 1 — Protocol and bounded server transaction core

- additive protobuf framing and range acknowledgement
- backfill admission mode and single-session ownership
- range validation, digest, and commit-receipt migration
- range staging/transfer/commit implementation
- bounded decode worker pool
- unit tests and a disabled-by-default server path

### PR 2 — Backfill client and bounded Flow delivery

- Flow SDK bounded ordered delivery and EOF drain semantics
- SDK dependency upgrade in this repository
- sink range assembler with count/byte/row/age limits
- stable uncertain-range retry and graceful drain
- client/server telemetry

If the SDK release is blocked, make the specialized local consumer explicit in
this PR and track removal; do not hide it behind the existing unbounded helper.

### PR 3 — Backfill checkpoint and maintenance admission

- hard-limit priority handoff in the writer coordinator
- saturated-soft-limit deferral telemetry
- degraded health during intentional hard pauses
- server-owned or explicitly paused bounded maintenance
- checkpoint and maintenance chaos regression

### PR 4 — Benchmark and release gate

- `ingest-replay` micro-batch transport
- retained candidate matrix and one-ledger parity baseline
- snapshot/file/WAL/RSS reporting
- fault injection at receive/stage/commit/ack/checkpoint boundaries
- multi-era fixture sampling and weighted completion forecast
- selected production limits and Nomad configuration

### PR 5 — Full-history runbook and cutover rehearsal

- fixed-target backfill orchestration
- durable progress reports and resume commands
- clean-catalog rehearsal through backfill-to-live transition
- serving-replica rebuild and rollback rehearsal
- operator sign-off checklist

## Dependencies and risks

- Begin implementation from the DuckDB 1.5.5-compatible dependency set in PR
  #14 or rebase after it merges. Extension binaries must come from the matching
  DuckDB extension namespace; never compare performance across an ABI mismatch.
- The Flow SDK bounded-delivery change is a production dependency, not an
  optional optimization.
- Larger transactions reduce snapshots but increase memory, retry scope, commit
  time, WAL increments, and time before progress is acknowledged.
- Combining ledgers changes DuckLake inlining and Parquet file behavior per
  `INSERT ... SELECT`; maintenance settings must be selected from the resulting
  file counts.
- Millions of commit receipts or retained snapshots may still be too much at a
  small selected range. Measure catalog metadata growth and snapshot expiry
  cost, not only ingest throughput.
- Remote Quack maintenance/materialization remains outside the writer
  coordinator until explicitly moved behind admission.
- A clean full-history catalog may expose schema or historical-data variants
  absent from the recent fixture. Multi-era samples are required before a
  completion estimate is credible.

## Definition of done

The mode is complete when a clean catalog can ingest a fixed historical range
through bounded Flow delivery and the new RPC, survive every declared crash
boundary, match the one-ledger baseline exactly, remain within configured
memory/WAL/transaction bounds, checkpoint and maintain through explicit bounded
pauses, resume deterministically, and cut over to the unchanged live RPC at the
next ledger. The final evidence must state the measured throughput and projected
full-mainnet duration; it must not infer either from configured batch size.
