# Parallel File Backfill Implementation Plan

**Date:** 2026-08-05
**Status:** Native Arrow worker, bounded extraction, and bounded row-group encoding passed; registration remains open
**Target stack:** DuckDB 1.5.5 with matching current DuckLake and Quack
extensions

This plan replaces the single-writer RPC as the full-history backfill data
plane. It keeps the bounded micro-batch path as the correctness-preserving
bridge for tail catch-up, restart recovery, and final cutover.

The decision is deliberately narrow:

- Live ingest remains ordered, ledger-bounded, and latency-oriented.
- Tail catch-up uses bounded micro-batches and durable range receipts.
- Full-history backfill becomes parallel production of immutable Parquet
  files followed by serialized, transactional metadata registration.
- Silver backfills use the same file-manifest protocol under Gatekeeper.
- Workers never attach to or write the shared DuckLake catalog.

This is a candidate-catalog build. Bulk workers and the registration
coordinator operate on a catalog that is not serving production traffic. The
catalog is exposed only after full coverage, parity, recovery, and cutover
gates pass.

## Implementation checkpoint — 2026-08-05

The first PR 1 slice now exists on
`feature/tmosley/parallel-file-backfill`:

- `internal/backfillmanifest` defines canonical job, shard, file, and result
  contracts; deterministic shard and generation digests; exact job coverage;
  strict range, URI, hash, schema, ordering, and table-count validation; and
  divergent retry detection.
- `internal/backfillworker` owns a disposable local DuckDB database, loads
  typed Bronze rows through table-specific Appenders, adds a staging-only
  ordinal as a duplicate-row tie-breaker, writes canonically ordered Zstd
  Parquet, fingerprints and hashes each file, and publishes it without
  overwriting an existing artifact.
- `cmd/ducklake-backfill-worker` now provides a bounded runnable entrypoint
  from a verified fixture range to job/result manifests and complete typed
  Bronze, ledger metadata, and watermark Parquet artifacts.
- The same worker now has a `ledger-stream` input lane pinned to the source and
  extraction PR commits. It consumes the SDK's borrowed raw XDR, performs one
  parsed compatibility decode, projects typed extraction rows with a bounded
  worker budget, and never creates a `LedgerBatch` protobuf or per-row JSON.
- Fresh extraction exposed nondeterministic state-row traversal that recorded
  fixtures had hidden. Canonical public-column ordering now makes logically
  identical retries byte-stable without leaking staging columns.
- Tests prove stable Parquet hashes across two independent worker runs, exact
  typed schema, no staging-column leakage, range rejection, overwrite
  rejection, cleanup of partial files, manifest coverage, and retry identity.

A 30-ledger mainnet smoke produced 18 byte-stable files and exact source/table
count parity with zero watermark gaps. The measurements and limitations are
recorded in
[`parallel-file-backfill-evidence-2026-08-05.md`](parallel-file-backfill-evidence-2026-08-05.md).

This is not yet a complete distributed shard worker. Streaming decode with a
hard memory limit, size-based file rolling, direct archive input,
attempt-scoped object-store publication, independent validation, and DuckLake
registration remain open.

## Why a second backfill lane is necessary

The first bounded micro-batch implementation improves transaction efficiency
and proves useful failure semantics, but it does not make a single DuckDB
writer horizontally scalable. On the recent-mainnet 1,000-ledger fixture:

| Path | Throughput | Transactions | Full 63,804,680-ledger projection |
|---|---:|---:|---:|
| one ledger per transaction | 3.049 ledgers/s | 1,000 | 242.2 days |
| 25-ledger bounded micro-batch | 9.561 ledgers/s | 45 | 77.2 days |
| 50-ledger bounded micro-batch | 10.013 ledgers/s | 22 | 73.8 days |

The evidence is recorded in
[`bounded-microbatch-backfill-evidence-2026-08-05.md`](bounded-microbatch-backfill-evidence-2026-08-05.md).
The micro-batch path is approximately `3.3x` faster than the one-ledger
control, not the two orders of magnitude needed for a practical full-history
rebuild.

The initial aggregate target is 1,000 ledgers/s. At a fixed tip of 63,804,680
ledgers, that is approximately 17.7 hours. Ledger rate alone is not an honest
capacity measure: the measured recent fixture contains 11.21 GB of serialized
protobuf and 8.11 million Bronze rows per 1,000 ledgers. Reaching 1,000 recent
ledgers/s would therefore require roughly 11.2 GB/s of decoded input and 8.1
million output rows/s across the fleet. The benchmark gate must always report
ledgers/s, input bytes/s, output bytes/s, and rows/s together.

This target cannot be reached by increasing the number of in-flight RPCs
against one catalog owner. The scalable work—archive reads, XDR extraction,
normalization, encoding, and Parquet production—must happen independently on
many workers. The serialized section must shrink to validating and registering
already-written files.

## Architecture

```text
fixed historical range + pinned versions
  -> backfill planner
       -> deterministic, disjoint shard specifications
            -> worker 1 ----\
            -> worker 2 -----+-> immutable typed Parquet + shard manifests
            -> worker N ----/
                                  |
                                  v
                         manifest/file validator
                                  |
                                  v
                       single catalog coordinator
                     (short metadata transactions only)
                                  |
                                  v
                         candidate DuckLake catalog
                                  |
                     coverage + parity + chaos gates
                                  |
                                  v
                bounded micro-batch tail catch-up and cutover
```

There are four runtime roles.

### Planner

The planner creates one immutable job manifest and deterministic shard specs
for an exact inclusive ledger range. It pins:

- Stellar network passphrase
- source range and source location
- extractor and schema versions
- DuckDB, DuckLake, and Quack versions
- partition and file-size policy
- code revision and container digest
- validation policy

Shard IDs are derived from the job ID, network, inclusive range, and schema
version. Replanning the same job therefore produces the same IDs. Shards are
disjoint and their union must exactly cover the requested range.

Start with source-aligned ranges targeting 60–180 seconds of worker time. The
planner should use observed bytes per ledger to adapt future shard widths while
keeping ledger boundaries deterministic. A range near 1,024 recent ledgers is
a reasonable first experiment, not a production default.

### Worker

A worker leases one shard, reads its fixed ledger range, invokes the same
versioned Stellar extraction semantics used by live Bronze, and writes typed
Parquet for each Bronze table. It has no DuckLake catalog credentials.

Workers stream through their range with bounded memory. Each table owns a
bounded row buffer and rolls files near a configured target, initially 256 MiB
with 512 MiB in the benchmark matrix. File rolling is independent of the shard
boundary, so a dense table may produce several files while sparse tables may
produce one. Empty-table coverage is represented in the manifest rather than
by empty Parquet files.

The implementation spike must compare two writers:

1. a native Go Arrow/Parquet writer; and
2. a worker-local DuckDB database followed by `COPY ... TO ... (FORMAT
   PARQUET)`.

Selection is based on end-to-end rows/s, peak RSS, output compatibility,
compression ratio, and operational complexity. A worker-local DuckDB instance
is acceptable because it owns only disposable local state; it must never
attach to the shared catalog.

Decision update, 2026-08-05: select the native Arrow/Parquet writer for the
historical candidate path and retain DuckDB Appender plus `COPY` as the parity
oracle and rollback. The Arrow writer owns bounded per-table builders, rolls
only after complete row groups, pins all physical writer options, publishes
without replacement, and produces the same logical schemas and rows for all 21
Bronze tables plus metadata and watermarks. The first generated direct builder
handles contract events, 53% of the measured recent-mainnet rows, without
reflection or generic SQL-value conversion. Remaining tables still use the
generic Arrow bridge and should migrate in measured volume order.

The writer selection does not select one codec for every deployment. Zstd is
the compact default. Snappy is the backfill throughput profile after the
120-ledger probe produced essentially uncompressed speed with 3.1 times fewer
bytes than uncompressed output. Every job pins the codec in its manifest and
must not mix codecs under one retry identity.

Decision update, 2026-08-05: overlap raw source reads and ledger extraction
through a bounded, ordered pipeline. The SDK source lends its XDR buffer only
until the next read, so admission owns a copy before advancing the source.
Workers may finish out of order, but a sequence-indexed reorder buffer exposes
only the next source ordinal to hashing and Arrow. One semaphore covers queued
copies, active extraction, completed results, and the ledger currently being
written. Worker count and admission depth are execution evidence rather than
artifact identity: sequential and concurrent attempts must produce identical
files.

The measured local knee is four extraction workers, two projection workers per
ledger, and eight ledgers in flight. Eight extraction workers increased RSS
and reduced throughput. A 1,000-ledger run moved the critical path to the
single Arrow writer, so the next implementation must measure per-table work
and add bounded parallel row-group encoding rather than increasing extraction
concurrency again.

Decision update, 2026-08-05: encode immutable row groups through per-table
ordered queues, a global encoder bound, and a global pending-record bound. Two
encoders with four pending row groups improved the 1,000-ledger GCS result to
35.22 ledgers/s; four encoders were flat and eight regressed. File identities
matched across every concurrency setting. The resulting per-table evidence
selects generated typed builders and removal of redundant canonical sorts as
the next worker optimization. See
[`parallel-arrow-writer-evidence-2026-08-05.md`](parallel-arrow-writer-evidence-2026-08-05.md).

Partitioned output must avoid small-file explosion. DuckDB's own guidance is
to keep partitions on the order of at least 100 MB; the exact file target is a
measured tuning parameter, not an invariant. See the official
[partitioned writes documentation](https://duckdb.org/docs/current/data/partitioning/partitioned_writes).

### Validator

The validator treats worker output as untrusted input. Before any catalog
mutation it verifies:

- job, shard, network, range, extractor, and schema identities
- manifest hash and every file's SHA-256 and byte size
- Parquet footer readability and exact logical schema fingerprint
- declared and observed row counts per table
- every row's ledger sequence is inside the shard range
- exactly one ledger metadata row and one ingest watermark per ledger
- no gaps or duplicates inside the shard
- no overlap with another accepted shard
- canonical table-level fingerprints against an independently built sample

Derived floating-point columns are not compared bit-for-bit across inline and
Parquet representations. Canonical parity uses lossless source fields such as
`amount_raw`, or an explicitly documented numeric tolerance. File-to-file
backfill comparisons should remain exact.

Validation produces a signed or content-hashed acceptance record. It does not
make files queryable.

### Catalog coordinator

Only the coordinator mutates the candidate catalog. It serializes registration
under the same ownership rule used by Quack, but the data files are already
complete. Its transaction conceptually performs:

```text
BEGIN
  register all accepted shard files with DuckLake
  write the immutable shard receipt
  advance the continuous committed prefix when coverage permits
COMMIT
```

Registration uses DuckLake's supported `ducklake_add_data_files` API rather
than direct metadata-table edits. DuckLake documents this API for adding
existing Parquet without copying it and gives the registered files DuckLake
ownership; see [Adding Files](https://ducklake.select/docs/stable/duckdb/metadata/adding_files).

The first vertical slice must prove with DuckDB 1.5.5 and the pinned DuckLake
extension that multiple registration calls across all Bronze tables plus the
receipt are atomic in one transaction. If that proof fails, the design must add
a catalog-visible publication pointer and keep incomplete shard generations
invisible. We must not infer cross-table atomicity from a single-file example.

The coordinator may register several already-validated shards per transaction
to amortize snapshot overhead, but it must cap metadata transaction time and
uncertain replay scope. Initial experiments should compare 1, 8, 32, and 128
shards per registration transaction.

## Manifest contract

The manifest is the durable interface between compute and catalog mutation.
Use canonical JSON for v2 so hashes are stable and Go tooling remains small.
Protobuf may be added later if schema evolution warrants it.

### Job manifest

```json
{
  "format_version": 2,
  "job_id": "pubnet-bronze-00000001-63804680-schema7",
  "network_passphrase": "Public Global Stellar Network ; September 2015",
  "ledger_start": 1,
  "ledger_end": 63804680,
  "source": {"kind": "history-archive", "uri": "..."},
  "schema_version": 7,
  "extractor_version": "stellar-extract-v0.1.4",
  "code_revision": "...",
  "image_digest": "sha256:...",
  "duckdb_version": "1.5.5",
  "ducklake_version": "...",
  "writer": "arrow-parquet",
  "compression": "zstd",
  "file_target_bytes": 268435456,
  "file_max_bytes": 536870912,
  "row_group_rows": 16384,
  "shards": ["..."]
}
```

### Shard specification

```json
{
  "job_id": "...",
  "shard_id": "sha256:...",
  "ledger_start": 62080000,
  "ledger_end": 62081023,
  "expected_predecessor": 62079999,
  "attempt_policy": {"max_attempts": 5, "lease_seconds": 900}
}
```

### Shard result manifest

```json
{
  "format_version": 2,
  "job_id": "...",
  "shard_id": "sha256:...",
  "ledger_start": 62080000,
  "ledger_end": 62081023,
  "ledger_count": 1024,
  "source_digest": "sha256:...",
  "schema_fingerprint": "sha256:...",
  "files": [
    {
      "table": "bronze.token_transfers_stream_v1",
      "uri": "s3://.../sha256.parquet",
      "sha256": "...",
      "bytes": 268001284,
      "rows": 193244,
      "min_ledger": 62080000,
      "max_ledger": 62081023,
      "parquet_schema_fingerprint": "sha256:..."
    }
  ],
  "table_counts": {"bronze.ledger_metadata": 1024},
  "worker": {"id": "...", "attempt": 1},
  "started_at": "...",
  "completed_at": "..."
}
```

Version 2 pins the writer, compression codec, file bounds, and row-group size;
version 1 could not distinguish physical writer policies and is rejected by
the current worker. The production schema will also include source archive
hashes, per-table logical fingerprints, Parquet row-group statistics, and the
exact extraction configuration. Timestamps are evidence and never part of the
deterministic shard ID.

## Publication and idempotency

Compute is at-least-once; publication and registration are idempotent.

1. A worker writes only to a process-private temporary directory.
2. It closes each Parquet writer and verifies the footer locally.
3. It computes the file hash and publishes to an immutable,
   content-addressed final key.
4. It verifies remote size/hash or object-store checksum.
5. It publishes the shard result manifest last.
6. The validator writes an acceptance record.
7. The coordinator registers the accepted generation once.

On an object store, completed object publication is used instead of pretending
that an S3 rename is atomic. On a shared filesystem, publication uses a rename
within the same filesystem. A retry that produces identical bytes converges on
the same keys. A retry that produces different bytes for the same shard ID is
a determinism failure and stops the job.

The catalog receipt key is `(job_id, shard_id, generation_digest)`. The
generation digest covers deterministic source, schema, file, and row-count
content while excluding worker identity, attempt number, and timestamps.
Repeating the same generation returns the prior receipt without a new
snapshot. Reusing a shard ID with another generation digest is rejected.

Suggested states are:

```text
planned -> leased -> writing -> published -> validated -> registered
             |          |            |            |
             +----------+------------+------------+-> retryable/failed
```

Leases expire; data does not. Expired work can be recomputed. Orphaned
temporary objects have a TTL. Published but unregistered immutable files are
garbage-collected only after confirming that no job manifest, acceptance
record, or DuckLake file listing references them.

Registered files are DuckLake-owned and must never be removed by the fixture
or job cleanup path. Physical reclamation follows the separate snapshot expiry
and `ducklake_cleanup_old_files` safety procedure.

## Coverage and watermark semantics

Workers and registrations may complete out of order. Therefore the maximum
registered ledger is not the ingest high watermark.

The coordinator maintains both:

- the set of registered, non-overlapping intervals; and
- the continuous committed prefix beginning at the job's first ledger.

Only the continuous prefix is exposed as the high watermark. Advancing it is a
pure interval operation performed in the same transaction as the shard
receipt. Cutover is forbidden while the interval set contains a gap or overlap.

The existing per-ledger `bronze.ingest_watermarks` rows remain part of the
logical dataset because downstream replication and parity checks rely on them.
The job and shard receipt tables add provenance; they do not replace ledger
watermarks.

## Silver and Gatekeeper integration

Bronze and Silver use the same physical publication protocol but different
authority rules.

For a large Silver backfill:

1. Gatekeeper accepts a transformation proposal and pins an accepted Bronze
   snapshot plus the transformation, invariant, schema, and code hashes.
2. The planner divides the declared Bronze ledger range into disjoint Silver
   shards.
3. Workers read only the pinned Bronze snapshot and write candidate Silver
   Parquet to the proposal's immutable staging prefix.
4. Gatekeeper validates coverage, reproducibility, reconciliation, replay, and
   confinement by querying the candidate files directly.
5. The catalog coordinator registers the accepted files and writes the
   promotion receipt atomically.
6. The generic incremental runner continues the promoted transformation over
   live ledger ranges. A failed increment leaves the table stale but on its
   last verified generation.

Workers receive read access to the pinned Bronze snapshot and write access to
one staging prefix. They receive no catalog-write capability. This makes
confinement structural rather than only a post-hoc SQL check.

Canonical Silver uses `obsrvr.*`; tenant-specific definitions use the tenant
schema, beginning with `prism.*`. File manifests describe materialized output,
while the Gatekeeper proposal continues to own the transformation and
invariants. Promoting files must not turn an output snapshot into the source of
truth for future computation.

## Full-history cutover

Use a new candidate catalog rather than mutating the current production
catalog in place.

1. Select cut line `C` and pin the source and software versions.
2. Build and register full-history Bronze through a recent bulk boundary below
   `C` using parallel file workers.
3. Prove exact continuous coverage, canonical table parity, and catalog
   restart recovery.
4. Start the candidate Quack server in `INGEST_PROFILE=backfill` and use bounded
   micro-batches to catch up from the bulk boundary through `C`.
5. Run required canonical and Prism Silver file backfills against the pinned
   accepted Bronze snapshot.
6. Pause source admission briefly, record the old system's final ledger, and
   apply any remaining ledgers to the candidate.
7. Run final gap, overlap, parity, receipt, checkpoint, replica, and query
   gates.
8. Switch live ingestion to the candidate at the next ledger and move readers
   only after replica health is green.
9. Keep the old catalog read-only for the rollback window.

Rollback switches ingestion and readers to the old path only if its source
position can be reconciled without a split brain. After the candidate accepts
new live ledgers, rollback requires replaying that suffix into the old catalog
or selecting one catalog as authoritative. The runbook must specify this range
before the cutover starts.

## Resource control and scheduling

The worker fleet is horizontally scalable, but each worker remains bounded:

- one active shard per worker initially
- fixed decode concurrency
- bounded per-table buffers and total RSS budget
- bounded local scratch usage
- upload backpressure
- lease heartbeat and cancellation
- independent timeouts for source fetch, extraction, write, and upload

Nomad parameterized batch jobs are sufficient for the first distributed run.
A small durable planner/lease store may begin in PostgreSQL or another existing
operational store; it is coordination state, not analytical truth. Do not use
the DuckLake catalog as a high-churn work queue.

The current Flow path can remain the live delivery mechanism. Full-history
workers should call the shared acquisition and extraction libraries directly
for fixed ranges instead of sending millions of events through one central
Flow pipeline. The shared library boundary prevents the fast path from
becoming a second interpretation of Stellar.

At the 1,000-ledger/s aggregate target:

| Fleet | Required average per worker | Recent-fixture input per worker |
|---:|---:|---:|
| 32 | 31.25 ledgers/s | approximately 350 MB/s |
| 64 | 15.63 ledgers/s | approximately 175 MB/s |
| 128 | 7.81 ledgers/s | approximately 88 MB/s |

These are capacity-budget examples, not a claim that the archive or object
store can sustain them. The scale gate must measure source, network, CPU,
scratch disk, and destination storage separately.

## Telemetry

Metrics use bounded labels such as table, phase, result, and worker pool; never
ledger sequence, shard ID, file URI, or job ID.

Worker metrics:

```text
obsrvr_backfill_shards_total{result}
obsrvr_backfill_shard_duration_seconds{phase}
obsrvr_backfill_input_bytes_total
obsrvr_backfill_output_bytes_total{table}
obsrvr_backfill_rows_total{table}
obsrvr_backfill_active_workers
obsrvr_backfill_worker_rss_bytes
obsrvr_backfill_worker_scratch_bytes
obsrvr_backfill_upload_retries_total{reason}
```

Coordinator metrics:

```text
obsrvr_backfill_shards_pending{state}
obsrvr_backfill_validation_duration_seconds{result}
obsrvr_backfill_registration_duration_seconds{result}
obsrvr_backfill_registration_files{result}
obsrvr_backfill_registered_ledger_intervals
obsrvr_backfill_continuous_high_watermark
obsrvr_backfill_coverage_gaps
obsrvr_backfill_coverage_overlaps
obsrvr_backfill_orphan_bytes
```

Every shard manifest and receipt carries detailed per-range evidence for
forensics; Prometheus does not.

## Benchmark and fault matrix

Large protobuf fixtures remain outside Git. The 11 GB 1,000-ledger payload was
deleted after its manifest, summaries, fingerprints, and benchmark evidence
were retained. Future multi-era fixtures must be published to object storage
with hashes and a retention policy, then removed from worker disks after the
evidence bundle is complete.

### Functional slice

- 30 real ledgers split into two out-of-order shards
- all Bronze tables written as Parquet and registered
- exact parity with the existing live writer using canonical comparisons
- identical retry creates no new files, rows, receipts, or snapshots
- changed retry for the same shard ID is rejected

### Era matrix

Capture bounded samples from early, middle, pre-Soroban, and recent dense
history. Report protobuf bytes, extracted rows, output bytes, and table mix for
each. A weighted full-history forecast must use this distribution rather than
extrapolating only the recent 1,000-ledger fixture.

### Scale matrix

Run 1, 4, 16, 32, 64, and 128 workers, subject to available hardware. Separate
the following measurements:

1. source fetch only
2. fetch plus extraction
3. extraction plus local Parquet
4. upload
5. validation
6. catalog registration
7. complete job wall time

Stop adding workers when aggregate throughput flattens. The result should name
the bottleneck instead of hiding it behind ledger/s.

### Failure matrix

Inject failure:

- before any file is closed
- during a Parquet write
- during upload
- after files publish but before the manifest
- after manifest publication but before validation
- during validation
- before, during, and after the registration transaction
- after receipt commit but before coordinator acknowledgement
- during catalog checkpoint and restart
- while a lease expires and another worker recomputes the shard

Every case must converge after retry with no partial registered shard, no
duplicate rows, no coverage gap, and no accepted divergent generation.

## Acceptance criteria

Correctness is mandatory at every scale:

- requested ledger coverage has zero gaps and zero overlaps
- every registered shard is represented by exactly one immutable receipt
- all logical Bronze tables match the reference writer under canonical parity
- no table contains a row outside its shard's declared range
- a worker or coordinator retry is idempotent
- a catalog crash exposes either all or none of a registration unit
- catalog restart and bounded micro-batch continuation succeed
- Gatekeeper Silver output reconciles to the pinned Bronze snapshot
- no registered file is deleted by job or fixture cleanup

Initial performance goals on hardware provisioned for the target are:

- aggregate sustained throughput at least 1,000 ledgers/s on the agreed
  weighted history corpus
- report and provision for the corresponding bytes/s and rows/s
- at least 80% parallel efficiency from 1 to 16 workers, with later fleet
  efficiency reported rather than assumed
- validation throughput stays ahead of worker production
- registration is less than 5% of end-to-end job wall time
- registration transaction p99 below 5 seconds
- peak worker RSS and scratch usage remain within configured hard limits

The 1,000-ledger/s number is an engineering target, not an SLO to claim before
the multi-era, end-to-end scale run. If source bandwidth or extraction cost
makes it uneconomic, the evidence should drive a revised completion-time and
fleet-cost contract.

## Incremental delivery

The consolidated active branch contains Gatekeeper, the bounded micro-batch
tail/cutover mechanism, this plan, and the first file-worker proof. Keep that
work in one active PR. The sections below are delivery stages; they are not
instructions to create another stack of dependent draft PRs.

### Stage 1 — File and manifest vertical slice

- add `internal/backfillmanifest` canonical types, hashing, and validation
- add `cmd/ducklake-backfill-worker`
- share the production Bronze schema/extraction mapping with the worker
- prototype native Go Parquet and worker-local DuckDB writers
- write one local shard with bounded memory and deterministic output
- add a small committed synthetic fixture and an opt-in real-ledger smoke
- document object naming, ownership, and cleanup rules

Exit gate: one shard is byte-stable across two runs and its logical rows match
the reference writer.

### Stage 2 — Validation and transactional registration

- add `cmd/ducklake-backfill-commit`
- add strict footer, hash, schema, range, count, and overlap validation
- add job, shard, file, and registration receipt migrations
- register existing Parquet through the supported DuckLake API
- prove cross-table transaction atomicity on DuckDB 1.5.5
- maintain registered intervals and the continuous committed prefix
- add duplicate, divergent retry, crash, and restart tests

Exit gate: two shards complete out of order, register atomically, converge on
retry, and match a live-writer baseline.

### Stage 3 — Distributed execution and scale harness

- add deterministic planning, leases, heartbeats, attempts, and cancellation
- package workers as digest-pinned Nomad batch jobs
- add object-store publication and orphan discovery
- add resource limits, backpressure, telemetry, and dashboards
- add the multi-era corpus manifest and 1/4/16/32/64/128-worker harness
- record throughput, cost, scaling efficiency, and the real bottleneck

Exit gate: an interrupted distributed job resumes without recomputing accepted
work and produces a complete candidate Bronze catalog.

### Stage 4 — Gatekeeper Silver and production cutover gate

- extend Gatekeeper proposals with pinned input and candidate file manifests
- validate candidate Silver files before promotion
- atomically register Silver files with promotion provenance
- run required canonical and Prism Silver backfills
- add full cutover, replica, query, rollback, and storage-cleanup runbooks
- execute one checkpoint- and restart-inclusive release rehearsal

Exit gate: a clean candidate catalog moves from historical Bronze through
gated Silver, bounded tail catch-up, serving replica, and reversible cutover
with retained evidence.

## First executable slice

The first implementation should be intentionally small:

```text
internal/backfillmanifest/manifest.go
internal/backfillmanifest/canonical.go
internal/backfillmanifest/validate.go
cmd/ducklake-backfill-worker/
cmd/ducklake-backfill-commit/
scripts/ducklake-file-backfill-smoke.sh
testdata/backfill/synthetic/
```

It should process 30 ledgers as two shards, deliberately finish them out of
order, kill one worker after its first file closes, retry it, register both
shards, restart the catalog, and compare all tables to the current writer. This
slice tests the new boundary—immutable file plus manifest plus metadata-only
commit—before scheduler or fleet work begins.

## Open decisions that require evidence

1. Generated direct Arrow builders for the remaining high-volume tables and
   whether compression should move to a separate bounded stage.
2. Initial shard target duration and adaptive sizing algorithm.
3. 256 MiB versus 512 MiB file targets and row-group sizing per table.
4. Local shared storage versus S3-compatible object storage for the first
   distributed run.
5. File-backed DuckDB catalog versus PostgreSQL-backed DuckLake metadata after
   the metadata transaction is removed from the bulk data path.
6. Maximum shards/files per registration transaction.
7. Whether all Bronze tables can be registered atomically through the current
   DuckLake API; the alternative publication-pointer design must be prototyped
   if not.
8. Schema-evolution handling for a job that spans an extraction or table
   version boundary.
9. Required canonical Silver tables at cutover; tenant Silver remains
   demand-driven.
10. Fleet size at which source archive, network, object store, or catalog
    metadata becomes the limiting resource.

Dependency upgrades follow compatibility evidence, not version drift. Every
benchmark records exact DuckDB and extension builds. Use the latest compatible
DuckDB and extension releases where possible, beginning with DuckDB 1.5.5, but
rerun manifest compatibility, file registration, transaction atomicity,
checkpoint recovery, and parity gates before changing a pinned production
stack.

## Definition of done

The project is complete when a fresh candidate catalog can be built from the
chosen historical start through a declared cut line, at the accepted time and
cost, by restartable parallel workers; every file and shard has verifiable
provenance; catalog publication is atomic and idempotent; Bronze and required
Silver tables pass exact coverage and canonical parity; the remaining tail is
closed by bounded micro-batches; serving replicas pass; and a rehearsed
cutover/rollback runbook plus retained evidence has production approval.
