# Current State

**As of:** 2026-08-06
**Status:** Local write-path and two-server replica gates passed; production deployment remains open.

This is the canonical status document for the Quack/DuckLake work. Historical
plans and phase handoffs explain how the architecture evolved, but this file
records the currently intended topology and remaining release gates.

## Current topology

```text
raw-ledger-source (archive-backed today; tip-capable source still to verify)
  -> stellar-ledger-processor
  -> ducklake-sink (DUCKLAKE_MODE=ingest-rpc)
       -> BronzeIngestService (ordered gRPC stream, one ledger in flight)
          quack-ducklake-server (primary catalog owner)
            -> DuckLake primary bronze catalog
            -> ducklake-maintenance (flush, merge, snapshot expiry)
            -> Quack SQL surface
                 -> index-materializer
                 -> ducklake-replica-sync
                      -> quack-ducklake-server (serving replica owner)
                           -> query API / gateway readers
```

The primary production ingest path is `DUCKLAKE_MODE=ingest-rpc`. Quack remains
the shared server-side SQL and operational surface for maintenance,
materialization, replication, and querying.

The other sink modes remain available:

- `quack`: staged-Parquet transport plus a KB-scale remote SQL script; fallback
  and compatibility path. It currently requires storage visible to both sink
  and server.
- `embedded`: development and test fallback; it is not the intended production
  ownership model.

## Shipped capabilities

The implementation through PR #6 (`e9f6c51`) provides:

- ledger-bounded DuckLake transactions and atomic ingest watermarks
- replay-on-uncertainty with ledger-scoped delete-then-insert replacement
- a chaos harness comparing failed/replayed output with a never-failed baseline
- typed history-loader-compatible bronze tables, including XDR and Soroban data
- ordered DuckLake schema migrations and one-network-per-catalog enforcement
- server and sink health endpoints, bounded remote operations, and session reset
- Quack insecure-mode opt-in, resource limits, connection pinning, and config lock
- snapshot-driven replica sync with bounded full-resync fallback
- replica schema-drift detection, explicit column copies, and secret redaction
- bounded, explicit-range index materialization
- maintenance for inlined-row flush, file merge, and snapshot expiry

Local measurements on the recorded test hardware:

- staged-Parquet Quack path: approximately 2.7s sustained per ledger
- ingest RPC with `DUCKLAKE_INLINE_ROW_LIMIT=256`: 232–318ms server-side and
  244–349ms sink-observed per ledger

The server default inline limit is `1024`, which trades higher commit latency
for fewer files. Sub-400ms deployments use `256` paired with frequent
maintenance; the Nomad templates use a two-minute maintenance interval.

## Verified local production gates

Evidence captured 2026-08-03:

- **1,000-ledger ingest-RPC chaos gate:** ledgers `62080000`–`62080999`;
  failed/replayed and never-failed runs each committed all 1,000 ledgers;
  watermark count `1000`, exact requested min/max, zero gaps, empty
  `EXCEPT ALL` parity diff, and no typed/XDR/Soroban gate failures. Evidence:
  `/tmp/obsrvr-ingest-rpc-1k-20260803`.
- **Live two-Quack replica gate:** an actual primary and serving Quack server
  established checkpoints for two tables; snapshot expiry forced bounded full
  resync for both; source/target row diffs were zero; a source-only
  `drift_probe` column produced the expected concise schema diff while the later
  table continued to checkpoint; no primary or target token appeared in logs or
  persisted checkpoint evidence. Evidence:
  `/tmp/obsrvr-two-quack-replica-gate-20260803`.

The retained result summary is in
[`production-gate-evidence-2026-08-03.md`](production-gate-evidence-2026-08-03.md).
The 1,000-ledger correctness gate passed, but its latency distribution does not
support a hard 400ms upper-bound claim. The follow-up diagnosis identified
DuckDB's synchronous 16 MiB catalog auto-checkpoint as the primary tail source;
see [`ingest-latency-diagnosis-2026-08-03.md`](ingest-latency-diagnosis-2026-08-03.md).

The replica gate exposed and fixed a real integration bug: DuckLake 1.5.4 emits
`No snapshot found at version N`, which the missing-snapshot classifier did not
recognize. A regression test now covers the exact engine message.

## Open production gates

The topology is not production-approved until these are complete:

1. Publish/apply the Quack and maintenance jobs to the production Nomad
   environment and verify their health checks there.
2. Verify a tip-capable raw-ledger source and measure ledger-close-to-queryable
   latency. All repository pipelines currently use the archive backend.
3. Define and prove the safe storage-reclamation procedure. Maintenance expires
   snapshots but intentionally does not run `ducklake_cleanup_old_files`.
4. Complete the
   [`checkpoint-latency-production-gate-plan.md`](checkpoint-latency-production-gate-plan.md):
   replace the interim `DUCKDB_CHECKPOINT_THRESHOLD=1GB` mitigation with an
   explicit, operationally bounded checkpoint policy before claiming a hard
   ingest-latency SLO. The telemetry/manual-checkpoint slice merged as
   `63e9944`; immutable server/sink/maintenance images are published and the
   registry-pulled server passes health, metrics, authentication, and checkpoint
   smoke. A 30-real-ledger local reconciliation gate accounts for every
   acknowledgement and matches catalog/WAL gauges to disk. The shared writer
   coordinator and authenticated manual checkpoint primitive also pass a real
   30-ledger gate: a 54.984ms checkpoint reduced the 10.6MB metadata WAL to
   zero without racing ingest. The complete 64/128/256/512MiB explicit sweep
   passed; checkpoint duration scaled from 172ms at 67MB WAL to 1.369s at
   620MB, with every WAL reduced to zero and no watermark gaps or retries.
   Strengthened 64/512MiB gates now prove deterministic logical parity and
   next-ledger resume. At ~620MB WAL, pre-checkpoint crash recovery completed in
   5.059s and synchronized kill-during-checkpoint recovery in 4.440s, both with
   zero parity differences, partial commits, gaps, or retries. Real failure
   injection proves three bounded attempts/two backoffs, persistent 503 health,
   and successful recovery. Latitude testnet runs a healthy checkpoint-disabled
   digest-pinned canary: its metrics target is `UP`, protocol targets are
   dropped, and all eight scoped rules are healthy. The measured controller
   candidates are 64MiB soft/512MiB hard. A disabled-by-default scheduler and
   real-ledger trigger gate now pass one soft-idle and one hard-limit checkpoint
   with no ingest errors/retries. The deterministic fixture recorder, paced
   ingest replay profiles, and checkpoint/maintenance/restart gate are now
   implemented, but the 1,000-ledger corpus and retained one-hour evidence
   remain open. The controller therefore remains disabled; Grafana and mainnet
   promotion are deferred.
5. Dependencies now use `stellar-extract v0.1.4`, `flowctl-sdk v0.1.3`, and
   `go-stellar-sdk v0.7.1`. The SDK release is dependency-only: its exported
   runtime remains unchanged from `v0.1.2`, so delivery backpressure,
   registration retry, and health fixes still require a future SDK release.
6. The existing one-ledger-in-flight contract remains correct for live ingest
   but is not the full-history loading strategy. The latest 1,000-ledger direct
   replay, measured on DuckDB 1.5.4, took `308.818s` (`3.238`
   ledgers/second). That projects to roughly 228 days for a fixed
   63.8-million-ledger target and would create one DuckLake snapshot per ledger
   transaction. The proposed separate backfill mode, including its mandatory
   bounded Flow delivery change, range transaction, checkpoint admission,
   DuckDB 1.5.5 benchmark matrix, and cutover procedure, is specified in
   [`bounded-microbatch-backfill-plan.md`](bounded-microbatch-backfill-plan.md).
7. Historical workers now have a native `arrow-parquet` candidate mode. It
   passed all-table logical parity and byte-stable retry gates while retaining
   `duckdb-appender` as rollback. On the same 120-ledger recent-mainnet range,
   Obsrvr GCS reduced source wait from 7.22s to 2.06s versus the public AWS
   archive. Arrow/Zstd reached 10.28 ledgers/s and Arrow/Snappy reached 12.47
   ledgers/s on one local worker; neither is a 1,000-ledger/s fleet result. See
   [`parallel-file-backfill-evidence-2026-08-05.md`](parallel-file-backfill-evidence-2026-08-05.md).
8. Raw Arrow workers now support a bounded cross-ledger extraction pipeline.
   Four extract workers with eight ledgers in flight processed 1,000 real GCS
   ledgers at 24.70 ledgers/s; two independent bounded workers reached 35.64
   aggregate ledgers/s. Retry artifacts matched exactly after excluding their
   attempt-local file URIs. The writer occupied 37.24s of the 39.84s staging
   interval, making parallel table encoding and direct builders the next
   measured constraint. See
   [`bounded-arrow-pipeline-evidence-2026-08-05.md`](bounded-arrow-pipeline-evidence-2026-08-05.md).
9. Arrow row-group encoding now runs through per-table ordered queues with
   global active and pending bounds. On the same 1,000-ledger GCS range, two
   encoders and four pending row groups reached 35.22 ledgers/s, 43% above the
   previous synchronous writer baseline. One, two, four, and eight encoder
   configurations produced identical normalized file identities. Four writers
   were flat and eight regressed; a two-process probe reached 40.59 aggregate
   ledgers/s and exposed host CPU contention. Per-table evidence makes typed
   builders and contract-event sort removal the next measured constraint. See
   [`parallel-arrow-writer-evidence-2026-08-05.md`](parallel-arrow-writer-evidence-2026-08-05.md).
10. Generated Arrow builders now consume typed transactions, operations,
   effects, and token transfers without reflected `[]any` projection. On the
   same 1,000-ledger GCS range, raw projection fell 78%, foreground append fell
   13.5%, and one worker reached 37.32 ledgers/s, 6% above the preceding
   writer result. All 22 normalized Parquet identities matched exactly. A
   two-process retune became source-bound rather than builder-bound, so source
   prefetch/caching and contract-event work are the next measured constraints.
   See
   [`typed-arrow-builder-evidence-2026-08-06.md`](typed-arrow-builder-evidence-2026-08-06.md).
11. Backfill workers can now measure a source or extraction stage without
   publishing artifacts, override archive prefetch per run, and serve a fully
   cached ledger range from local disk. The SDK's default prefetch delivered
   only 20 ledgers/s of source and left the writer waiting 34.1 of 51.1
   seconds; 40 buffered ledgers with 16 fetchers doubled end-to-end throughput
   to 39.45 ledgers/s, and a warm cache reached 42.17. Acquisition saturates
   near 100 MiB/s on the test host regardless of process count, while the same
   host reaches 53.6 ledgers/s with the network removed, so the two-process
   regression is shared source bandwidth rather than CPU contention. Artifact
   identity was unchanged across every prefetch depth, process count, and cache
   mode. See
   [`bounded-source-prefetch-evidence-2026-08-06.md`](bounded-source-prefetch-evidence-2026-08-06.md).

## Operational constraints and residual risks

- `SNAPSHOT_RETENTION` must exceed every replica consumer's worst-case
  checkpoint lag. Falling behind retention triggers a bounded full resync.
- File-backed DuckLake currently requires Quack's explicit insecure/external
  filesystem mode. Keep endpoints isolated or place TLS termination in front of
  cross-host traffic.
- Snapshot expiry and physical file deletion are separate operations. Do not
  automate deletion merely because expiry is enabled.
- `index-materializer` rebuilds explicit ranges but does not yet own a
  continuous checkpoint loop.
- DuckDB 2.0's planned native Quack/DuckLake integration may replace the custom
  ingest RPC. The ledger transaction, watermark, replay, and replica contracts
  remain the durable architecture.

## Document precedence

When documents disagree, use this order:

1. `docs/current-state.md` — current topology and gate status
2. `docs/production-gate-runbook.md` — commands and evidence
3. component READMEs — runtime configuration and component semantics
4. phase handoffs and pitches — historical measurements and design evolution
5. `docs/production-hardening-plan.md` and `docs/rebuild-plan.md` — historical
   plans and findings registers
