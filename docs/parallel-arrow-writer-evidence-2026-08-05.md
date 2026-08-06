# Bounded Parallel Arrow Writer Evidence

**Date:** 2026-08-05

**Status:** Passed; use two Parquet encoders and four pending row groups for the measured throughput profile

## Purpose

The first bounded extraction pipeline left one synchronous Arrow/Parquet
writer on the critical path. A 1,000-ledger run spent 37.24s in Arrow
append/write and sustained 24.70 ledgers/s. This slice separates mutable Arrow
record construction from immutable Parquet row-group encoding so the two may
overlap.

Each table retains one ordered queue. A global encoder semaphore bounds active
Parquet work, and a second semaphore bounds all admitted immutable record
batches. A row group is not admitted unless both its Arrow buffers and eventual
release are covered by the pending bound. Close drains every table queue before
footer canonicalization, verification, hashing, and publication.

Execution concurrency is deliberately absent from shard identity. The same
records, writer settings, and shard range must produce the same artifacts
regardless of scheduling.

## Correctness and resource invariants

- One goroutine owns each table's Parquet writer and preserves record-batch
  order for that table.
- At most `parquet-writers` row groups encode concurrently across all tables.
- At most `max-pending-row-groups` immutable Arrow batches are queued or
  active across the worker.
- The first asynchronous error stops admission, releases queued records, and
  removes unpublished artifacts.
- Final file order is canonicalized after all workers stop.
- Unit and race tests cover bounds, per-table ordering, first-error cleanup,
  and byte stability across one and four writer workers.

## 1,000-ledger writer sweep

All runs used mainnet range `62,080,000-62,080,999`, the Obsrvr GCS archive,
Arrow/Snappy, four extract workers, two projection workers per ledger, eight
ledgers in flight, one process, and 16,384-row groups.

| Parquet writers | Pending row groups | Wall | Ledgers/s | Admission wait | Encoder worker-seconds | Foreground append | Peak RSS |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2 | 31.368s | 31.880 | 3.896s | 20.469s | 28.019s | 1,270,480 KiB |
| 2 | 4 | 28.391s | 35.223 | 0.266s | 22.487s | 25.261s | 1,354,864 KiB |
| 4 | 8 | 28.420s | 35.187 | 0.050s | 22.670s | 25.414s | 1,316,060 KiB |
| 8 | 16 | 29.179s | 34.271 | 0.012s | 22.610s | 25.324s | 1,420,236 KiB |

One asynchronous encoder is 29% faster than the previous synchronous writer
because extraction, Arrow construction, and Parquet encoding now overlap. Two
encoders are 43% faster than that merged baseline. Four are statistically flat
and eight regress while consuming more memory. The measured knee is therefore
two encoders with four pending row groups.

Every run processed 1,733,356,100 source bytes, 8,108,413 Bronze rows, and
8,110,413 total output rows. All 22 normalized file identities matched exactly
across the four concurrency configurations: table, SHA-256, byte size, row
count, ledger range, and schema fingerprint were unchanged.

`Encoder worker-seconds` is aggregate active encoder time and may exceed wall
time. `Foreground append` includes per-ledger sorting, Arrow construction, and
admission wait. The final queue drain was 0.25s or less in every run.

## Per-table evidence

The two-writer run attributes the remaining work as follows:

| Table | Rows | Sort | Build | Encode | Output |
|---|---:|---:|---:|---:|---:|
| contract events | 4,152,056 | 5.168s | 5.070s | 10.193s | 270.96 MB |
| transactions | 310,805 | 0.259s | 3.915s | 5.209s | 636.48 MB |
| operations | 704,848 | 0.492s | 2.332s | 2.239s | 58.56 MB |
| token transfers | 879,574 | 0.875s | 1.305s | 1.805s | 46.29 MB |
| effects | 524,134 | 0.242s | 0.882s | 0.975s | 31.23 MB |

Contract events alone consume about 10.2 foreground seconds in sorting and
typed Arrow construction. Transactions, operations, token transfers, and
effects still pass through the generic `[]any` bridge and consume another
8.4 build seconds. Adding encoder workers cannot remove this serial foreground
work.

## Host scaling probe

Two disjoint 500-ledger processes, each with two extract workers, four ledgers
in flight, two Parquet encoders, and four pending row groups, completed the
same range in 24.635s: 40.592 aggregate ledgers/s and 329,221 output rows/s.
Peak RSS was approximately 1.05 GiB per process. Ordered pipeline wait rose to
5.98-7.45s, so two processes delivered only 58% parallel efficiency relative
to two copies of the best single-process rate. The host is CPU-contended.

At 40.59 ledgers/s, this host would need about 18.2 days for a fixed
63,804,680-ledger projection. An ideal 1,000-ledger/s fleet would require about
25 equivalent hosts before coordination, upload, registration, skew, and
historical density effects. This is not yet the release-gate result.

## Next implementation gate

The next change should reduce serial row preparation rather than increase
worker counts:

1. replace the generic bridge for transactions, operations, token transfers,
   and effects with generated typed Arrow builders;
2. prove whether canonical extractor order can replace the 5.2s per-ledger
   contract-event sort without weakening retry stability;
3. preserve the current one-table owner, global admission bound, logical
   parity, and cross-concurrency file-hash gates; and
4. rerun the same GCS range before widening the fleet benchmark.

The new telemetry makes that decision falsifiable: a successful direct-builder
slice must reduce foreground build/sort time and wall time, not merely move CPU
between counters.
