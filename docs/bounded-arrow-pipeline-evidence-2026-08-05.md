# Bounded Arrow Pipeline Evidence

**Date:** 2026-08-05

**Status:** Ordered extraction pipeline passed; the single Arrow writer is now the critical path

## Purpose

The native Arrow worker initially processed each raw ledger serially:

```text
source read -> decode/extract/project -> Arrow append -> next source read
```

This left archive acquisition and Stellar extraction idle while Arrow encoded
the previous ledger. The new path overlaps those stages without weakening
bounded memory, source hashing, or deterministic file generation.

## Ownership and ordering contract

The SDK ledger stream returns borrowed XDR whose lifetime ends at the next
source read. Pipeline admission therefore copies each ledger before advancing
the source. One semaphore bounds all admitted ledgers, including queued
copies, active extraction, completed out-of-order results, and the ledger
currently owned by the writer.

Each admitted ledger receives a monotonic source ordinal. Extract workers may
finish in any order, but a bounded reorder map releases only the next ordinal
to the existing accumulator and Arrow writer. Source errors and extraction
errors are also observed in ordinal order. Cancellation drains the worker set;
no concurrent code touches a borrowed source buffer.

Pipeline concurrency is not part of artifact identity. It is recorded in the
run summary, while writer, codec, row-group size, and file bounds remain pinned
in manifest v2.

## 120-ledger configuration sweep

All runs used the same mainnet range `62,080,000-62,080,119`, the Obsrvr GCS
archive, Arrow/Snappy, one backfill process, and approximately eight total
projection workers split across concurrent ledgers.

| Extract workers | Project workers per ledger | Max in flight | Aggregate ledgers/s | Peak RSS | Pipeline wait | Append/write |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 8 | 2 | 12.06 | 785,804 KiB | 0s | 3.23s |
| 2 | 4 | 4 | 18.79 | 890,824 KiB | 1.71s | 4.15s |
| 4 | 2 | 8 | 19.90 | 1,133,048 KiB | 1.39s | 4.08s |
| 8 | 1 | 16 | 19.26 | 1,311,540 KiB | 1.46s | 4.22s |

Four extract workers were the throughput knee. Eight workers increased peak
RSS by 16% and reduced throughput by 3% relative to four. All four runs matched
on source digest, table counts, file hashes, file bytes, row counts, ledger
ranges, and schema fingerprints after removing attempt-local file URIs.

## Two 1,000-ledger retries

The longer gate used mainnet range `62,080,000-62,080,999` with:

```text
BACKFILL_WRITER=arrow-parquet
BACKFILL_COMPRESSION=snappy
BACKFILL_EXTRACT_WORKERS=4
BACKFILL_MAX_INFLIGHT_LEDGERS=8
BACKFILL_DECODE_WORKERS=2
```

| Measurement | Attempt 1 | Attempt 2 |
|---|---:|---:|
| Wall time | 40.491s | 40.500s |
| Aggregate ledgers/s | 24.697 | 24.691 |
| Output rows/s | 200,301 | 200,255 |
| Peak RSS | 1,242,044 KiB | 1,302,992 KiB |
| Source read time | 2.923s | 2.931s |
| Borrowed-XDR copy time | 0.380s | 0.354s |
| Ordered pipeline wait | 1.823s | 1.656s |
| Arrow append/write | 37.243s | 37.398s |

Each attempt processed 1,733,356,100 raw bytes, 8,108,413 Bronze rows, and
8,110,413 total output rows into 22 Parquet files. The pipeline never admitted
more than eight ledgers and buffered at most three out-of-order results. All 22
normalized file identities matched across retries. The common source digest
was `sha256:0ade95b84f367c1856a599fb0c3dedf2fea98c519d82aa46c092a60a004c38d8`.

Extraction phase values in the summary are aggregate worker-seconds and may
exceed wall time once extraction is concurrent. Pipeline wait is the writer
wall time spent waiting for the next ordered decoded ledger.

## Two-process scaling probe

Two disjoint 500-ledger workers, each configured with two extract workers and
four ledgers in flight, processed the same 1,000-ledger range at 35.64 aggregate
ledgers/s in 28.06s. That is 72% parallel efficiency relative to two copies of
the best single-process result. Peak RSS was approximately 1.0 GiB per worker.

The improvement proves that more than one Parquet writer can use the host, but
the efficiency loss also shows CPU and memory contention. Source wait rose and
writer time remained dominant. Replicating whole workers is therefore a useful
fleet mechanism, not the next single-worker optimization.

## Interpretation and next gate

The pipeline improved the 120-ledger single-process result by 65% and the
longer run sustained 24.70 ledgers/s. It also made the next constraint
unambiguous: Arrow append/write occupied 37.24s of a 39.84s staging interval,
while ordered pipeline wait was only 1.82s. Increasing extraction concurrency
again cannot materially improve that path.

The next implementation priority is:

1. measure sort, conversion, record construction, and compression per table;
2. generate direct typed builders for token transfers, operations, and effects;
3. encode independent table row groups through a bounded worker set; and
4. preserve per-table record order and the existing byte-stable retry gate.

At the measured two-process host rate, an ideal 1,000-ledger/s fleet would need
about 29 equivalent hosts before coordination, upload, registration, and skew
losses. This is a materially smaller fleet than the earlier 81-worker linear
estimate, but it is not yet the production fleet result.
