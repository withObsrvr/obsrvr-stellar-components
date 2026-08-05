# Parallel File Backfill: First Real-Ledger Evidence

**Date:** 2026-08-05
**Status:** Local file-worker smoke passed; catalog registration is not yet implemented

This report covers the first executable slice of the parallel file-oriented
backfill. It proves the boundary from a verified real `LedgerBatch` corpus to
complete, deterministic Bronze Parquet artifacts and manifests. It does not
claim a complete DuckLake backfill: object-store publication, independent
validation, transactional catalog registration, crash recovery, Silver, and
cutover remain open.

## Implementation exercised

- a bounded CLI reads and verifies a length-delimited fixture manifest
- input is rejected before materialization when protobuf-byte or Bronze-row
  limits are exceeded
- typed Bronze rows are decoded with one shared worker budget
- every recognized table is loaded through the DuckDB Appender API into a
  disposable local database
- typed Bronze, `main.ledger_batches`, and `main.ingest_watermarks` are written
  as deterministic Zstd Parquet
- a pinned watermark timestamp keeps retry artifacts byte-stable
- unsupported Bronze table names fail closed before any Parquet is published
- per-table source and output row counts must match before the result manifest
  is accepted
- every artifact is size-bounded, schema-fingerprinted, SHA-256 hashed, and
  published without overwrite

The worker uses `duckdb-go` `v2.10505.0` and its DuckDB 1.5.5 bindings. It does
not load DuckLake; workers deliberately have no shared catalog authority.

## Real mainnet corpus

```text
network:                Public Global Stellar Network ; September 2015
ledger range:           62,080,000-62,080,029
ledgers:                30
protobuf payload bytes: 363,068,724
framed fixture bytes:   363,068,844
Bronze rows:            264,607
fixture SHA-256:        c967e17a7ae0cf08d20be755b70ec577911cbb7a9d074c58e211cc7d80381f97
schema fingerprint:     sha256:4f1d35405c783aae0cdd15ff6f87dd95178183d91d885898716426c97c6d206d
decode workers:         8
compression:            Zstd
configured file target: 256 MiB
```

The public AWS archive flowed through `raw-ledger-source ->
stellar-ledger-processor -> jsonl-sink`. The sink reported 30 events sent and
zero send errors. The resulting JSONL contained exactly 30 unique, contiguous
ledger sequences. Flowctl logged `bufio.Scanner: token too long` while
aggregating the processor's very large log token; this did not affect stream
delivery, but the component logging path needs a bounded diagnostic format.

The JSONL was 416,223,147 bytes. Fixture recording took 2.21 seconds and peaked
at 856,932 KiB RSS. The JSONL and protobuf payload are disposable and were
removed after this evidence was recorded.

## Worker results

Two independent output directories were generated from the same fixture and
pinned watermark time.

| Run | Worker elapsed | Process wall time | Ledgers/s | Output rows/s | Peak RSS |
|---|---:|---:|---:|---:|---:|
| 1 | 3.076s | 4.08s | 9.754 | 86,049 | 2,477,844 KiB |
| 2 | 2.981s | 3.98s | 10.062 | 88,770 | 2,187,540 KiB |

Each run produced:

```text
Parquet files:          18
Parquet bytes:          15,747,607
Bronze rows:            264,607
metadata rows:          30
watermark rows:         30
total output rows:      264,667
input/output byte ratio: 23.06x
```

The two-run mean materialization rate was 9.905 ledgers/s. At that single
worker rate, a fixed 63,804,680-ledger history would take about 74.55 days.
An idealized 1,000-ledger/s fleet would require roughly 101 equally fast
workers before accounting for archive fetch, extraction, upload, validation,
registration, skew, or contention. The experiment therefore validates the
parallelization direction, not the fleet target.

## Correctness gates

All of the following passed:

- every one of the 18 final files matched its manifest SHA-256
- both runs matched exactly on table, SHA-256, bytes, rows, ledger bounds, and
  Parquet schema fingerprint
- the normalized two-run file-set digest was
  `8c9c3d4f8b1f74210f359e13a5b654ae9f8fb92fcac92ae71ae4dfb7aa33c649`
- an independent DuckDB read opened all 18 files and counted 264,667 rows
- source JSONL table counts matched every Bronze result-manifest count
- metadata contained 30 ledgers and 264,607 declared Bronze rows
- watermarks contained 30 distinct ledgers, range 62,080,000-62,080,029, with
  zero gaps
- rerunning against an occupied output directory failed closed on the first
  existing immutable file; all 18 original hashes remained unchanged and no
  temporary file remained
- a `SIGKILL` during staging left only the disposable local DuckDB/WAL and no
  result manifest
- a later `SIGKILL` after publication began left four complete Parquet files,
  one `.partial` file, and disposable DuckDB state, but still no result
  manifest; the incomplete attempt therefore had no acceptance artifact
- `go test ./...` passed

## Findings and next gates

The current slice is correct for this bounded sample, but it is not ready to
scale by simply increasing the shard size:

1. Peak worker RSS was 2.1-2.4 GiB for only 30 recent ledgers. The command
   retains the complete protobuf range and decoded row values. Streaming
   decode into bounded per-table buffers and real file rolling are required
   before a larger shard benchmark.
2. The 256 MiB setting is currently a hard post-write rejection, not a rolling
   policy. No file in this sample approached the target, so rolling remains
   untested.
3. Retry artifacts are byte-stable, and the kill tests confirm that the result
   manifest is the acceptance boundary. A killed attempt still needs an
   attempt-scoped staging prefix plus orphan cleanup semantics. Reusing its
   partially occupied output directory correctly refuses overwrite; a fresh
   attempt directory is required.
4. This sample exercises only one recent, dense era and one local worker. The
   early/middle/pre-Soroban matrix and 1/4/16-worker scaling run remain
   mandatory.
5. The next architectural gate is strict independent validation followed by
   transactional registration into a candidate DuckLake catalog. Until that
   exists, these files are artifacts, not queryable Obsrvr Lake history.

The immediate implementation priority is therefore bounded streaming and
attempt-scoped publication, followed by the two-shard out-of-order
registration/restart test described in the implementation plan.
