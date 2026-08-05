# Parallel File Backfill: First Real-Ledger Evidence

**Date:** 2026-08-05
**Status:** Bounded fixture and direct raw-XDR scaling probes passed; 1,000 ledgers/s and catalog registration remain open

This report covers the first executable slice of the parallel file-oriented
backfill. It proves the boundary from a verified real `LedgerBatch` corpus to
complete, deterministic Bronze Parquet artifacts and manifests. It does not
claim a complete DuckLake backfill: object-store publication, independent
validation, transactional catalog registration, crash recovery, Silver, and
cutover remain open.

## Implementation exercised

- a bounded CLI reads a length-delimited fixture manifest and verifies each
  selected chunk inline while streaming it exactly once
- input is rejected before materialization when protobuf-byte or Bronze-row
  limits are exceeded
- only one protobuf batch and its decoded Bronze rows are retained in Go at a
  time; input bytes, row count, and DuckDB buffer memory have explicit limits
- typed Bronze rows are decoded with one shared worker budget
- every recognized table is loaded through the DuckDB Appender API into a
  disposable local database
- typed Bronze, `main.ledger_batches`, and `main.ingest_watermarks` are written
  as deterministic Zstd Parquet
- a pinned watermark timestamp keeps retry artifacts byte-stable
- unsupported Bronze table names fail closed before any Parquet is published
- per-table source and output row counts must match before the result manifest
  is accepted
- DuckDB rolls output near a configured file target at row-group boundaries;
  every artifact also has a separate hard size maximum
- every artifact is schema-fingerprinted, SHA-256 hashed, and published without
  overwrite
- `scripts/ducklake-file-backfill-benchmark.sh` plans disjoint local shards,
  runs 1-N workers, records phase/resource evidence, and can enforce an
  aggregate ledgers/s floor

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

## Bounded-streaming and scaling probe

The next implementation slice used 120 real mainnet ledgers,
`62,080,000-62,080,119`, split into twelve independently hashed 10-ledger
fixture chunks. The corpus contains 1,337,822,269 protobuf bytes and 974,166
Bronze rows. A worker verifies only chunks overlapping its assigned range and
never reads earlier chunks to seek to its start.

The benchmark ran on an AMD Ryzen 9 7940HS with 8 physical cores / 16 logical
CPUs and local NVMe. Each row below processed the same complete 120-ledger
range as disjoint shards:

| Workers | Aggregate ledgers/s | Rows/s | Input MB/s | Parallel efficiency | Peak RSS per worker |
|---:|---:|---:|---:|---:|---:|
| 1 | 9.04 | 73,444 | 100.8 | 100.0% | 1,708,468 KiB |
| 2 | 14.78 | 120,052 | 164.8 | 81.7% | 1,175,056 KiB |
| 4 | 22.36 | 181,561 | 249.3 | 61.8% | 788,636 KiB |
| 8 | 27.08 | 219,871 | 301.9 | 37.4% | 514,100 KiB |

The final memory-limited one-worker critical path was 10.79 seconds of staging
and 2.22 seconds of Parquet export. Staging was not dominated by one
replaceable operation:

```text
fixture protobuf read/unmarshal/hash verification: 1.47s
deterministic source-digest protobuf encoding:     1.27s
typed JSON/reflection decode:                      2.92s
DuckDB Appender calls:                             3.45s
Appender close and other staging overhead:         1.53s
Parquet export, hashing, and validation:            2.22s
```

Inline fixture verification removed a redundant filesystem pass but did not
materially change the warm-cache result. The resource contract now also sets
a `1GB` DuckDB buffer-manager limit by default. Total process RSS can exceed
that limit because protobuf, decoded Go values, and driver allocations live
outside DuckDB; the measured one-worker run peaked at 1.63 GiB even with the
DuckDB limit active.

This host does not meet the 1,000-ledger/s target. At the measured 8-worker
rate it would need about 37 equivalent hosts, and the recent-ledger corpus
implies approximately 11.15 GB/s of protobuf input, 8.12 million Bronze rows/s,
and 516 MB/s of Parquet output at the target. These are capacity requirements,
not a validated fleet projection. At exactly 1,000 ledgers/s, 63,804,680
ledgers take 17.72 hours before archive extraction, publication, validation,
registration, retries, or skew.

The next throughput implementation priority is therefore a columnar worker
spike: produce table-partitioned Arrow/Parquet directly from extraction, avoid
the `LedgerBatch -> protobuf -> row JSON -> reflection -> DuckDB Appender`
round trip, and compare it against this measured baseline. Distributed
execution remains required, but adding workers to the present row-oriented
path alone is not an honest route to the target.

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
claim the production throughput gate:

1. The complete protobuf range and decoded row collection are no longer held
   in Go memory, and deterministic rolling is covered by a forced small-target
   regression. DuckDB plus driver memory is still substantial, so production
   shard sizing must obey measured process RSS rather than the `1GB`
   DuckDB-only limit.
2. Local scaling falls from 81.7% efficiency at two workers to 37.4% at eight.
   The current row-oriented representation and writer cannot be multiplied to
   1,000 ledgers/s without an uneconomic CPU and input-bandwidth footprint.
3. Retry artifacts are byte-stable, and the kill tests confirm that the result
   manifest is the acceptance boundary. A killed attempt still needs an
   attempt-scoped staging prefix plus orphan cleanup semantics. Reusing its
   partially occupied output directory correctly refuses overwrite; a fresh
   attempt directory is required.
4. This sample exercises only one recent, dense era and one local host. The
   early/middle/pre-Soroban matrix and multi-host 1/4/16/32/64/128-worker run
   remain mandatory.
5. The next architectural gate is strict independent validation followed by
   transactional registration into a candidate DuckLake catalog. Until that
   exists, these files are artifacts, not queryable Obsrvr Lake history.

The immediate implementation priority at that checkpoint was the direct
writer spike and attempt-scoped publication, followed by the two-shard
out-of-order registration/restart test described in the implementation plan.

## Direct raw-XDR to typed-value spike

The next slice removed the already-normalized fixture as the worker's required
input. The worker can now consume the SDK `LedgerStream` from
`stellar-raw-ledger-origin`, validate its borrowed raw XDR through
`stellar-extract`, perform one full ledger decode, and project typed extraction
rows directly to DuckDB Appender values.

The lane removes this work from the backfill data plane:

```text
LedgerBatch protobuf construction
  -> deterministic protobuf encoding and hashing
  -> one JSON document per Bronze row
  -> JSON unmarshal back to the same typed row
```

Fixture mode remains available as the reference and regression lane. The raw
lane pins the source and extraction module versions in the job manifest and
pins materialization timestamps to the job's watermark timestamp. Raw XDR is
hashed before the borrowed buffer can be reused.

Fresh archive extraction exposed an assumption hidden by recorded fixtures:
some state extractors can return the same logical rows in a different order.
Ordering Parquet only by a staging ordinal therefore produced different file
hashes. Publication now orders by every public column and uses the private
ordinal only to break ties between identical rows. Two independent 30-ledger
archive runs matched on SHA-256, bytes, rows, and schema for all 18 files.
Two final independent 10-ledger runs after narrowing timestamp normalization
also matched all 18 files byte-for-byte. A Parquet query confirmed transaction
timestamps still come from ledger close time rather than the pinned
materialization time.

### Direct 120-ledger result

The direct sweep used the same mainnet range `62,080,000-62,080,119`, the
public AWS archive in `us-east-2`, one ledger per object, 64,000 objects per
partition, 50 archive fetch workers, and a local Ryzen 9 7940HS host. Unlike
the fixture benchmark, these measurements include archive acquisition and
Stellar extraction.

```text
raw XDR bytes:     210,865,168
Bronze rows:       974,166
total output rows: 974,406
```

| Workers | Aggregate ledgers/s | Rows/s | Raw input MB/s | Efficiency vs 1 | Peak RSS per worker |
|---:|---:|---:|---:|---:|---:|
| 1 | 5.79 | 47,000 | 10.17 | 100.0% | 1,737,140 KiB |
| 2 | 7.22 | 58,661 | 12.69 | 62.4% | 1,157,700 KiB |
| 4 | 9.15 | 74,304 | 16.08 | 39.5% | 784,988 KiB |

The one-worker critical path was:

```text
archive source waits:                      7.59s
view validation:                           0.07s
full LedgerCloseMeta decode:               0.82s
stellar-extract table extraction:          2.44s
pinned timestamp normalization:            0.005s
transaction envelope/result/meta encoding: 0.72s
parallel typed-value projection:           1.09s
DuckDB Appender calls:                     3.37s
canonical Parquet export:                  2.62s
```

Bounded parallel projection reduced that phase from 1.47 seconds to about
0.29 seconds on the 30-ledger probe, roughly a five-fold isolated improvement.
Appender time did not materially change; the new path still uses DuckDB as a
worker-local columnar staging engine.

### Interpretation

The earlier 9.04-ledger/s one-worker fixture result started after archive
fetch, XDR extraction, protobuf construction, and JSON encoding had already
happened. It is a file-materialization rate, not an end-to-end backfill rate.
The direct 5.79-ledger/s result includes those missing phases while moving only
210.9 MB of raw XDR instead of 1.34 GB of expanded protobuf for this range.

On this off-region workstation, four workers reach only 9.15 ledgers/s. A
linear 1,000-ledger/s extrapolation would require about 110 equivalent hosts,
and the falling local efficiency makes that a capacity bound rather than a
deployment plan. At this recent-ledger density, the target also implies about
1.76 GB/s of raw archive input and 8.12 million typed rows/s across the fleet.

The practical next measurement is the same disjoint-shard sweep on compute in
or near `us-east-2`, where the archive source should not consume most of the
critical path. The next writer comparison is native Arrow/Parquet versus the
remaining worker-local DuckDB Appender and canonical sort. Neither result
changes the authority model: workers publish immutable files and manifests;
only the future coordinator may register accepted files into DuckLake.
