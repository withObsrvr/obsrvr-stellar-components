# Bounded Source Prefetch and Caching Evidence

**Date:** 2026-08-06

**Status:** Passed; the archive prefetch default was the dominant single-worker
limit, and the multi-process regression is a shared source ceiling rather than
local CPU contention

## Purpose

Every prior backfill measurement mixed object acquisition with extraction and
encoding in one wall-clock number. That made two questions unanswerable:

1. how much of a worker's time is actually spent acquiring source bytes; and
2. whether the two-process regression recorded in
   [`typed-arrow-builder-evidence-2026-08-06.md`](typed-arrow-builder-evidence-2026-08-06.md)
   came from concurrent object access or from local CPU contention.

This change adds the measurement seams needed to answer both, then answers
them.

## What this adds

**Stage isolation.** `--stage` selects how much of the pipeline runs:

| Stage | Work performed | Artifacts |
|---|---|---|
| `source` | read raw XDR, hash it in source order | none |
| `extract` | additionally decode, extract, and project typed rows | none |
| `full` | the complete artifact-producing run | Parquet + manifests |

`source` and `extract` are evidence-only and publish nothing. Both report the
same length-prefixed payload digest the artifact accumulator computes, so a
probe is provably reading the same bytes the writer reads. `--stage` is
rejected for anything other than `--source=ledger-stream`.

**Prefetch control.** `--source-buffer-size` and `--source-workers` override
the archive backend's `BUFFER_SIZE` and `NUM_WORKERS` per run instead of
requiring process-wide environment changes.

**Local raw ledger cache.** `--source-cache-dir` enables a read-through cache
of canonical `LedgerCloseMeta` XDR in `internal/rawledgercache`. A cold run
populates it while passing borrowed bytes through unchanged; a run whose exact
range is fully cached is served from local disk and never constructs the object
store backend at all. The cache is measurement infrastructure, not a
publication path:

- a range index is written only after the complete range has been observed, so
  an aborted or byte-capped run leaves the range cold;
- every warm read verifies each file against its recorded size and recomputes
  the range payload digest, failing closed on divergence;
- ranges are scoped to a network passphrase and to exact bounds;
- `--source-cache-max-bytes` stops caching rather than exceeding its ceiling.

## Prefetch is the dominant single-worker limit

Source stage, one process, mainnet `62,080,000-62,080,999`, Obsrvr GCS archive,
1,733,356,100 source bytes. The sweep was run in both directions because a
one-directional sweep cannot distinguish prefetch depth from archive-side cache
warming.

| Buffer / fetchers | Ascending l/s | Ascending MiB/s | Descending l/s | Descending MiB/s |
|---|---:|---:|---:|---:|
| 5 / 2 (default) | 20.018 | 33.09 | 21.154 | 34.97 |
| 10 / 4 | 31.828 | 52.61 | 35.743 | 59.08 |
| 20 / 8 | 42.852 | 70.84 | 49.161 | 81.27 |
| 40 / 16 | 63.624 | 105.17 | 64.434 | 106.51 |
| 64 / 32 | 64.336 | 106.35 | 66.547 | 110.01 |

The descending sweep read the default configuration last, after every object
had already been fetched four times, and still measured 21.15 ledgers/s. The
curve is therefore prefetch depth, not warming. Peak RSS stayed between 88,992
and 112,632 KiB across every depth, so this is not a memory trade.

The measured knee is 40 buffered ledgers and 16 fetchers. Doubling again to
64/32 returns roughly 3% for 17% more resident memory.

## The default starved the writer

Full artifact runs, one process, same range, Arrow/Snappy, four extraction
workers, eight ledgers in flight, two projection workers, two Parquet encoders,
four pending row groups, 16,384-row groups.

| Source configuration | Wall | Ledgers/s | Source | Pipeline wait | Foreground append | Encode worker-s | Peak RSS |
|---|---:|---:|---:|---:|---:|---:|---:|
| 5 / 2 (default) | 51.064s | 19.583 | 49.982s | 34.102s | 15.573s | 17.820s | 955,868 KiB |
| 40 / 16 | 25.348s | 39.450 | 2.941s | 1.316s | 22.522s | 21.944s | 1,313,772 KiB |
| warm cache, no network | 23.970s | 41.718 | 1.186s | 0.099s | 22.366s | 21.702s | 1,259,236 KiB |
| warm cache, repeat | 23.711s | 42.175 | 1.239s | 0.095s | 22.118s | 21.572s | 1,211,840 KiB |

At the default depth the writer spent 34.1 of 51.1 seconds waiting for input.
Raising prefetch to the measured knee doubles end-to-end throughput and reduces
pipeline wait by 96%. Removing the network entirely adds only a further 5.7%,
so a correctly prefetched single worker is CPU-bound, not network-bound.

This also resolves an inconsistency in the prior evidence: 37.32 ledgers/s
cannot be produced by a source lane that delivers 20 ledgers/s. That run was
measured under more favourable archive conditions than today's default-depth
run. Measured under identical conditions today, tuned prefetch reaches 39.45
ledgers/s cold and 42.17 ledgers/s warm.

## The multi-process regression is a shared source ceiling

Source stage, same 1,000 ledgers split evenly across processes:

| Processes × fetchers | Total fetchers | Wall | Aggregate l/s | Aggregate MiB/s |
|---|---:|---:|---:|---:|
| 1 × 16 | 16 | 16.888s | 59.215 | 97.89 |
| 2 × 8 | 16 | 17.628s | 56.728 | 93.78 |
| 2 × 16 | 32 | 16.333s | 61.225 | 101.21 |
| 4 × 16 | 64 | 16.728s | 59.782 | 98.82 |

Acquisition saturates near 100 MiB/s and does not improve with more processes
or more fetchers. One process at the knee already reaches the ceiling, so the
limit is shared host bandwidth rather than per-process concurrency.

Full artifact runs make the consequence explicit:

| Processes | Source mode | Wall | Aggregate l/s | Source | Pipeline wait |
|---|---|---:|---:|---:|---:|
| 1 | warm cache | 23.970s | 41.718 | 1.186s | 0.099s |
| 2 | warm cache | 18.827s | 53.117 | 0.812s | 0.268s |
| 4 | warm cache | 18.657s | 53.598 | 0.517s | 0.492s |
| 1 | cold 40 / 16 | 25.348s | 39.450 | 2.941s | 1.316s |
| 2 | cold 40 / 16 | 26.565s | 37.644 | 19.036s | 14.767s |
| 4 | cold 40 / 16 | 25.937s | 38.555 | 23.603s | 19.267s |
| 2 | cold 5 / 2 | 41.420s | 24.143 | 40.683s | 31.982s |

With the network removed, two processes scale to 53.12 ledgers/s and four are
flat at 53.60, so the host's CPU ceiling is roughly 53.5 ledgers/s and is
reached at two processes. With the network present, every process count lands
near 38 ledgers/s and pipeline wait rises from 1.3s to 14.8s as soon as a
second writer competes for the link.

The answer to the open question is therefore: **concurrent object access, not
local CPU.** Two writers each want about 72 MiB/s of source; the host supplies
about 100 MiB/s in total, so they starve each other. Colocating shard workers
on one box cannot help once its link is saturated.

## The concurrency knee did not move

The four-worker, two-encoder knee was originally selected while the source lane
was starving the writer. Re-measured warm, with no network in the path:

| Extraction workers / encoders | Wall | Ledgers/s | Peak RSS |
|---|---:|---:|---:|
| 4 / 2 | 23.970s | 41.718 | 1,259,236 KiB |
| 6 / 2 | 24.509s | 40.802 | 1,476,956 KiB |
| 8 / 2 | 24.980s | 40.033 | 1,543,816 KiB |
| 4 / 4 | 24.369s | 41.036 | 2,029,688 KiB |

The existing selection stands. Four encoders cost 61% more resident memory for
a slightly worse result.

## Correctness

Artifact identity did not move under any prefetch depth, process count, or
cache mode.

`scripts/ducklake-backfill-file-identity.sh` digests every produced file's
table, Parquet SHA-256, byte size, row count, ledger range, and schema
fingerprint, excluding attempt-local URIs.

| Sharding | Files | Parquet bytes | Output rows | Normalized identity |
|---|---:|---:|---:|---|
| 1 × 1,000 | 22 | 1,115,553,147 | 8,110,413 | `151c8450eac1920da99b8b5ad3474afba1859fb0f53c3135dbdfa22c4550dc25` |
| 2 × 500 | 39 | 1,115,924,052 | 8,110,413 | `06e7281d70dc6d18fa79634fa4f34f5ff5e08979e88bd31f271ea106bea5f95f` |
| 4 × 250 | 73 | 1,116,737,363 | 8,110,413 | `03ce131ab1ceec026b88a6e8a7bbb8018f23c226898f13304424641ae4a21d28` |

Within each sharding the digest was identical across default prefetch, tuned
prefetch, warm cache, and every concurrency setting — seven runs for the
single-shard case and three for each split. Different shardings legitimately
produce different files, because file rolling follows shard boundaries.

The single-shard totals match the previously recorded typed-builder run exactly
at 22 files, 1,115,553,147 bytes, and 8,110,413 rows. The digest value itself
is not comparable to the earlier ad-hoc digest, because the normalization is
now computed by a committed script rather than by hand.

Every 1,000-ledger source probe, at every prefetch depth and in both cold and
warm cache modes, produced the payload digest
`sha256:0ade95b84f367c1856a599fb0c3dedf2fea98c519d82aa46c092a60a004c38d8`.
Cache population cost 0.861s for 1.7 GB and warm verification 0.016s per 20
ledgers.

## Implications for the fleet target

At a fixed 63,804,680-ledger history, one tuned worker projects to 18.7 days
cold and 17.7 days warm. Reaching 1,000 ledgers/s requires 1,733,356,100 bytes
per second of decoded source, roughly 13.9 Gbit/s across the fleet.

Two provisioning statements follow from the two measured ceilings:

- a host held at its CPU ceiling of 53.6 ledgers/s needs about 88.6 MiB/s
  (743 Mbit/s) of dedicated archive bandwidth, and roughly 19 such hosts reach
  the target;
- a host on this test box's link saturates end-to-end near 38.6 ledgers/s
  regardless of process count, and roughly 26 such hosts reach the target.

The gap between those numbers is bandwidth provisioning, not code. Shard
workers must therefore be planned as independently provisioned network
consumers. Adding processes to a bandwidth-saturated host is measurably
negative.

## Reproduction

```text
BACKEND_TYPE=ARCHIVE
ARCHIVE_STORAGE_TYPE=GCS
ARCHIVE_BUCKET_NAME=obsrvr-stellar-ledger-data-pubnet-data
ARCHIVE_PATH=landing/ledgers/pubnet
LEDGERS_PER_FILE=1
FILES_PER_PARTITION=64000
```

```bash
BACKFILL_SOURCE=ledger-stream \
BACKFILL_STAGE=source \
BACKFILL_LEDGER_START=62080000 BACKFILL_LEDGER_END=62080999 \
BACKFILL_SOURCE_BUFFER_SIZE=40 BACKFILL_SOURCE_WORKERS=16 \
BACKFILL_CONCURRENCY=1 \
  scripts/ducklake-file-backfill-benchmark.sh
```

Set `BACKFILL_STAGE=full`, `BACKFILL_WRITER=arrow-parquet`,
`BACKFILL_COMPRESSION=snappy`, `BACKFILL_MAX_ENCODED_BYTES=8589934592`, and
`BACKFILL_MAX_BRONZE_ROWS=20000000` for artifact runs, and
`BACKFILL_SOURCE_CACHE_DIR` to populate or serve the local cache.

## What this does not cover

The cache is a local measurement aid. It is not a distributed read-through
tier, it does not participate in publication or registration, and no artifact
path depends on it. Selecting a production prefetch default for Nomad workers
requires re-running the sweep on the target instance type, because the knee is
a property of the host's link rather than of the code.
