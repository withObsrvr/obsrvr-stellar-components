# Bounded Micro-Batch Backfill: Initial Evidence

**Date:** 2026-08-05
**Status:** First implementation slice; not yet a production cutover gate

This report records the first direct-ingest measurements for the additive
`IngestLedgerMicroBatches` path. Archive fetch and processor CPU are excluded.
The implementation follows
[`bounded-microbatch-backfill-plan.md`](bounded-microbatch-backfill-plan.md)
and uses the consolidated DuckDB 1.5.5 / Gatekeeper dependency stack.

The measurements show that this path should remain the bounded tail and
cutover mechanism rather than the full-history data plane. The parallel
file-oriented successor is specified in
[`parallel-file-backfill-implementation-plan.md`](parallel-file-backfill-implementation-plan.md).

## Implementation exercised

- explicit `INGEST_PROFILE=live|backfill` admission; the two RPCs cannot write
  the catalog concurrently
- one framed, contiguous micro-batch in flight and one durable range ack
- client bounds by ledger count, encoded protobuf bytes, and Bronze row count
- server revalidation of count, bytes, rows, network, range, and SHA-256 digest
- one shared eight-worker decode budget across the range
- one native staging load and one DuckLake transaction per range
- atomic per-ledger metadata, watermarks, typed rows, and commit receipt
- identical-receipt deduplication after restart
- range replacement for an uncertain commit and rejection of arbitrary sparse
  corrections

Batch age, an explicit Go memory budget, Flow SDK bounded delivery, backfill
telemetry, hard-checkpoint priority, maintenance admission, and fault injection
remain follow-up work.

## Fixture and engine

```text
network:             Public Global Stellar Network ; September 2015
ledger range:        62,080,000-62,080,999
ledgers:             1,000
protobuf bytes:      11,212,932,724
Bronze rows:         8,108,409
manifest sha256:     7b1e79bfb8f315d8c11edd88ce055749709adb74b9f73899d1aaadfb656dd5ab

DuckDB:              1.5.5
duckdb-go:           v2.10505.0
duckdb-go-bindings:  v0.10505.0
DuckLake extension:  d8a1881e
Quack extension:     c154811
stellar-extract:     v0.1.4
decode workers:      8
checkpoint threshold: 1GB emergency fallback
```

Every replay verified the manifest and all ten chunk hashes before sending a
ledger. Fixture payloads were deleted after the reports, results, manifest,
and logical fingerprints were retained.

## Results

All runs used fresh catalogs and DuckDB 1.5.5-compatible extensions.

| Candidate | Inline limit | Effective ledgers/txn | Transactions | Elapsed | Ledgers/s | RPC p99 | Snapshots |
|---|---:|---:|---:|---:|---:|---:|---:|
| one-ledger control | 256 | 1 | 1,000 | 327.926s | 3.049 | 470.607ms | 1,010 |
| target 25, cap 256 MiB / 500k rows | 0 | 17-25 | 45 | 104.592s | 9.561 | 2.151s | 55 |
| target 50, cap 512 MiB / 500k rows | 0 | 16-50 | 22 | 99.874s | 10.013 | 4.397s | 32 |

The 25-ledger candidate is `3.14x` the current-stack one-ledger control and
reduces snapshots by `18.4x`. The 50-ledger candidate is `3.28x` faster and
reduces snapshots by `31.6x`, but it only narrowly clears the initial
10-ledger/s floor while doubling buffered bytes and uncertain replay scope.
It is an experiment result, not a selected production default.

An earlier target-25 run using per-ledger nested decode fan-out and inline
limit 256 reached only `6.829` ledgers/s. The shared range worker budget plus
backfill-specific no-inlining profile materially improved the result. A
controlled matrix is still required to attribute each change independently.

## Correctness

Both bounded candidates produced:

```text
watermarks:              1,000
watermark min/max:       62,080,000 / 62,080,999
watermark gaps:          0
ledger metadata rows:    1,000
partial metadata commits: 0
receipt coverage gaps:   0
receipt-covered ledgers: 1,000
```

The 25/256 and 50/512 catalogs had identical count-and-hash fingerprints for
every logical table except `ingest_microbatch_commits`, whose range boundaries
are intentionally different. This is the transaction-grain parity gate.

The one-ledger control initially showed 236 symmetric differences in
`token_transfers_stream_v1`. Diagnosis found no missing or duplicated logical
rows: `amount_raw` and every other field matched. Seven low-row-count ledgers
were inlined in the one-ledger catalog (1,351 rows total), while batching made
the same table file-backed. DuckLake's inline and Parquet representations of
the derived `DOUBLE amount` differed by at most `1.11e-16`. Setting the
backfill inline limit to zero made the two bounded transaction grains exactly
match. Cross-storage parity should use canonical `amount_raw` or an explicit
floating-point tolerance; exact bitwise `DOUBLE` equality is not a safe
cross-inline/file invariant.

After restarting the 50/512 server, resending all 22 exact ranges returned
deduplicated acknowledgements for all 1,000 ledgers. Receipt count remained
22, snapshot count remained 32, and no data transaction was added.

## Full-mainnet projection

These are sink-only fixed-rate projections from a dense recent-mainnet sample,
not end-to-end production forecasts.

| Rate | Fixed 63,804,680 ledgers | Catch a tip growing at 0.2 ledger/s |
|---:|---:|---:|
| 3.049 ledgers/s control | 242.2 days | 259.2 days |
| 9.561 ledgers/s | 77.2 days | 78.9 days |
| 10.013 ledgers/s | 73.8 days | 75.3 days |

Early history is generally smaller, but archive fetch, extraction, Flow
delivery, checkpoints, maintenance, and shared-host resource contention are
absent here. Multi-era fixtures and an end-to-end bounded Flow run are required
before using this projection for scheduling a production cutover.

## Next gates

1. Record peak RSS and enforce a Go-side memory budget for buffered protobufs
   plus decoded values.
2. Benchmark 256/384/512 MiB caps with controlled decode-worker and inline
   settings, including checkpoints and bounded maintenance.
3. Add backfill-specific Prometheus phase, size, retry, admission, and pause
   telemetry.
4. Land bounded ordered Flow delivery and the sink-side age/drain/retry
   assembler.
5. Run receive/decode/stage/commit/post-commit-ack crash injection and prove the
   same receipt/parity/resume gates.
6. Capture early-, middle-, and recent-history samples for a weighted forecast.

Until those gates pass, backfill remains disabled by the default
`INGEST_PROFILE=live` configuration.
