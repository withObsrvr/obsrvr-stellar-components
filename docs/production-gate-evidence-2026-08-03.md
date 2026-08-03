# Production Gate Evidence — 2026-08-03

This record preserves the results of telemetry reconciliation, coordinated
manual and 64MiB checkpoint gates, the 1,000-ledger ingest-RPC gate, and the
live local two-Quack replica gate. Raw logs remain in the listed `/tmp`
directories on the machine that ran the gates.

## Workstream 1 telemetry reconciliation gate

A fresh 30-ledger sample used real mainnet batches `62080000`–`62080029` and
the production ingest profile (`DUCKDB_CHECKPOINT_THRESHOLD=1GB`,
`DUCKLAKE_INLINE_ROW_LIMIT=256`). `scripts/ducklake-telemetry-gate.sh` scraped
the server and sink before shutdown and reconciled metrics against both logs.

Results:

```text
expected acknowledgements:       30
server success batches:          30
server/sink log acknowledgements: 30 / 30
sink round-trip observations:    30
all seven server phase counts:   30 each
server error batches:            0
server retries / sink retries:   0 / 0
over-budget ledgers:             0
server total histogram sum:      7.957392461s
server total average:            0.265246s
sink round-trip sum / average:   8.496332658s / 0.283211s
six engine-phase sum:            7.952607701s
unaccounted request/ack overhead: 0.004785s (0.060%)
catalog gauge / stat:            12,288 / 12,288 bytes
WAL gauge / stat:                10,621,253 / 10,621,253 bytes
```

The local gate proves one observation per successful acknowledgement, close
phase/total reconciliation, and exact scrape-time file gauges. It does not
claim target Nomad/Prometheus/Grafana acceptance or a hard 400ms maximum from a
30-ledger sample.

Raw evidence: `/tmp/obsrvr-ducklake-telemetry-gate-20260803`.

## Coordinated manual checkpoint gate

The same real-mainnet 30-ledger profile was run with the shared writer
coordinator and authenticated manual checkpoint endpoint enabled.

The first run falsified the original SQL assumption: `CHECKPOINT stellar_lake`
returned success after 550ms but grew the catalog WAL from `10,621,263` to
`10,940,096` bytes. Upstream DuckLake implements a logical-attachment
checkpoint as data-file flush, snapshot expiry, compaction, old-file cleanup,
and orphan cleanup. It is not the DuckDB metadata-WAL checkpoint.

The corrected controller targets DuckLake's hidden metadata attachment:

```sql
CHECKPOINT __ducklake_metadata_stellar_lake;
```

Corrected-run results:

```text
requested/acknowledged ledgers:  30 / 30
checkpoint result:               success
checkpoint duration:             0.054983670s
checkpoint error observations:   0
WAL before checkpoint:           10,621,220 bytes
WAL after checkpoint:            0 bytes
catalog after checkpoint:        10,498,048 bytes
server/sink retries:             0 / 0
```

The gate verifies that ingest and manual checkpointing share one writer
coordinator and that the checkpoint metric/file evidence agrees. It does not
yet prove crash recovery or checkpoint duration at the candidate maximum WAL.

Raw evidence: `/tmp/obsrvr-ducklake-manual-checkpoint-gate-20260803`.

## Explicit checkpoint candidate sweep

`scripts/ducklake-checkpoint-gate.sh` ran all four candidates against real
mainnet ledger batches:

| Candidate | Observed WAL | Duration | Ledgers | Gaps | Ingest >400ms |
|---|---:|---:|---:|---:|---:|
| 64 MiB | 67,307,196 B | 0.172250732s | 132 | 0 | 1 |
| 128 MiB | 153,589,657 B | 0.372306273s | 260 | 0 | 2 |
| 256 MiB | 300,343,110 B | 0.699943115s | 516 | 0 | 25 |
| 512 MiB | 620,088,779 B | 1.369335457s | 1,028 | 0 | 30 |

Every checkpoint reduced WAL to zero and emitted exactly one success with zero
checkpoint errors. Every run retained exact requested watermark count/min/max,
zero gaps, and zero ingest retries. All observed durations pass the proposed
`<3s` explicit-checkpoint target.

The over-budget ingest counts are independent evidence that saturated ingest is
not a hard 400ms contract. Full logical parity/resume and crash scenarios remain
open.

Raw evidence:
`/tmp/obsrvr-ducklake-checkpoint-gate-{64,128,256,512}mib-20260803`.

## 1,000-ledger ingest-RPC chaos gate

Command profile:

```bash
QUACK_CHAOS_RUNTIME_DIR=/tmp/obsrvr-ingest-rpc-1k-20260803 \
QUACK_CHAOS_START_LEDGER=62080000 \
QUACK_CHAOS_END_LEDGER=62080999 \
QUACK_CHAOS_REPLAY_TIMEOUT=900 \
QUACK_CHAOS_BASELINE_TIMEOUT=900 \
QUACK_CHAOS_INGEST_TIMEOUT=120 \
DUCKLAKE_INLINE_ROW_LIMIT=256 \
make test-ingest-chaos
```

Correctness results:

```text
kill-mid-stream failure: expected non-zero
maintenance during replay: passed
replay committed ledgers: 1000
baseline committed ledgers: 1000
watermark count: 1000
watermark min/max: 62080000 / 62080999
watermark gaps: 0
chaos-vs-baseline parity differences: 0
typed/XDR/Soroban gate failures: 0
```

Observed sink-side commit latency over the 1,000-ledger runs:

```text
replay:   median 296ms, mean 318ms, p95 414ms, p99 864ms, max 1.923s
baseline: median 297ms, mean 328ms, p95 421ms, p99 1.088s, max 6.598s
```

The correctness gate passed. The longer run does **not** support treating
400ms as a hard per-ledger upper bound: 62 replay commits and 61 baseline
commits exceeded 400ms. Most large server-side outliers were commit-heavy;
the largest sink-observed baseline outlier exceeded the corresponding logged
server phases, so the tail needs a separate latency diagnosis before making a
hard-SLO claim. The root-cause analysis and controlled checkpoint-threshold A/B
results are in `docs/ingest-latency-diagnosis-2026-08-03.md`.

Raw evidence: `/tmp/obsrvr-ingest-rpc-1k-20260803`.

## Live local two-Quack replica gate

The gate started two real `quack-ducklake-server` processes:

- primary catalog owner (`stellar_lake`)
- serving replica owner (`serving_lake`)

`ducklake-replica-sync` ran in `TARGET_MODE=quack` against two source tables
with `LEDGER_BATCH_SIZE=2`.

Results:

```text
initial source rows: transactions=4, operations=4
initial target rows: transactions=4, operations=4
initial successful checkpoints: 2
checkpoint snapshot: 7
snapshot after two additional primary commits: 9
snapshot-expiry full-resync starts: 2
snapshot-expiry full-resync checkpoints: 2
post-recovery source/target EXCEPT ALL differences: 0
```

Schema drift was then introduced by adding source-only column `drift_probe` to
`bronze.transactions_row_v2`. The sync exited non-zero with:

```text
schema drift for bronze.transactions_row_v2:
missing target columns: drift_probe
```

The later `bronze.operations_row_v2` table still advanced to snapshot 10,
proving per-table failure isolation. The transaction checkpoint remained at
snapshot 9 with `status=error`; the operation checkpoint reached snapshot 10
with `status=ok`.

Neither the primary nor target test token appeared in captured logs or exported
checkpoint evidence.

### Integration defect found and fixed

DuckLake 1.5.4 reported expired history as:

```text
Invalid Input Error: No snapshot found at version 8
```

`isMissingSnapshotError` recognized `snapshot ... not found` but not this exact
word order, so replica sync recorded an error instead of entering full resync.
The classifier now recognizes `no snapshot found`, and a regression test uses
the exact engine message. Re-running the complete two-server gate passed.

Raw evidence: `/tmp/obsrvr-two-quack-replica-gate-20260803`.
