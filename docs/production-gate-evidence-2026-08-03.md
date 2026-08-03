# Production Gate Evidence — 2026-08-03

This record preserves the results of telemetry reconciliation, coordinated
manual and WAL-candidate checkpoint gates, Latitude testnet monitoring acceptance, the 1,000-ledger ingest-RPC gate, and the
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
claim a hard 400ms maximum from a 30-ledger sample.

Raw evidence: `/tmp/obsrvr-ducklake-telemetry-gate-20260803`.

## Latitude testnet monitoring acceptance

The immutable server image from merge `63e9944` was pulled by digest and staged
as a checkpoint-disabled Nomad canary. The accepted Prometheus discovery maps
the retained Nomad service to `nomad_service`, drops the Quack and ingest
protocol services, and loads the eight service-scoped rules.

```text
quack-ducklake-metrics:       UP
DuckLake metric series:       279
catalog file gauge:           12,288 bytes
active protocol targets:      0
healthy service-scoped rules: 8
firing DuckLake alerts:       0
```

The final canary uses fixed container ports plus a task-level
`address_mode="driver"` metrics registration because Latitude testnet lacks the
CNI bridge plugin and Docker-bridged Prometheus cannot hairpin through the
node's public dynamic port. The target stack has no Grafana deployment, so
visual dashboard acceptance remains open. No mainnet deployment was made.

Raw evidence:
`/tmp/obsrvr-ducklake-testnet-telemetry-acceptance-20260803`.

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
coordinator and that the checkpoint metric/file evidence agrees. The later
maximum-WAL scenarios below own parity and crash-recovery proof.

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
not a hard 400ms contract.

The strengthened 64MiB and 512MiB runs additionally exported every logical
table excluding volatile timestamp columns, compared sorted-row SHA-256
fingerprints against no-checkpoint baselines, resumed with the next ledger, and
rechecked the original range. Both had zero parity differences and 1,029/1,029
contiguous watermarks at the hard candidate. The hard-candidate checkpoint saw
`620,092,928` bytes and completed in `1.266759255s`.

Raw evidence:
`/tmp/obsrvr-ducklake-checkpoint-gate-{64,128,256,512}mib-20260803`,
`/tmp/obsrvr-ducklake-checkpoint-parity-{64,512}mib-20260803`.

## Maximum-WAL recovery and interruption gates

The 512MiB hard candidate was tested at an observed ~620MB catalog WAL:

| Scenario | Observed WAL | Recovery | Parity differences | Partial commits | Final watermarks |
|---|---:|---:|---:|---:|---:|
| `SIGKILL` before checkpoint | 620,090,984 B | 5.058841s | 0 | 0 | 1,029 contiguous |
| `SIGKILL` during checkpoint | 620,093,010 B | 4.440246s | 0 | 0 | 1,029 contiguous |

The interruption harness waited for
`obsrvr_ducklake_checkpoint_inflight 1`, proved the HTTP checkpoint request was
interrupted rather than successful, sent `SIGKILL`, and restarted against the
same files. WAL remained `620,093,059` bytes after recovery, proving the process
was killed before checkpoint truncation. The next ledger then committed and all
original logical fingerprints matched the no-failure baseline.

Both scenarios pass the `<30s` recovery objective with zero gaps, parity
differences, partial commits, or retries.

Raw evidence:
`/tmp/obsrvr-ducklake-crash-recovery-512mib-20260803` and
`/tmp/obsrvr-ducklake-kill-checkpoint-512mib-20260803`.

## Real checkpoint failure and bounded retry gate

A real DuckDB catalog error was injected by targeting an absent metadata
database. The endpoint made exactly three bounded attempts with two exponential
backoffs, returned HTTP 500, cleared the inflight gauge, and left `/healthz` at
503 across repeated checks. Restarting with the correct hidden metadata target
completed one checkpoint, restored health 200, and reduced WAL from 182 bytes
to zero.

Raw evidence: `/tmp/obsrvr-ducklake-checkpoint-failure-gate-20260803`.

These results select 64MiB soft and 512MiB hard candidates for the disabled
idle-controller/cadence experiment. They do not prove a hard 400ms SLO.

## Disabled checkpoint-controller trigger gate

The controller remains default-off. Two six-ledger real-data profiles lowered
the thresholds only inside the harness to exercise each trigger:

```text
idle checkpoint successes:       1
hard-limit checkpoint successes: 1
unexpected trigger successes:    0
ingest errors/retries:           0 / 0
controller default enabled:      false
```

Both profiles reconciled every acknowledgement and metric. This is trigger and
coordination evidence only; it does not replace the five-second cadence gate.

Raw evidence: `/tmp/obsrvr-ducklake-controller-gate-20260803-{idle,hard}`.

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
