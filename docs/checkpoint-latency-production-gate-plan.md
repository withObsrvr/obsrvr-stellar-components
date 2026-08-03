# Checkpoint and Ingest-Latency Production Gate Project

**Date:** 2026-08-03  
**Status:** In progress — Workstream 1 implementation started 2026-08-03  
**Scope:** `quack-ducklake-server`, `ducklake-sink`, test tooling, telemetry,
and Nomad deployment configuration

## Purpose

Turn the ingest-RPC latency diagnosis into an operationally complete production
gate. The project must prove that DuckLake catalog checkpoints are scheduled,
bounded, recoverable, observable, and tested under realistic Stellar ledger
cadence.

The existing `DUCKDB_CHECKPOINT_THRESHOLD=1GB` setting is an interim mitigation.
It moves automatic checkpoint stalls later but does not remove checkpoint work
or prove a hard latency maximum.

Related evidence:

- `docs/ingest-latency-diagnosis-2026-08-03.md`
- `docs/production-gate-evidence-2026-08-03.md`
- `docs/production-gate-runbook.md`
- `docs/current-state.md`

## Problem

DuckDB defaults `checkpoint_threshold` and `wal_autocheckpoint` to `16 MiB`.
During sustained ingest, the DuckLake catalog WAL repeatedly crosses that limit.
The checkpoint then runs synchronously inside the ledger's `tx.Commit()`, making
one ledger pay the entire checkpoint cost.

The 1,000-ledger run observed a 6.460-second commit. Controlled experiments
confirmed checkpointing as causal:

| Checkpoint threshold | Median | p95 | p99 | Max | Ledgers over 400ms |
|---|---:|---:|---:|---:|---:|
| DuckDB default, 16 MiB | 296ms | 400ms | 592ms | 2.504s | 33/650 |
| 1GB | 288ms | 369ms | 418ms | 918ms | 13/650 |
| 1MB inverse test | 381ms | 835ms | 1.365s | 1.372s | 39/100 |

Deferring checkpoints improved steady-state latency, but a file-backed catalog
must eventually checkpoint. Production readiness therefore requires an
explicit policy rather than an arbitrarily large automatic threshold.

## Product and operational contracts

### Live ingest

Live ingest follows Stellar ledger cadence, currently approximately five
seconds. Its target is:

```text
ledger arrival -> queryable and acknowledged: <400ms
```

Checkpoint work should run in the idle interval after one ledger commits and
before the next arrives.

### Backfill

Backfill intentionally saturates the write path. Its contract is:

```text
correctness + throughput + bounded checkpoint pauses
```

A hard per-ledger 400ms maximum is not required during saturated backfill. A
single catalog cannot perform checkpoint I/O off-path when there is no idle
path.

### Hard-SLO boundary

If a checkpoint exceeds the interval before the next live ledger, a single
file-backed DuckDB catalog cannot honestly guarantee a hard 400ms maximum. The
response must be one of:

1. checkpoint a smaller WAL more frequently
2. accept a bounded freshness pause and publish the weaker SLO
3. move the DuckLake catalog to a backend without local DuckDB checkpoint stalls

The project must measure this boundary rather than assume it.

## Target architecture

```text
BronzeIngestService
  -> shared writer coordinator
       -> ledger transaction
       -> checkpoint controller

checkpoint controller
  observes:
    - ingest in-flight state
    - last commit completion
    - catalog WAL bytes
    - last successful checkpoint
  applies:
    - soft WAL trigger
    - idle-window checkpoint
    - hard WAL admission boundary
  emits:
    - checkpoint duration/result metrics
    - WAL/catalog gauges
    - health degradation and operator-visible logs
```

The controller belongs inside `quack-ducklake-server`. An external periodic job
cannot reliably know whether ingestion is idle and can race with a ledger
transaction.

## Proposed checkpoint policy

Initial configuration candidates:

```text
DUCKDB_CHECKPOINT_THRESHOLD=1GB
CHECKPOINT_ENABLED=true
CHECKPOINT_WAL_SOFT_LIMIT=64MB
CHECKPOINT_WAL_HARD_LIMIT=512MB
CHECKPOINT_IDLE_AFTER=750ms
CHECKPOINT_MAX_AGE=10m
CHECKPOINT_TIMEOUT=30s
```

These limits are experimental starting points. The maximum-WAL gate must select
the final values.

Policy:

```text
WAL below soft limit
  -> do nothing

WAL at or above soft limit, ingest idle
  -> acquire writer coordinator without racing a ledger
  -> execute CHECKPOINT <attach-name>
  -> record duration and post-checkpoint WAL size

WAL at or above soft limit, saturated backfill
  -> defer while below hard limit
  -> report the deferral in telemetry

WAL at or above hard limit
  -> intentionally pause admission
  -> checkpoint before accepting more work
  -> report degraded health until successful

Checkpoint failure
  -> retain WAL
  -> report unhealthy/degraded
  -> retry with bounded backoff
  -> never pretend that a failed checkpoint succeeded
```

The catalog-WAL SQL shape is:

```sql
CHECKPOINT __ducklake_metadata_stellar_lake;
```

DuckLake internally attaches its file-backed metadata database as
`__ducklake_metadata_<attach-name>`. Checkpoint that hidden DuckDB database to
merge and truncate `<catalog>.wal`. Do **not** use `CHECKPOINT stellar_lake` for
this purpose: DuckLake overrides checkpointing on the logical attachment to run
flush/expiry/compaction/physical-cleanup maintenance, and the operation can grow
the metadata WAL rather than truncate it. The metadata attach name must be
sanitized and configurable.

## Workstream 1 — Commit and checkpoint telemetry

### Goal

Make latency and checkpoint behavior visible without parsing text logs.

### Implementation

Promote `github.com/prometheus/client_golang` from an indirect to a direct
dependency. Serve `/metrics` from the existing health HTTP server.

Refactor ingest timing so every phase is measured explicitly:

```text
decode
staging clear + appender writes
preface
staging-to-DuckLake transfer
commit
post-commit staging cleanup
total server receive-to-ack
sink send-to-ack
```

Histograms:

```text
obsrvr_ducklake_ingest_phase_seconds{
  phase="decode|staging|preface|transfer|commit|cleanup|total"
}

obsrvr_ducklake_checkpoint_duration_seconds{
  trigger="idle|manual|hard_limit",
  result="success|error"
}

obsrvr_ducklake_ingest_rpc_round_trip_seconds
```

Recommended buckets:

```text
10ms, 25ms, 50ms, 75ms, 100ms,
150ms, 200ms, 250ms, 300ms, 350ms,
400ms, 500ms, 750ms, 1s, 2s, 5s, 10s
```

Counters and gauges:

```text
obsrvr_ducklake_ingest_batches_total{result,replayed}
obsrvr_ducklake_ingest_retries_total
obsrvr_ducklake_ingest_over_budget_total
obsrvr_ducklake_ingest_inflight
obsrvr_ducklake_ingest_last_ledger

obsrvr_ducklake_checkpoint_total{trigger,result}
obsrvr_ducklake_checkpoint_deferred_total{reason}
obsrvr_ducklake_checkpoint_last_success_timestamp_seconds
obsrvr_ducklake_catalog_wal_bytes
obsrvr_ducklake_catalog_file_bytes
```

Do not use ledger sequence, transaction hash, table name, or token values as
metric labels. Over-budget logs may include ledger sequence for diagnosis, but
metrics must remain bounded-cardinality.

### Alerts

Initial alerts:

- any live ledger exceeds 400ms
- p99 live ingest exceeds 400ms for 15 minutes
- WAL remains above the soft limit for 15 minutes
- WAL reaches the hard limit
- checkpoint fails
- checkpoint exceeds the available idle budget
- last successful checkpoint is too old
- ingest retries or uncertain commits occur

### Acceptance

- `/metrics` is scrapeable through the Nomad service.
- Histograms account for every successfully acknowledged ledger.
- Total server timing approximately equals the sum of measured phases.
- WAL gauges match the catalog and `.wal` files on disk.
- Tests use isolated Prometheus registries and do not leak global collectors.

### Implementation checkpoint — 2026-08-03

The telemetry/manual-checkpoint slice merged through PR #7 as `63e9944`:

- Prometheus is a direct dependency and both the server and sink use isolated
  registries.
- The server health listener serves `/metrics`; the Nomad template registers a
  dedicated metrics service on that listener.
- Successful acknowledgements record decode, staging, preface, transfer,
  commit, cleanup, and total receive-to-ack histograms. Failed commit attempts
  are accumulated into the eventual acknowledged ledger's phase totals.
- The sink records successful send-to-ack round trips and bounded retries.
- Batch result/replay, retry, over-budget, in-flight, last-ledger, catalog-file,
  and WAL gauges/counters are wired. Checkpoint collectors are defined for the
  controller work but remain zero until Workstreams 2–3 invoke them.
- Initial Prometheus rules and a Grafana dashboard are under
  `deploy/monitoring/`.
- Unit tests scrape isolated registries and verify live catalog/WAL file gauges.
- `go test ./...`, `go vet ./...`, dashboard JSON validation, Nomad formatting,
  offline job validation, and Prometheus 3.13.1 `promtool check rules` pass.
  Latitude testnet target/rule acceptance is recorded below.

The local reconciliation gate now passes for 30 real mainnet ledgers
`62080000`–`62080029`: every server phase and total histogram, server success
counter, sink round-trip histogram, and both acknowledgement logs counted 30;
errors, retries, and over-budget ledgers were zero. Total server time was
`7.957392461s` versus `7.952607701s` across the six measured phases, leaving
`4.785ms` (`0.060%`) unaccounted. Catalog and WAL gauges exactly matched
`stat` at `12,288` and `10,621,253` bytes. The reusable gate is
`scripts/ducklake-telemetry-gate.sh`; raw evidence is retained in
`/tmp/obsrvr-ducklake-telemetry-gate-20260803`.

Static inspection of the Latitude mainnet/testnet Prometheus packs confirms
Nomad service discovery already requests `/metrics`, so the dedicated
health-port metrics service has the correct discovery shape. As the safer
incremental fix, both packs' existing service-name drop regexes now also exclude
`quack-ducklake-primary` and `quack-ducklake-ingest`; only the dedicated metrics
service remains eligible. Both packs render successfully with Nomad Pack.
Mainnet's invalid `scrape_timeout: 10s` / `scrape_interval: 5s` pairing is
corrected to a
5-second timeout, and both complete Prometheus configs now pass Prometheus
3.13.1 validation.

Latitude testnet acceptance passed on 2026-08-03. The checkpoint-disabled,
digest-pinned Quack canary is healthy; `quack-ducklake-metrics` is `UP`; 279
`obsrvr_ducklake_*` series are queryable; the catalog gauge reports 12,288
bytes; protocol services have zero active scrape targets; all eight
`nomad_service="quack-ducklake-metrics"` rules are healthy; and no DuckLake
alerts fire. Source alert-scoping PR #8 and infra PR #2/#3–#7 are merged. Raw
evidence is retained at
`/tmp/obsrvr-ducklake-testnet-telemetry-acceptance-20260803`.

Still open in Workstream 1: the target testnet stack has no Grafana deployment,
so visual dashboard import/acceptance needs a target or explicit deferral.
Mainnet deployment remains separate. Final WAL alert thresholds remain gated on
parity/recovery testing and selected limits.

## Workstream 2 — Writer coordination and explicit checkpointing

### Goal

Execute checkpoints only when ingestion is idle or when a declared hard WAL
boundary requires an intentional pause.

### Implementation

Add a shared writer coordinator to `quack-ducklake-server`.

The ingest path holds the coordinator while decoding/staging and committing a
batch. The checkpoint controller uses `TryLock`, then rechecks all conditions
after acquiring the lock. A dedicated configured `sql.Conn` executes the
checkpoint; it must not run concurrently with another operation on the ingest
connection.

Controller state:

```text
in-flight ledger count
last request time
last commit completion time
last checkpoint start/end/result
current WAL bytes
checkpoint pending flag
consecutive checkpoint failures
```

The controller should inspect `<catalog-path>.wal` with `os.Stat`, avoiding a
DuckDB query merely to decide whether DuckDB is idle.

A manual checkpoint trigger should use an internal authenticated administration
surface or test hook. It must use the same controller and writer coordinator;
it must not bypass policy with an uncoordinated SQL call.

### Concurrency caveat

Quack remote SQL clients currently execute outside the ingest mutex. Production
operations must continue scheduling maintenance and materialization carefully.
This project coordinates server-owned ingest and checkpoint work; generic Quack
write arbitration is a separate architectural expansion unless testing proves
it is required for the gate.

### Acceptance

- A checkpoint never starts while an ingest transaction is active.
- A live idle interval can trigger and complete a checkpoint.
- Saturated backfill defers checkpoints below the hard limit.
- The hard limit creates an intentional, measured pause rather than an
  accidental engine auto-checkpoint.
- Failed checkpoints preserve health/error state and retry with bounded backoff.
- DuckDB's automatic threshold remains above the controller hard limit as an
  emergency fallback.

### Implementation checkpoint — coordinated manual checkpoint

The shared writer coordinator and authenticated manual checkpoint primitive are
implemented in the working tree:

- ingest holds the coordinator across decode, staging, transfer, and commit
- manual checkpointing uses `TryLock` and returns HTTP 409 with an
  `ingest_active` deferral when ingest owns the writer
- checkpoint SQL runs on a dedicated configured `sql.Conn`
- `POST /admin/checkpoint` is disabled by default, requires
  `CHECKPOINT_ADMIN_TOKEN`, and is bounded by `CHECKPOINT_TIMEOUT`
- success/error duration, count, and last-success metrics are wired
- a failed checkpoint persists error/consecutive-failure state and degrades
  `/healthz` until a later successful checkpoint clears it

The first real gate exposed an important correction to the original plan.
`CHECKPOINT stellar_lake` succeeded but grew the metadata WAL from `10,621,263`
to `10,940,096` bytes because DuckLake overrides logical-attachment
checkpointing with flush/expiry/compaction/physical-cleanup maintenance. The
catalog WAL belongs to DuckLake's hidden metadata attachment. Targeting
`CHECKPOINT __ducklake_metadata_stellar_lake` fixed the gate.

A 30-real-mainnet-ledger run then passed: the coordinated checkpoint completed
in `54.984ms`, reduced WAL from `10,621,220` bytes to zero, grew the catalog to
`10,498,048` bytes, emitted one successful manual checkpoint observation with
no errors, and preserved all ingest telemetry counts. Evidence:
`/tmp/obsrvr-ducklake-manual-checkpoint-gate-20260803`.

Local packaging also exposed and fixed two deployment blockers: DuckDB CGO
binaries require `distroless/cc-debian12` rather than `base-debian12`, and the
Nomad `0.0.0.0` Quack bind requires the already security-gated
`QUACK_ALLOW_OTHER_HOSTNAME=true`. PR #7 merged as `63e9944`; immutable server,
maintenance, and sink images are published. The digest-pulled server passes
`/healthz`, `/metrics`, authentication rejection, and an authenticated metadata
checkpoint, and the checkpoint-disabled Latitude testnet canary is healthy.

Manual requests now make at most three attempts with bounded exponential
backoff. A real engine error from an absent metadata database produced exactly
three errors/two backoffs, persistent health 503, and cleared inflight state;
a subsequent process with the correct target checkpointed successfully,
restored health 200, and reduced WAL. Generic remote Quack writes remain outside
this coordinator as documented.

## Workstream 3 — Maximum-WAL checkpoint and recovery gate

### Goal

Select safe soft/hard WAL limits and prove recovery at the maximum supported
size.

### Harness

The initial explicit-checkpoint harness is implemented at:

```text
scripts/ducklake-checkpoint-gate.sh
```

It wraps the real-ledger telemetry/manual-checkpoint gate, requires the WAL to
reach the requested minimum, verifies reduction, compares sorted deterministic
logical-row fingerprints against a no-checkpoint baseline, resumes with the
next ledger, and checks watermark count/min/max/gaps. Separate crash and
checkpoint-interruption harnesses are implemented at
`scripts/ducklake-crash-recovery-gate.sh` and
`scripts/ducklake-kill-checkpoint-gate.sh`; real failure/backoff is covered by
`scripts/ducklake-checkpoint-failure-gate.sh`.

The harness ingests realistic batches while monitoring `<catalog>.wal`. It runs
at candidate WAL sizes:

```text
64MB
128MB
256MB
512MB
```

Add larger sizes only if the measured recovery objective requires them.

### Scenario A — Explicit checkpoint

For each WAL size:

1. record catalog and WAL bytes
2. trigger a coordinated checkpoint
3. measure duration
4. assert WAL reduction
5. run watermark count/min/max/gap checks
6. compare logical contents against a never-failed baseline
7. resume ingestion and verify the next ledger commits

### Scenario B — Crash recovery

For each WAL size:

1. `SIGKILL` the server before checkpoint
2. restart against the same catalog and data path
3. measure process start to healthy and ingest-ready
4. verify the recovered high watermark
5. run watermark and parity checks
6. resume ingestion and verify replay convergence

### Scenario C — Kill during checkpoint

1. trigger an explicit checkpoint
2. detect checkpoint start through telemetry/test synchronization
3. `SIGKILL` the server
4. restart and measure recovery
5. verify catalog parity, watermarks, and continued ingestion

### Candidate acceptance targets

```text
checkpoint duration at soft limit: <3s
recovery at hard limit:            <30s
watermark gaps:                    0
logical parity differences:        0
partial ledger commits:            0
resume after recovery:             required
```

The measured checkpoint duration must fit the intended live idle window. If it
does not, lower the soft limit or revise the storage/SLO decision.

### Explicit checkpoint candidate sweep — 2026-08-03

All four real-mainnet explicit-checkpoint candidates passed:

| Candidate | Observed WAL | Duration | Ledgers | Gaps | Ingest >400ms |
|---|---:|---:|---:|---:|---:|
| 64 MiB | 67,307,196 B | 0.172s | 132 | 0 | 1 |
| 128 MiB | 153,589,657 B | 0.372s | 260 | 0 | 2 |
| 256 MiB | 300,343,110 B | 0.700s | 516 | 0 | 25 |
| 512 MiB | 620,088,779 B | 1.369s | 1,028 | 0 | 30 |

Every checkpoint reduced WAL to zero, emitted one success and no checkpoint
errors, and retained exact watermark count/min/max with no gaps or ingest
retries. All candidates pass the proposed `<3s` explicit-checkpoint target,
and even the largest observed WAL fits inside the current five-second ledger
interval on this hardware.

The strengthened 64MiB and 512MiB Scenario A runs also passed deterministic
logical parity and next-ledger resume. At the hard candidate, `620,092,928`
bytes checkpointed to zero in `1.266759255s`; 1,029 watermarks were contiguous.

Scenarios B and C passed at the 512MiB hard candidate with observed WALs of
`620,090,984` and `620,093,010` bytes. Recovery completed in `5.058841s` before
checkpoint and `4.440246s` after a synchronized kill during checkpoint. Both
had zero logical parity differences, partial commits, watermark gaps, or
retries and resumed with ledger `62081028`.

These results select 64MiB soft and 512MiB hard candidates for the disabled
idle-controller/cadence experiment. This saturated-ingest evidence still does
not prove the hard 400ms ingest SLO. Evidence is retained under
`/tmp/obsrvr-ducklake-checkpoint-parity-{64,512}mib-20260803`,
`/tmp/obsrvr-ducklake-crash-recovery-512mib-20260803`, and
`/tmp/obsrvr-ducklake-kill-checkpoint-512mib-20260803`.

## Workstream 4 — Cadence-shaped ingest gate

### Goal

Measure live behavior without archive download speed, processor CPU, or a
continuously saturated sink distorting the result.

### Fixture set

Capture approximately 1,000 real mainnet `LedgerBatch` protobufs as a
length-delimited fixture set. Large fixtures may live in object storage with a
repository manifest containing:

```text
network passphrase
ledger start/end
batch count
per-file hashes
schema and extraction versions
object-store URL
```

### Replay driver

Add:

```text
cmd/ingest-replay
```

The driver reads fixtures, opens `BronzeIngestService`, sends one ledger at a
configured schedule, waits for the matching acknowledgement, and writes a
machine-readable result summary.

Profiles:

```text
live:
  cadence: 5s
  jitter: +/-250ms

future:
  cadence: 2s
  jitter: +/-100ms

catch-up:
  100-ledger saturated burst
  then live cadence

checkpoint:
  enough duration/data to observe at least three idle checkpoints

maintenance:
  live cadence while flush/merge/expiry runs
```

Example:

```bash
bin/ingest-replay \
  --fixtures testdata/ledger-batches/pubnet-62080000-62080999.manifest.json \
  --cadence 5s \
  --jitter 250ms \
  --duration 1h \
  --max-latency 400ms \
  --require-checkpoints 3
```

### Test tiers

- CI smoke: 30–60 ledgers at shortened cadence
- nightly: 720 ledgers at real five-second cadence
- release gate: real cadence with multiple checkpoints, maintenance, and one
  restart
- future-cadence experiment: two-second cadence, non-blocking until adopted as a
  product requirement

### Acceptance

- Every requested fixture ledger is acknowledged exactly once per run.
- Watermark range and count match the fixture manifest.
- Gap and parity checks are empty.
- The live run includes at least three successful checkpoints.
- Latency output includes median, p95, p99, maximum, and count over budget.
- A hard 400ms gate passes across checkpoint cycles before that SLO is claimed.

## Delivery sequence

### PR 1 — Telemetry foundation

- explicit ingest phase timings
- Prometheus registry and `/metrics`
- WAL/catalog gauges
- Nomad scrape wiring
- dashboards and initial alerts

### PR 2 — Manual checkpoint and recovery harness

- shared writer coordinator
- timed manual checkpoint primitive
- `ducklake-checkpoint-gate.sh`
- WAL-size sweep and crash scenarios
- choose measured soft/hard limits

### PR 3 — Idle checkpoint controller

- idle detection
- soft/hard WAL policy
- health degradation and bounded retries
- checkpoint metrics
- chaos regression with explicit checkpoints

### PR 4 — Cadence driver and release gate

- fixture manifest and acquisition tooling
- `cmd/ingest-replay`
- live/future/catch-up profiles
- checkpoint-inclusive release gate
- final SLO decision

## Rollout plan

1. Deploy telemetry with checkpoint scheduling disabled.
2. Observe WAL growth and existing checkpoint behavior in a non-production
   catalog.
3. Run the maximum-WAL gate and select limits.
4. Enable manual coordinated checkpoints and verify dashboards/alerts.
5. Enable idle checkpoints in a canary allocation.
6. Run the cadence-shaped release gate.
7. Deploy to the production primary only after parity, recovery, and latency
   gates pass.
8. Retain `DUCKDB_CHECKPOINT_THRESHOLD` above the controller hard limit as an
   emergency fallback.

Rollback is configuration-first: disable the controller and return to the
measured `1GB` interim threshold while preserving WAL durability and existing
crash/replay behavior.

## Risks and rabbit holes

### Risks

- A checkpoint may exceed the live idle interval.
- A large WAL may make restart recovery exceed the Nomad health deadline.
- Generic Quack writes can still race server-owned scheduling.
- Prometheus labels can create accidental high cardinality.
- Fixture replay can stop representing current mainnet row shape as extraction
  evolves.

### Rabbit holes to avoid

- Do not redesign the ledger batch protobuf as part of this project.
- Do not build a generic distributed writer scheduler.
- Do not claim checkpoint work has disappeared because the threshold increased.
- Do not use saturated backfill as the only live-latency benchmark.
- Do not automate physical DuckLake file cleanup in this project; snapshot/file
  retention remains separate.

## Definition of done

This project is complete when:

- checkpoints are explicitly scheduled by the catalog-owning server
- soft/hard WAL limits are selected from measured checkpoint and recovery data
- maximum-WAL crash and kill-during-checkpoint scenarios preserve exact logical
  contents and resume successfully
- a real-cadence run observes multiple checkpoints and reports no unaccounted
  latency phases
- Prometheus histograms and WAL/checkpoint gauges are available in Nomad
- dashboards and alerts distinguish ingest, checkpoint, and recovery failures
- the production runbook contains repeatable commands and retained evidence
- the team either proves the hard 400ms live SLO across checkpoint cycles or
  explicitly publishes the weaker measured contract
