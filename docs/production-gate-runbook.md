# Production Gate Runbook

Date: 2026-07-02. Revised: 2026-08-03 (telemetry reconciliation, shared writer
coordination, corrected metadata-WAL checkpoint target, and first 64MiB gate).

This runbook turns the production-hardening plan gate into repeatable checks.
It records which checks can run locally, which require the Latitude mainnet
Nomad environment, and what evidence to capture before shipping the
Quack/replica topology.

The write path changed substantially on 2026-07-23 — see
`sub-400ms-ingest-phase-handoff-2026-07-23.md` and
`ducklake-write-path-phase-handoff-2026-07-23.md`. Evidence captured before
that date describes the superseded SQL-literal transport and is kept below as
historical record only.

## Local Quack Gate (staged-parquet transport)

Run:

```bash
make test-quack-chaos
```

The harness:

1. builds `quack-ducklake-server`, `ducklake-sink`, `ducklake-maintenance`,
   and `stellar-ledger-processor`
2. starts a local Quack server and verifies `/healthz`
3. runs an archive-backed ingest (`DUCKLAKE_MODE=quack`: typed rows staged as
   per-table Parquet, KB-scale `read_parquet` write script)
4. kills Quack mid-ingest and requires the ingest to fail loudly
5. restarts Quack, replays the same ledger range, and runs a full
   `ducklake-maintenance` cycle (flush inlined data, merge files, expire
   snapshots) concurrently with the active replay
6. runs a never-failed baseline ingest and compares the failed/replayed
   catalog against it with `EXCEPT ALL` (this also proves maintenance is
   invisible to logical contents: the baseline never runs maintenance)
7. verifies the watermark gap query is empty
8. verifies representative typed/XDR/Soroban columns are populated
9. enforces the transport size gates: staged Parquet floor
   `QUACK_CHAOS_MIN_STAGED_MIB` (default `1`, proves the full typed surface
   flows) and write-script ceiling `QUACK_CHAOS_MAX_SCRIPT_KIB` (default
   `256`; a breach means row data leaked back into SQL text)

Evidence captured 2026-07-23 (post-rewrite):

- transport gates: max staged parquet `1.43 MiB`, max script `18.6 KiB` —
  both gates passed
- maintenance-during-replay: succeeded during active ingest
- parity compare and typed/XDR/Soroban gates: passed
- watermark gap query: `0 rows`

## Local Ingest Gate (BronzeIngestService)

Run:

```bash
make test-ingest-chaos
```

Same harness with `QUACK_CHAOS_SINK_MODE=ingest-rpc`: the server starts with
`INGEST_PORT` set, the sink forwards batches over the gRPC stream, and the
same kill/replay/baseline parity, watermark, and typed/XDR/Soroban gates
apply. Transport size gates are skipped (no staged-parquet transport). This
gate proves the ingest path's crash contract: watermark-gated delete-skip,
replay-on-uncertainty, stream reset and retry, and byte-identical recovery.

Performance reference for the ingest path (2026-07-23, local, 6-ledger run,
`DUCKLAKE_INLINE_ROW_LIMIT=256`): 232–318ms per ledger server-side commit,
zero watermark gaps, exact count parity, XDR enrichment intact. The 1,000-ledger
run on 2026-08-03 exposed synchronous DuckDB catalog auto-checkpoints as the
primary source of multi-second tails. `DUCKDB_CHECKPOINT_THRESHOLD=1GB` is an
interim mitigation; see `docs/ingest-latency-diagnosis-2026-08-03.md` before
using the short-run numbers as a hard SLO.

## Local Telemetry Reconciliation Gate

Run a 30-ledger real-mainnet sample through the ingest RPC and scrape both
processes before shutdown:

```bash
make test-telemetry-gate
```

Override the retained evidence directory, range, or timeout with
`TELEMETRY_GATE_RUNTIME_DIR`, `TELEMETRY_GATE_START_LEDGER`,
`TELEMETRY_GATE_END_LEDGER`, and `TELEMETRY_GATE_TIMEOUT`; the runtime
directory must resolve beneath `/tmp`. The gate:

1. builds and starts the server with the 1GB interim checkpoint threshold and
   inline row limit 256
2. runs real archive ledgers through processor, sink, and ingest RPC
3. waits for the expected server last-ledger gauge and sink acknowledgement
   histogram count
4. scrapes server and sink `/metrics`
5. requires every phase, total, server success, sink round trip, and both logs
   to account for every requested ledger exactly once
6. requires zero error batches and retries
7. compares total histogram sum with the six phase sums using a bounded
   request/ack-overhead tolerance
8. compares catalog and WAL gauges byte-for-byte with `stat`

Evidence captured 2026-08-03 for real mainnet ledgers `62080000`–`62080029`:
all seven server histogram counts, both logs, server successes, and sink round
trips were `30`; errors/retries and over-budget ledgers were zero; total server
sum was `7.957392s` versus `7.952608s` across the six phases, leaving `4.785ms`
(`0.060%`) unaccounted; catalog and WAL gauges exactly matched `12,288` and
`10,621,253` bytes on disk. Raw evidence:
`/tmp/obsrvr-ducklake-telemetry-gate-20260803`.

This proves local metric accounting. A real Nomad allocation and target
Prometheus/Grafana rule import remain required before the scrape/dashboard/alert
acceptance item is closed. The Latitude mainnet/testnet Prometheus pack working
tree now explicitly drops the Quack and ingest protocol services, leaving the
health-port metrics service eligible. Mainnet's scrape timeout was corrected
from 10 seconds to match its 5-second interval. Both packs now render and pass
full Prometheus 3.13.1 configuration validation.

## Coordinated Manual Checkpoint Gate

Run the real-ledger telemetry gate with the shared writer coordinator and
manual checkpoint enabled:

```bash
make test-manual-checkpoint-gate
```

The server enables `POST /admin/checkpoint` only when `CHECKPOINT_ENABLED=true`
and `CHECKPOINT_ADMIN_TOKEN` is configured. The request uses a bearer token;
never place that token in a jobspec or evidence file. If ingest owns the writer
coordinator, the endpoint returns HTTP 409 and records an `ingest_active`
deferral.

The gate waits for all ingest acknowledgements, records pre-checkpoint WAL
bytes, triggers one coordinated checkpoint, and requires success metrics plus a
strict WAL reduction. It targets the hidden DuckDB metadata attachment:

```sql
CHECKPOINT __ducklake_metadata_stellar_lake;
```

Do not substitute `CHECKPOINT stellar_lake`: DuckLake maps that logical
attachment checkpoint to maintenance including physical cleanup, rather than a
metadata-WAL checkpoint.

Evidence captured 2026-08-03 for 30 real mainnet ledgers: one successful manual
checkpoint completed in `54.984ms`, reduced WAL from `10,621,220` bytes to zero,
and left a `10,498,048`-byte catalog. All 30 ingest acknowledgements remained
accounted for with zero retries/errors. Raw evidence:
`/tmp/obsrvr-ducklake-manual-checkpoint-gate-20260803`.

## Maximum-WAL Explicit Checkpoint Candidates

Run the first candidate gate:

```bash
make test-checkpoint-gate
```

The default target is 64 MiB. Override `CHECKPOINT_GATE_WAL_MIB`,
`CHECKPOINT_GATE_LEDGER_COUNT`, `CHECKPOINT_GATE_TIMEOUT`, and
`CHECKPOINT_GATE_RUNTIME_DIR` for later candidates. The harness requires the
pre-checkpoint WAL to reach the target, triggers the coordinated metadata
checkpoint, compares deterministic logical rows against a no-checkpoint
baseline, resumes with the next ledger, and requires WAL reduction, zero parity
differences, and contiguous watermarks.

All explicit candidates passed on 2026-08-03:

| Candidate | Observed WAL | Duration | Ledgers | Gaps | Ingest >400ms |
|---|---:|---:|---:|---:|---:|
| 64 MiB | 67,307,196 B | 0.172s | 132 | 0 | 1 |
| 128 MiB | 153,589,657 B | 0.372s | 260 | 0 | 2 |
| 256 MiB | 300,343,110 B | 0.700s | 516 | 0 | 25 |
| 512 MiB | 620,088,779 B | 1.369s | 1,028 | 0 | 30 |

Every checkpoint reduced WAL to zero with no checkpoint errors or ingest
retries, and watermark count/min/max/gaps matched. The largest candidate remains
below the proposed three-second checkpoint target. Independent over-budget
ingest observations mean this saturated sweep is not a hard latency-SLO proof.
Evidence directories follow
`/tmp/obsrvr-ducklake-checkpoint-gate-<candidate>mib-20260803`.

The strengthened 64MiB and 512MiB gates also passed exact deterministic logical
fingerprinting against no-checkpoint baselines and committed the next ledger
with zero gaps. At the 512MiB candidate, an observed `620,092,928`-byte WAL
checkpointed to zero in `1.267s`; all 1,029 watermarks were contiguous.

## Crash and Checkpoint-Interruption Gates

```bash
make test-crash-recovery-gate
make test-kill-checkpoint-gate
make test-checkpoint-failure-gate
```

Override `RECOVERY_GATE_WAL_MIB` or `KILL_CHECKPOINT_GATE_WAL_MIB` to select a
candidate. Both hard-limit 512MiB scenarios passed against observed ~620MB WALs:

- pre-checkpoint `SIGKILL`: recovery and next-ledger resume in `5.059s`
- `SIGKILL` after `obsrvr_ducklake_checkpoint_inflight` became `1`: recovery in
  `4.440s`, followed by successful next-ledger resume
- both: zero logical parity differences, partial commits, watermark gaps, or
  retries across 1,029 contiguous watermarks

The engine-level failure gate targets a deliberately absent metadata database.
It requires HTTP 500, persistent health 503, exactly three bounded attempts and
two retry backoffs, then restarts with the correct metadata target and requires
a successful checkpoint, health 200, and WAL reduction.

These results select 64MiB soft and 512MiB hard candidates for the idle
controller/cadence experiment. They do not enable that controller or prove the
hard 400ms SLO.

## Disabled Checkpoint Controller Gate

```bash
make test-checkpoint-controller-gate
```

The gate runs two small real-ledger profiles. The idle profile requires one
`trigger="idle"` checkpoint after the soft limit and idle duration. The hard
profile uses an intentionally long idle duration and requires one
`trigger="hard_limit"` checkpoint instead. Both require zero ingest errors and
retries. Evidence from 2026-08-03 recorded exactly one expected trigger in each
profile and no unexpected trigger.

Keep `CHECKPOINT_CONTROLLER_ENABLED=false` in deployments until the
cadence-shaped gate observes multiple checkpoints. The 1GB DuckDB threshold
remains the emergency fallback.

## Cadence-Shaped Ingest Release Gate

Build a real mainnet fixture manifest and local chunks as documented in
`testdata/ledger-batches/README.md`, then run:

```bash
CADENCE_GATE_FIXTURES=testdata/ledger-batches/pubnet-62080000-62080999.manifest.json \
make test-cadence-gate
```

The default release profile sends 720 ledgers at five-second cadence with
deterministic +/-250ms jitter and a 400ms scheduled-arrival-to-ack ceiling. It
runs DuckLake flush/merge/expiry maintenance concurrently, requires at least
three successful idle checkpoints, gracefully restarts the server against the
same catalog, resumes with the next fixture ledger, and requires contiguous
watermarks plus one ledger-batch row per commit. By default it also ingests the
same range into a controller-free baseline and compares deterministic logical
fingerprints.

Evidence is retained under `/tmp/obsrvr-ducklake-cadence-gate`. The JSON summary
separates RPC send-to-ack, schedule lag, and scheduled arrival-to-ack so a slow
prior ledger cannot disappear from the live SLO. A 30-ledger shortened-cadence
run is useful for iteration but is not release evidence.

Until the real-cadence run passes across at least three checkpoints, keep the
controller disabled and retain the 1GB DuckDB threshold as the measured interim
fallback.

## Historical evidence (superseded SQL-literal transport)

Local evidence captured on 2026-07-02 (pre-rewrite; script sizes reflect the
old inline-VALUES transport that no longer exists):

- kill-mid-ingest: ingest failed as expected after the Quack server was killed
- replay parity: `parity-diffs.csv` contained only the CSV header
- watermark gap query: `0 rows`
- remote script sizes: max `31.42 MiB` under the old transport
- typed XDR shape: `1092` transaction rows; `0` rows had NULL
  `tx_envelope`, `tx_result`, or `tx_meta`
- Soroban shape: `583` transaction rows populated
  `soroban_resources_instructions`; `583` operation rows populated
  `soroban_operation` and `soroban_arguments_json`

1k-ledger backfill evidence captured on 2026-07-05 (pre-rewrite): ledgers
`62080000`–`62080999`, `events_sent=1000`, `send_errors=0`, parity and gap
queries clean. For a longer watermark run today, use the same env overrides:

```bash
QUACK_CHAOS_START_LEDGER=62080000 \
QUACK_CHAOS_END_LEDGER=62080999 \
QUACK_CHAOS_REPLAY_TIMEOUT=300 \
QUACK_CHAOS_BASELINE_TIMEOUT=300 \
make test-quack-chaos
```

## Nomad Liveness Gate

The production target inspected for mainnet is:

```text
/home/tillman/Documents/infra/environments/prod/latitude/mainnet
```

That environment has Nomad jobs under `nomad/`, but it does not currently
contain the Quack/DuckLake jobs. This repo includes validated starter jobs:

```text
deploy/nomad/quack-ducklake-server.nomad   # Quack + ingest port, inline limit 256
deploy/nomad/ducklake-maintenance.nomad    # 2m flush/merge, 48h snapshot retention
```

Validate locally:

```bash
make validate-nomad
```

Before applying to the infra repo:

1. build and publish the `quack-ducklake-server` and `ducklake-maintenance`
   images referenced by the job variables
2. define a Nomad host volume named `ducklake-primary` on the target clients
3. set Nomad variable `nomad/jobs/obsrvr-stellar-ducklake.quack_token`
4. copy or adapt both jobs into the infra repo's `nomad/` directory
5. run `nomad job plan` against the production cluster
6. verify the `quack-ducklake-health` and `maintenance-health` checks go green

Operational pairing (enforced by comments in both jobs): the server's
`DUCKLAKE_INLINE_ROW_LIMIT=256` produces ~7 small parquet files per ledger,
which the maintenance job's `MAINTENANCE_INTERVAL=2m` merges. Widen both
together, never independently. `SNAPSHOT_RETENTION` (48h) must exceed
replica-sync's worst-case checkpoint lag or replicas fall back to full resync.

Container packaging uses `gcr.io/distroless/cc-debian12`; the plain distroless
base lacks `libstdc++.so.6` required by DuckDB's CGO binary. The Nomad template
also sets `QUACK_ALLOW_OTHER_HOSTNAME=true` because its Quack URI binds
`0.0.0.0`; this remains guarded by `QUACK_INSECURE=true`. A local server image
smoke passed health, metrics, and authenticated manual checkpointing, but no
immutable registry image has been published from this working tree.

Current Quack beta limitation: file-backed DuckLake catalogs require explicit
`QUACK_INSECURE=true`, `QUACK_DISABLE_SSL=true`,
`QUACK_ENABLE_EXTERNAL_ACCESS=true`, and `QUACK_DISABLED_FILESYSTEMS=none`.
The ingest stream is plaintext gRPC with the shared token. Treat the
resulting deployment as isolated-host until Quack supports a narrower
allowlist; front any cross-host exposure with a TLS-terminating proxy.

## Replica Gate

Unit coverage exists for the dangerous branches:

```bash
go test ./components/ducklake-replica-sync/cmd/component
go test ./components/index-materializer/cmd/component
```

The live two-Quack integration gate was run locally on 2026-08-03 with real
primary and serving Quack endpoints. To repeat it:

1. run replica sync once to establish checkpoints
2. expire primary snapshots past a table checkpoint
3. rerun replica sync and verify it performs a bounded full resync for that
   table, checkpoints to the current snapshot, and continues later tables
4. add or remove a primary column and verify the sync fails with a concise
   schema drift diff
5. grep replica logs and `replica.sync_checkpoints.error_message` for the
   primary token and verify no token material is present

The exact command shape is:

```bash
QUACK_URI=quack:<primary-host>:9494 \
QUACK_TOKEN=<primary-token> \
TARGET_MODE=quack \
TARGET_QUACK_URI=quack:<replica-host>:9494 \
TARGET_QUACK_TOKEN=<replica-token> \
SOURCE_TABLES=bronze.transactions_row_v2,bronze.operations_row_v2 \
LEDGER_BATCH_SIZE=1000 \
bin/ducklake-replica-sync
```

Evidence captured 2026-08-03:

- two source tables established target checkpoints
- expiring snapshots behind both checkpoints triggered bounded full resync for
  both tables and advanced both checkpoints
- source/target `EXCEPT ALL` row differences were zero after recovery
- adding source-only column `drift_probe` failed with the exact schema diff while
  the later source table still advanced, proving per-table isolation
- neither Quack token appeared in captured logs or persisted checkpoint output
- the run exposed DuckLake's exact missing-snapshot message (`No snapshot found
  at version N`); the classifier was fixed and that engine message is now a
  regression test

Evidence directory: `/tmp/obsrvr-two-quack-replica-gate-20260803`. The retained
summary for this and the 1,000-ledger ingest gate is
`docs/production-gate-evidence-2026-08-03.md`.

Note: inlined (not-yet-flushed) rows are visible to `table_changes` — verified
2026-07-23 — so replica freshness does not depend on maintenance cadence.

## Gate Status

- Local Quack chaos/replay/parity (staged-parquet transport): passed
  2026-07-23, including concurrent-maintenance scenario and transport gates.
- Local ingest-rpc chaos/replay/parity: passed 2026-07-23 — kill-mid-stream,
  replay via the RPC path, catalog byte-identical to the never-failed
  baseline; maintenance-during-ingest succeeded; typed/XDR/Soroban gates
  passed.
- Typed XDR/Soroban shape: passed 2026-07-23.
- Watermark gap query: passed 2026-07-23.
- Local Workstream 1 telemetry reconciliation: passed 2026-08-03 for 30 real
  mainnet ledgers; all acknowledgement/histogram counts matched, phase sums
  reconciled within 0.060%, and catalog/WAL gauges matched `stat` exactly.
- Coordinated manual checkpoint: passed 2026-08-03; the shared writer
  coordinator kept checkpointing off active ingest, and the corrected hidden
  metadata checkpoint reduced a 10.6MB WAL to zero in 54.984ms.
- Explicit WAL candidate sweep: 64/128/256/512MiB candidates passed; observed
  checkpoint duration remained below 1.37s through a 620MB WAL, with every WAL
  reduced to zero and no watermark gaps/retries. Logical parity/resume and
  crash recovery remain open.
- Nomad liveness and metrics wiring: jobs validate as repo templates; not yet
  applied to the prod infra repo or scraped by the target Prometheus stack.
- Replica snapshot-expiry/schema-drift/token-redaction demos: passed 2026-08-03
  against live local primary/target Quack endpoints; source/target row diffs
  were zero after full-resync recovery.
- 1k-ledger ingest-RPC chaos/replay gate: passed 2026-08-03 for ledgers
  `62080000`–`62080999`; both replay and baseline committed 1,000 ledgers,
  watermark count/min/max matched the requested range, gap and parity results
  were empty, and typed gates passed. Evidence:
  `/tmp/obsrvr-ingest-rpc-1k-20260803`; retained summary:
  `docs/production-gate-evidence-2026-08-03.md`.
