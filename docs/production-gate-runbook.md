# Production Gate Runbook

Date: 2026-07-02. Revised: 2026-07-23 (write-path rewrite: envelope removal,
inline-first tiering + maintenance, Parquet-staged transport, BronzeIngestService).

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
zero watermark gaps, exact count parity, XDR enrichment intact.

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

Production-gate integration still requires live primary and target Quack
endpoints:

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
- Nomad liveness wiring: both jobs validated as repo templates; not yet
  applied to the prod infra repo.
- Replica snapshot-expiry/schema-drift/token-redaction demos: not yet run
  against live primary/target Quack endpoints.
- 1k-ledger backfill gap query: passed 2026-07-05 (pre-rewrite transport);
  re-run against the ingest-rpc path before shipping.
