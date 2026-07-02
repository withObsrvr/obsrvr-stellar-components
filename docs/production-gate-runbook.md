# Production Gate Runbook

Date: 2026-07-02

This runbook turns the production-hardening plan gate into repeatable checks.
The code cycles are merged; this document records which checks can run locally,
which require the Latitude mainnet Nomad environment, and what evidence to
capture before shipping the Quack/replica topology.

## Local Quack Gate

Run:

```bash
GOCACHE=/tmp/obsrvr-go-build-cache \
GOMODCACHE=/tmp/obsrvr-go-mod-cache \
make test-quack-chaos
```

The harness:

1. builds `quack-ducklake-server`, `ducklake-sink`, and
   `stellar-ledger-processor`
2. starts a local Quack server and verifies `/healthz`
3. runs an archive-backed Quack ingest
4. kills Quack mid-ingest and requires the ingest to fail loudly
5. restarts Quack, replays the same ledger range, and runs a never-failed
   baseline ingest
6. compares the failed/replayed catalog against the baseline with `EXCEPT ALL`
7. verifies the watermark gap query is empty
8. verifies representative typed/XDR/Soroban columns are populated
9. verifies at least one remote write script is at least
   `QUACK_CHAOS_MIN_SCRIPT_MIB` MiB, default `15`

Local evidence captured on 2026-07-02:

- command: `GOCACHE=/tmp/obsrvr-go-build-cache GOMODCACHE=/tmp/obsrvr-go-mod-cache make test-quack-chaos`
- runtime: `/tmp/obsrvr-quack-chaos`
- ledgers: `62080000` through `62080002`
- kill-mid-ingest: ingest failed as expected after the Quack server was killed
- replay parity: `parity-diffs.csv` contained only the CSV header
- watermark gap query: `0 rows`
- remote script sizes: max `31.42 MiB`; all observed scripts were above
  the `15 MiB` large-ledger threshold
- typed XDR shape: `1092` transaction rows; `0` rows had NULL
  `tx_envelope`, `tx_result`, or `tx_meta`
- Soroban shape: `583` transaction rows populated
  `soroban_resources_instructions`; `583` operation rows populated
  `soroban_operation` and `soroban_arguments_json`

For a longer watermark run, use the same harness with a 1k-ledger range and
larger timeouts:

```bash
QUACK_CHAOS_START_LEDGER=62080000 \
QUACK_CHAOS_END_LEDGER=62080999 \
QUACK_CHAOS_REPLAY_TIMEOUT=3600 \
QUACK_CHAOS_BASELINE_TIMEOUT=3600 \
GOCACHE=/tmp/obsrvr-go-build-cache \
GOMODCACHE=/tmp/obsrvr-go-mod-cache \
make test-quack-chaos
```

## Nomad Liveness Gate

The production target inspected for mainnet is:

```text
/home/tillman/Documents/infra/environments/prod/latitude/mainnet
```

That environment has Nomad jobs under `nomad/`, but it does not currently
contain the Quack/DuckLake jobs. This repo now includes a validated starter job:

```text
deploy/nomad/quack-ducklake-server.nomad
```

Validate locally:

```bash
make validate-nomad
```

Before applying it to the infra repo:

1. build and publish `components/quack-ducklake-server/Dockerfile` as the image
   referenced by `var.quack_image`
2. define a Nomad host volume named `ducklake-primary` on the target clients
3. set Nomad variable `nomad/jobs/obsrvr-stellar-ducklake.quack_token`
4. copy or adapt the job into
   `/home/tillman/Documents/infra/environments/prod/latitude/mainnet/nomad/`
5. run `nomad job plan` against the production cluster
6. verify the registered service check named `quack-ducklake-health` goes green

The job deliberately wires the public Quack service to the health endpoint with:

```hcl
check {
  name     = "quack-ducklake-health"
  type     = "http"
  port     = "health"
  path     = "/healthz"
  interval = "10s"
  timeout  = "2s"
}
```

Current Quack beta limitation: file-backed DuckLake catalogs require explicit
`QUACK_INSECURE=true`, `QUACK_DISABLE_SSL=true`,
`QUACK_ENABLE_EXTERNAL_ACCESS=true`, and `QUACK_DISABLED_FILESYSTEMS=none`.
Treat the resulting job as an isolated-host deployment until Quack supports a
narrower local-file allowlist.

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

## Gate Status

- Local Quack chaos/replay/parity: passed on 2026-07-02.
- Large-ledger Quack script threshold: passed on 2026-07-02 with max
  `31.42 MiB`.
- Typed XDR/Soroban shape: passed on 2026-07-02 for the local Quack run.
- Watermark gap query: passed on 2026-07-02 for the local Quack run.
- Nomad liveness wiring: validated as a repo job template; not yet applied to
  the prod infra repo.
- Replica snapshot-expiry/schema-drift/token-redaction demos: not yet run
  against live primary/target Quack endpoints.
- 1k-ledger backfill gap query: not yet run.
