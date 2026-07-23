# Flowctl Nomad Runner Build and Deploy

This document describes the working build and deploy path for running
`obsrvr-stellar-components` pipelines through `flowctl` inside Nomad.

The current target shape is:

1. Build a single runner image containing `flowctl` and the component binaries.
2. Push that image to Docker Hub.
3. Register the parameterized Nomad job.
4. Dispatch a pipeline run with ledger range metadata.
5. Verify Nomad health checks, flowctl component health, and sink output.

## Why the Runner Image Uses Nix

The first image approach copied Nix-built binaries into a Debian base image:

```dockerfile
FROM debian:bookworm-slim
COPY bin/flowctl /app/bin/flowctl
COPY bin/ducklake-sink /app/bin/ducklake-sink
```

That is brittle for CGO-linked binaries. The binaries contain exact dynamic
loader and shared library paths under `/nix/store`. If the Docker image does not
contain those exact paths, the container can fail with errors such as:

```text
/app/bin/ducklake-sink: error while loading shared libraries: libgcc_s.so.1: cannot open shared object file
```

or:

```text
/bin/sh: /app/bin/ducklake-sink: cannot execute: required file not found
```

The second error usually means the ELF interpreter path itself is missing, not
that the file is absent.

The reliable build path is to build the Docker image with Nix so the image
contains the runtime closure needed by the binaries.

## Runner Contents

The runner image currently contains:

- `flowctl`
- `raw-ledger-source`
- `stellar-ledger-processor`
- `ducklake-sink`
- `postgres-sink`
- `jsonl-sink`
- `index-materializer`

The image entrypoint is:

```text
/app/bin/flowctl
```

Nomad should pass `flowctl` arguments only. Do not set the Nomad task command to
`/app/bin/flowctl` when the image already has that entrypoint, or Docker will run
`flowctl /app/bin/flowctl ...` and fail with:

```text
Error: unknown command "/app/bin/flowctl" for "flowctl"
```

## Current Manual Build Path

The manual test image was built with a temporary Nix expression that:

- imports the local `bin/` directory into the Nix store;
- disables fixup so copied binaries are not rewritten by `patchelf`;
- includes the exact glibc and gcc store paths referenced by the binaries;
- builds a Docker image with `dockerTools.buildLayeredImage`.

The resulting image was loaded and tested locally:

```bash
nix-build /tmp/flowctl-runner-image.nix -o /tmp/flowctl-runner-image
docker load < /tmp/flowctl-runner-image
timeout 8s docker run --rm \
  --entrypoint /bin/sh \
  withobsrvr/obsrvr-flowctl-runner:latest \
  -lc "/app/bin/ducklake-sink"
```

Expected output includes:

```text
Starting Stellar Ledger DuckLake Sink
Stellar Ledger DuckLake Sink is running
```

Then push the image:

```bash
docker push withobsrvr/obsrvr-flowctl-runner:latest
```

The validated image digest from the Nomad smoke test was:

```text
withobsrvr/obsrvr-flowctl-runner:latest@sha256:dd05652a1b9c072b515ae6a31830455a9822016be88b920f8c8a9b5efb146cb8
```

## Recommended Permanent Build Path

Move the temporary Nix image definition into `flake.nix` as a real package, for
example:

```text
packages.x86_64-linux.flowctl-runner-image
```

The package should:

- build or import all runner binaries from pinned flake inputs;
- include `flowctl` from a pinned source;
- include `raw-ledger-source` from the pinned `stellar-raw-ledger-origin` source;
- include `obsrvr-stellar-components` binaries from this repo;
- use `dockerTools.buildLayeredImage`;
- avoid copying Nix-linked binaries into a non-Nix base image;
- produce a Docker archive that can be loaded and pushed.

The Makefile target should become a thin wrapper:

```bash
nix build .#flowctl-runner-image
docker load < result
docker push withobsrvr/obsrvr-flowctl-runner:latest
```

This makes the image reproducible and avoids hand-maintaining shared library
paths.

## Nomad Job Shape

The production smoke job lives outside this repo in the infra repository:

```text
/home/tillman/Documents/infra/environments/prod/latitude/mainnet/nomad/obsrvr-flowctl-pipeline.nomad
```

It is a parameterized batch job. Required dispatch metadata:

- `pipeline_id`
- `team_slug`
- `pipeline_env`
- `start_ledger`
- `end_ledger`

Example dispatch:

```bash
source .secrets
nomad job run obsrvr-flowctl-pipeline.nomad
nomad job dispatch \
  -meta pipeline_id=flowctl-test-007 \
  -meta team_slug=obsrvr \
  -meta pipeline_env=mainnet \
  -meta start_ledger=62080000 \
  -meta end_ledger=62080000 \
  obsrvr-flowctl-pipeline
```

The Nomad job renders a `pipeline.yaml` into the allocation using Nomad template
functions:

```text
{{ env "NOMAD_META_start_ledger" }}
{{ env "NOMAD_PORT_control" }}
```

Do not use literal `${NOMAD_*}` placeholders inside the template body for the
flowctl pipeline YAML. Those values are not expanded in the rendered file the
way shell environment variables are.

## Pipeline Shape

The smoke pipeline uses:

```text
raw-ledger-source -> stellar-ledger-processor -> ducklake-sink
```

Archive source settings used in the test:

```yaml
BACKEND_TYPE: "ARCHIVE"
ARCHIVE_STORAGE_TYPE: "S3"
ARCHIVE_BUCKET_NAME: "aws-public-blockchain"
ARCHIVE_PATH: "v1.1/stellar/ledgers/pubnet"
AWS_REGION: "us-east-2"
LEDGERS_PER_FILE: "1"
FILES_PER_PARTITION: "64000"
NETWORK_PASSPHRASE: "Public Global Stellar Network ; September 2015"
```

The sink writes DuckLake state inside the allocation:

```text
/alloc/ducklake/stellar.ducklake
/alloc/ducklake/stellar.ducklake.wal
/alloc/ducklake/data
```

## Verification

Check allocation state:

```bash
source .secrets
nomad alloc status <alloc-id>
```

A healthy run should show all service checks as `success`:

```text
flowctl-control-plane-tcp            success
raw-ledger-source-health-tcp         success
stellar-ledger-processor-health-tcp  success
ducklake-sink-health-tcp             success
```

Check logs:

```bash
nomad alloc logs <alloc-id> runner
nomad alloc logs -stderr <alloc-id> runner
```

Expected flowctl messages:

```text
✓ Control plane ready
✓ Pipeline is running
Pipeline status {"total_components": 3, "healthy_components": 3}
```

Expected data-path messages:

```text
Source raw-ledger-source: Started streaming events
Source raw-ledger-source: Producer completed
emitting ledger batch ledger=62080000 txs=354 ops=723
Sink writer finished, closing stream {"sink": "ducklake-sink", "events_sent": 1, "send_errors": 0}
```

Verify DuckLake files:

```bash
nomad alloc exec -task runner <alloc-id> /bin/sh -lc \
  "find /alloc/ducklake -maxdepth 3 -type f -o -type d | sort | sed -n '1,120p'"
```

Expected paths include:

```text
/alloc/ducklake/stellar.ducklake
/alloc/ducklake/stellar.ducklake.wal
/alloc/ducklake/data/bronze/transactions_row_v2
/alloc/ducklake/data/bronze/operations_row_v2
/alloc/ducklake/data/main/ledger_batches
/alloc/ducklake/data/main/bronze_rows
```

## Known Warnings

Some components currently log duplicate health server startup attempts:

```text
Health server started on port <port>
Health server error: listen tcp :<port>: bind: address already in use
```

In the successful smoke test this was non-blocking. The first health server
started successfully, components registered with flowctl, Nomad service checks
passed, and the pipeline processed the ledger.

This should still be cleaned up in the component or SDK startup path, but it is
not currently blocking Nomad execution.

## Successful Smoke Test

The first successful remote smoke test used:

```text
pipeline_id: flowctl-test-007
ledger: 62080000
allocation: e6db0ec1
image digest: sha256:dd05652a1b9c072b515ae6a31830455a9822016be88b920f8c8a9b5efb146cb8
```

Results:

```text
Nomad allocation: running
Nomad service checks: success
flowctl healthy components: 3
processor output: ledger=62080000 txs=354 ops=723
ducklake sink: events_sent=1 send_errors=0
DuckLake files: created under /alloc/ducklake
```
