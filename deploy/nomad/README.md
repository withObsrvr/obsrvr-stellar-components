# Nomad Flowctl Runner Jobs

These jobs test the managed Flow runtime shape where Nomad schedules one
allocation and `flowctl run` manages the pipeline components inside that
allocation.

## Local Raw Exec Test

`flowctl-runner-local.nomad` is a local development job. It uses the Nomad
`raw_exec` driver and local binaries:

- `/home/tillman/Documents/flowctl/bin/flowctl`
- `/home/tillman/Documents/obsrvr-stellar-components/bin/raw-ledger-source`
- `/home/tillman/Documents/obsrvr-stellar-components/bin/stellar-ledger-processor`
- `/home/tillman/Documents/obsrvr-stellar-components/bin/ducklake-sink`

Your local Nomad client must allow `raw_exec`. If it is disabled, use this job
as the reference shape and move to the production runner image/bundle described
below.

Run it with:

```bash
nomad job run deploy/nomad/flowctl-runner-local.nomad
```

Override the tested ledger range with:

```bash
nomad job run \
  -var start_ledger=62080000 \
  -var end_ledger=62080000 \
  deploy/nomad/flowctl-runner-local.nomad
```

Inspect it with:

```bash
nomad job status obsrvr-flowctl-runner-local
nomad alloc logs -stderr <alloc-id> flowctl-run
nomad alloc logs <alloc-id> flowctl-run
```

Stop it with:

```bash
nomad job stop -purge obsrvr-flowctl-runner-local
```

## What This Tests

The job validates the first managed runtime shape:

```text
Nomad allocation
  -> flowctl run
  -> raw-ledger-source
  -> stellar-ledger-processor
  -> ducklake-sink
```

The flowctl embedded control plane is exposed as a Nomad service named
`flowctl-runner-local`. Component health ports are also registered as Nomad
services so the console can discover the runtime and component endpoints.

## Production Direction

This local job intentionally uses `raw_exec` and host paths. Production should
replace that with a Nix/flake-built runner bundle or Docker image containing:

- `flowctl`
- allowlisted component binaries
- generated flowctl pipeline YAML
- secret references mounted from Vault/Nomad templates

The production job should keep the same runtime model: one scheduled allocation
running `flowctl run`, with flowctl supervising component processes inside the
allocation.

## Runner Image

`Dockerfile.flowctl-runner` builds a Docker image for the production-style
Nomad job. It expects prebuilt binaries under `bin/`, including `bin/flowctl`.

Build component binaries:

```bash
make build
```

Copy the external source and flowctl binaries into this repo's `bin/`
directory:

```bash
cp /home/tillman/Documents/stellar-raw-ledger-origin/result/bin/raw-ledger-origin bin/raw-ledger-source
cp /home/tillman/Documents/flowctl/bin/flowctl bin/flowctl
```

Build the runner image:

```bash
make docker-flowctl-runner
```

Push it under the tag used by the infra Nomad job:

```bash
docker push withobsrvr/obsrvr-flowctl-runner:latest
```
