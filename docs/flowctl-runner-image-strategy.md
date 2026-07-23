# Flowctl Runner Image Strategy

This document describes how `obsrvr-console` should translate user-selected
pipeline components into deployable `flowctl` runtime images.

The goal is to let users build flexible pipelines from the UI without building a
custom Docker image for every pipeline.

## Core Recommendation

Use prebuilt runner image families.

The console should not build a new image when a user creates or edits a
pipeline. Instead:

```text
user selects components
  -> console validates the component graph
  -> console finds the smallest approved runner image containing those components
  -> console renders flowctl YAML using registry-owned binary paths
  -> console dispatches the Nomad job with that runner image
```

The selected components change the rendered `flowctl` pipeline. They do not
change the container at request time.

## Why Not Build Per Pipeline

Building a custom container for each user pipeline creates avoidable operational
cost:

- pipeline deployment becomes dependent on Docker build latency;
- image build failures become user-facing deployment failures;
- registry push/pull behavior enters the product request path;
- rollback becomes harder because image artifacts are tied to individual edits;
- build cache misses become runtime incidents;
- secrets can accidentally leak into image layers if the process is wrong;
- accepting arbitrary images or commands from users becomes a security boundary.

The console should accept component IDs and configuration, not arbitrary
commands or image references.

## Why Not One Image Per Component

One image per component is attractive for isolation, but it does not match the
current runtime shape.

Today, `flowctl run --orchestrator process` starts local child processes inside
one runner allocation. That means the component binaries must be present inside
the runner image.

One image per component becomes a better fit later if flowctl has a mature
container-per-component orchestrator where each source, processor, and sink can
run as its own Nomad task or container.

Until then, runner images should contain compatible sets of binaries.

## Why Not One Huge Image Forever

A single image containing every component is useful for early development, but
it does not scale well:

- every component upgrade forces retesting the largest possible runtime;
- heavy dependencies are included for pipelines that do not need them;
- image size and pull time grow over time;
- unrelated component families become coupled;
- customer-specific or experimental components become harder to isolate.

Use a full image for development and migration, but do not make it the only
production artifact indefinitely.

## Runner Image Families

Runner images should be grouped by runtime family, dependency profile, and
compatibility set.

Initial recommendation:

```text
stellar-lake-runner
stellar-full-runner
```

Later split into:

```text
stellar-core-runner
stellar-lake-runner
stellar-streaming-runner
stellar-warehouse-runner
stellar-full-runner
```

### stellar-core-runner

Minimal Stellar processing and debug output.

Expected contents:

- `flowctl`
- `raw-ledger-source`
- `stellar-ledger-processor`
- `jsonl-sink`
- stdout/debug sinks

Use for:

- smoke tests
- local validation
- debug pipelines
- low-dependency deployments

### stellar-lake-runner

Lakehouse and DuckLake-oriented pipelines.

Expected contents:

- `flowctl`
- `raw-ledger-source`
- `stellar-ledger-processor`
- `ducklake-sink`
- `postgres-sink`
- `index-materializer`
- `quack-ducklake-server`

Use for:

- managed Flow lake deployments
- DuckLake bronze writes
- PostgreSQL sink deployments
- derived index materialization
- query-serving backends built on DuckLake

This should be the first production runner family for the current
`obsrvr-stellar-components` path.

### stellar-streaming-runner

Streaming and notification outputs.

Expected contents:

- `flowctl`
- `raw-ledger-source`
- `stellar-ledger-processor`
- Kafka or Pub/Sub sinks
- webhook sinks
- websocket sinks
- ZeroMQ sinks
- notification dispatcher components

Use for:

- event delivery
- customer webhook pipelines
- Pub/Sub fanout
- real-time streaming integrations

### stellar-warehouse-runner

Database, warehouse, and object storage outputs.

Expected contents:

- `flowctl`
- `raw-ledger-source`
- `stellar-ledger-processor`
- Postgres sink
- ClickHouse sink
- MongoDB sink
- Parquet sink
- S3 or GCS sinks
- TimescaleDB sink

Use for:

- customer-owned storage destinations
- warehouse ingestion
- analytics exports
- long-running backfills

### stellar-full-runner

Development and migration image containing all approved components.

Use for:

- local development
- CI smoke tests
- migration while porting from `cdp-pipeline-workflow`
- internal experiments

Avoid using this as the default production image once the component surface
grows.

## Component Registry Contract

The console should store component metadata in a registry. The UI should render
choices from this registry, and the renderer should use it to validate and
deploy pipelines.

Each component should declare:

```json
{
  "id": "ducklake-sink",
  "version": "0.1.0",
  "kind": "sink",
  "runtime": "flowctl",
  "binary_path": "/app/bin/ducklake-sink",
  "input_event_types": ["stellar.ledger.batch.v1"],
  "output_event_types": [],
  "config_schema": {},
  "secret_schema": {},
  "supported_networks": ["mainnet", "testnet"],
  "runner_families": ["stellar-lake-runner", "stellar-full-runner"],
  "dependencies": ["duckdb", "libstdc++"]
}
```

Important fields:

- `id`: stable component identifier selected by the UI.
- `version`: component package version.
- `kind`: source, processor, sink, service, or job.
- `binary_path`: path inside approved runner images.
- `input_event_types`: event types this component accepts.
- `output_event_types`: event types this component emits.
- `config_schema`: non-secret user configuration schema.
- `secret_schema`: required secret fields stored through Vault.
- `runner_families`: compatible runner image families.

The user never submits `binary_path`. The backend resolves it from the registry.

## Runner Image Registry Contract

Runner images should also be registry objects.

Each runner image should declare:

```json
{
  "family": "stellar-lake-runner",
  "image": "withobsrvr/stellar-lake-runner:v2026.07.05",
  "digest": "sha256:...",
  "status": "active",
  "runtime": "flowctl",
  "components": [
    "raw-ledger-source@0.2.2",
    "stellar-ledger-processor@0.1.0",
    "ducklake-sink@0.1.0",
    "postgres-sink@0.1.0",
    "index-materializer@0.1.0",
    "quack-ducklake-server@0.1.0"
  ],
  "supported_networks": ["mainnet", "testnet"]
}
```

Store the resolved image digest on every pipeline run. Tags are convenient for
humans, but runs should be auditable by immutable digest.

## Image Resolution

Given a published pipeline definition, the console should resolve the image with
this process:

1. Read selected component IDs and versions.
2. Validate each component exists and is active.
3. Validate source, processor, and sink event-type compatibility.
4. Validate network compatibility.
5. Validate config and secret schemas.
6. Find active runner images containing all required components.
7. Prefer the smallest or most specific runner family.
8. Store the selected image tag and digest on the run.
9. Render flowctl YAML with registry-owned binary paths.
10. Dispatch the Nomad job.

Example:

```text
selected:
  raw-ledger-source@0.2.2
  stellar-ledger-processor@0.1.0
  ducklake-sink@0.1.0
  index-materializer@0.1.0

resolved:
  withobsrvr/stellar-lake-runner:v2026.07.05
```

Another example:

```text
selected:
  raw-ledger-source@0.2.2
  stellar-ledger-processor@0.1.0
  google-pubsub-sink@0.1.0
  webhook-sink@0.1.0

resolved:
  withobsrvr/stellar-streaming-runner:v2026.07.05
```

If no compatible runner image exists, deployment should fail before Nomad
dispatch with a clear validation error:

```text
No active runner image contains:
  google-pubsub-sink@0.1.0
  ducklake-sink@0.1.0

Available choices:
  stellar-lake-runner contains ducklake-sink
  stellar-streaming-runner contains google-pubsub-sink
```

In that case either create a new runner family, add the component to an existing
family, or mark that combination unsupported.

## Flowctl YAML Rendering

The renderer should convert component selections into flowctl YAML.

The UI submits a high-level definition:

```json
{
  "source": "raw-ledger-source@0.2.2",
  "processors": ["stellar-ledger-processor@0.1.0"],
  "sinks": ["ducklake-sink@0.1.0"],
  "config": {
    "network": "mainnet",
    "start_ledger": 62080000,
    "end_ledger": 62080000
  }
}
```

The backend renders:

```yaml
sources:
  - id: raw-ledger-source
    command: ["/app/bin/raw-ledger-source"]
    env:
      BACKEND_TYPE: "ARCHIVE"
      START_LEDGER: "62080000"
      END_LEDGER: "62080000"

processors:
  - id: stellar-ledger-processor
    command: ["/app/bin/stellar-ledger-processor"]
    inputs: ["raw-ledger-source"]

sinks:
  - id: ducklake-sink
    command: ["/app/bin/ducklake-sink"]
    inputs: ["stellar-ledger-processor"]
```

The command paths come from the registry, not from user input.

## Porting from cdp-pipeline-workflow

The `cdp-pipeline-workflow` repository contains many processor and consumer
implementations. As they move into `obsrvr-stellar-components`, do not create a
new runner image for each one.

Group them by runtime family.

Suggested initial mapping:

| CDP Surface | Target Family |
| --- | --- |
| raw ledger sources, RPC/archive/GCS/S3 adapters | core, lake, full |
| normalization and ledger processors | core, lake, streaming, warehouse, full |
| DuckDB and DuckLake consumers | lake, full |
| PostgreSQL consumers | lake, warehouse, full |
| Parquet, S3, GCS consumers | warehouse, full |
| Kafka, Pub/Sub, ZeroMQ, websocket consumers | streaming, full |
| webhook and notification consumers | streaming, full |
| Redis latest-ledger and cache consumers | streaming or warehouse, full |
| ClickHouse, MongoDB, TimescaleDB consumers | warehouse, full |
| contract event and invocation extractors | lake, streaming, warehouse, full |
| index/materializer jobs | lake, full |
| experimental or customer-specific components | full first, then dedicated family if promoted |

Porting order should favor reusable contracts first:

1. Shared event contracts.
2. Shared decode and normalize packages.
3. Source components.
4. Core ledger processor.
5. Lake and database sinks.
6. Streaming sinks.
7. Derived materializers and index jobs.
8. Customer-specific processors.

## Versioning and Compatibility

Every pipeline run should pin:

- pipeline definition version;
- component IDs and versions;
- runner image tag;
- runner image digest;
- flowctl version;
- source version, especially `raw-ledger-source`;
- network and protocol assumptions.

This is what makes protocol upgrades manageable. A user can edit a pipeline,
publish a new definition version, and restart into a new runner image without
mutating the history of the prior run.

## Operational Policy

Recommended policies:

- Use `stellar-full-runner` for development and migration only.
- Use `stellar-lake-runner` as the first production managed Flow image.
- Add new components to `stellar-full-runner` first.
- Promote components into a production family after smoke tests pass.
- Keep production runner images immutable by digest.
- Do not delete old image tags until no active run references their digest.
- Store the rendered flowctl YAML for each run.
- Store secret-free rendered YAML in Postgres and secret values in Vault.

## Initial Implementation Plan

1. Add runner image metadata to the console registry.
2. Add component metadata for current Stellar components.
3. Implement image resolution in the flowctl runtime backend.
4. Render flowctl YAML from registry metadata.
5. Dispatch the Nomad job with the resolved image.
6. Store resolved image tag and digest on the run/session model.
7. Add a validation page that shows why a selected component combination is or
   is not deployable.
8. Start with two images:

```text
withobsrvr/stellar-lake-runner:v2026.07.05
withobsrvr/stellar-full-runner:v2026.07.05
```

9. Split into additional families when image size, dependency conflicts, or
   product packaging require it.

## Decision

Use many runner images eventually, but only a small number of runner image
families.

Do not build an image per user pipeline.

Do not create an image per component while the runtime uses the process
orchestrator.

Start with:

```text
stellar-lake-runner
stellar-full-runner
```

Then split into:

```text
stellar-core-runner
stellar-lake-runner
stellar-streaming-runner
stellar-warehouse-runner
stellar-full-runner
```

as the component surface grows.
