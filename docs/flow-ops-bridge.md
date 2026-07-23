# FLOW Ops Bridge

The FLOW ops bridge is the application-facing control and telemetry service for
flowctl pipelines built from `obsrvr-stellar-components`.

It is separate from any Stream Deck or operator-input bridge. Hardware input can
drive the FLOW UI, but pipeline state should come from this service.

## Purpose

The bridge gives a dashboard one stable API for observing and operating:

- flowctl pipeline runs
- component registration and health
- source, processor, and sink progress
- Quack and DuckLake status
- index materialization freshness
- recent logs and operational events

The dashboard should not connect directly to every component. It should connect
to the ops bridge, and the ops bridge should collect state from flowctl,
component health endpoints, Quack, DuckLake, and log files.

## Runtime Shape

```text
raw-ledger-source
  -> stellar-ledger-processor
  -> ducklake-sink

index-materializer
  -> tx_hash_index

index-materializer
  -> contract_events_index

quack-ducklake-server
  -> owns DuckLake attachment

flow-ops-bridge
  -> flowctl control plane
  -> component health endpoints
  -> component logs
  -> Quack/DuckLake status queries
  -> index checkpoint/freshness state

FLOW dashboard
  -> flow-ops-bridge HTTP API
  -> flow-ops-bridge SSE/WebSocket event stream
```

## Boundaries

### Ops Bridge

Owns operational state and control.

- lists configured pipelines
- lists active runs
- reports component health
- reports latest ledger and row counts
- reports sink/index lag
- tails structured logs
- starts and stops allowed pipelines
- triggers bounded smoke runs
- triggers index materialization over explicit ranges

### FLOW UI

Owns presentation and local UI state.

- renders pipeline health
- renders run timelines
- renders log panels
- renders lake freshness and index freshness
- sends explicit operator commands to the bridge
- may also receive Stream Deck actions from a separate operator input bridge

### Operator Input Bridge

Owns hardware input only.

- translates Stream Deck, keyboard simulator, or macro events into UI actions
- does not read pipeline state
- does not own flowctl credentials
- does not start or stop processes directly

The operator input bridge may send actions such as `NAV.OPS` or `EXE.RUN` to the
FLOW UI. The UI should then call the ops bridge if that action requires an
operational command.

## Placement

`flow-ops-bridge` should be an internal service. It should not live inside
`stellar-query-api`.

- `stellar-query-api` should remain the user/query API over lake data.
- `obsrvr-gateway` should remain the public auth, routing, metering, and product
  boundary.
- `flow-ops-bridge` should own operational control and telemetry aggregation.

For external access, put `obsrvr-gateway` in front of `flow-ops-bridge` and
apply authz there. Internally, dashboards and operator tools can talk to the
bridge directly when running on a trusted network.

## State Sources

The bridge should collect state from these sources.

| Source | Data |
| --- | --- |
| flowctl control plane | pipeline definitions, run IDs, run status, component registration, endpoints, stream wiring |
| component health endpoints | liveness, readiness, current backend, latest processed ledger, local counters |
| component logs | startup events, warnings, send errors, sink commit logs, shutdown events |
| Quack | server availability, attached catalog, query errors, active clients if available |
| DuckLake | latest bronze ledger, row counts by kind, table coverage, snapshot/checkpoint state |
| index materializers | latest indexed ledger/range, index row counts, lag from bronze, last error |

The bridge should normalize these into one operational model so the UI does not
need to understand every component's native format.

## Resource Model

### Pipeline

```json
{
  "id": "local-archive-quack-ducklake",
  "name": "Local Archive -> Quack DuckLake",
  "status": "running",
  "run_id": "8820556d-8be4-47ff-ab67-a677a9b0e241",
  "source": "raw-ledger-source",
  "processors": ["stellar-ledger-processor"],
  "sinks": ["ducklake-sink"],
  "started_at": "2026-07-01T11:16:58Z",
  "updated_at": "2026-07-01T11:17:05Z"
}
```

### Component

```json
{
  "id": "ducklake-sink",
  "type": "sink",
  "status": "healthy",
  "endpoint": "127.0.0.1:55173",
  "health_endpoint": "127.0.0.1:19183",
  "input_event_types": ["stellar.ledger.batch.v1"],
  "output_event_types": [],
  "version": "0.1.0",
  "latest_ledger": 62080000,
  "events_in": 1,
  "events_out": 0,
  "rows_written": 9168,
  "errors": 0,
  "updated_at": "2026-07-01T11:17:05Z"
}
```

### Lake Status

```json
{
  "catalog": "ducklake",
  "access_mode": "quack",
  "quack_uri": "quack:127.0.0.1:9494",
  "status": "healthy",
  "latest_ledger": 62080000,
  "latest_snapshot_id": 42,
  "bronze_row_count": 9168,
  "row_counts_by_kind": {
    "transactions_row_v2": 354,
    "operations_row_v2": 723,
    "contract_events_stream_v1": 5165,
    "trades_row_v1": 32
  },
  "updated_at": "2026-07-01T11:17:05Z"
}
```

### Index Status

```json
{
  "name": "tx_hash_index",
  "status": "fresh",
  "source_table": "bronze.transactions_row_v2",
  "target_table": "index.tx_hash_index",
  "latest_indexed_ledger": 62080000,
  "bronze_latest_ledger": 62080000,
  "lag_ledgers": 0,
  "row_count": 354,
  "last_materialized_range": {
    "start_ledger": 62079999,
    "end_ledger": 62080000
  },
  "updated_at": "2026-07-01T11:17:06Z"
}
```

## HTTP API

The first API should be small and stable.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/ops/health` | bridge liveness and dependency summary |
| `GET` | `/api/ops/pipelines` | configured and active pipelines |
| `GET` | `/api/ops/pipelines/{id}` | one pipeline with current run and components |
| `GET` | `/api/ops/runs/{run_id}` | run status and timeline |
| `GET` | `/api/ops/components` | all known components |
| `GET` | `/api/ops/components/{id}` | one component's normalized state |
| `GET` | `/api/ops/components/{id}/logs` | recent component logs |
| `GET` | `/api/ops/lake/status` | Quack and DuckLake status |
| `GET` | `/api/ops/indexes` | all derived index freshness records |
| `POST` | `/api/ops/pipelines/{id}/start` | start an allowed pipeline |
| `POST` | `/api/ops/pipelines/{id}/stop` | stop an active pipeline |
| `POST` | `/api/ops/smoke/ledger` | run a bounded one-ledger smoke pipeline |
| `POST` | `/api/ops/indexes/{name}/materialize` | materialize an explicit ledger range |
| `GET` | `/api/ops/events` | SSE or WebSocket event stream |

Control endpoints should be allowlisted. The bridge should not execute arbitrary
shell commands from API input.

## Event Stream

The UI needs push updates for logs, status changes, and progress. Server-sent
events are enough for the first implementation. WebSockets are also acceptable
if the dashboard already uses them.

Events should have a stable envelope:

```json
{
  "type": "component.updated",
  "id": "evt_01",
  "time": "2026-07-01T11:17:05Z",
  "pipeline_id": "local-archive-quack-ducklake",
  "run_id": "8820556d-8be4-47ff-ab67-a677a9b0e241",
  "component_id": "ducklake-sink",
  "data": {
    "status": "healthy",
    "latest_ledger": 62080000,
    "rows_written": 9168
  }
}
```

Initial event types:

- `pipeline.started`
- `pipeline.stopped`
- `pipeline.failed`
- `run.updated`
- `component.registered`
- `component.updated`
- `component.unhealthy`
- `lake.updated`
- `index.updated`
- `log.appended`
- `command.accepted`
- `command.failed`

## Commands

Commands should be explicit, auditable, and bounded.

### Start Pipeline

```http
POST /api/ops/pipelines/local-archive-quack-ducklake/start
```

```json
{
  "ledger_range": {
    "start_ledger": 62080000,
    "end_ledger": 62080000
  },
  "environment": {
    "BACKEND_TYPE": "ARCHIVE"
  }
}
```

The bridge should merge request values with an allowlisted pipeline template. It
should reject unknown environment variables, unknown binaries, and unbounded
production operations unless explicitly configured.

### One-Ledger Smoke Run

```http
POST /api/ops/smoke/ledger
```

```json
{
  "network": "pubnet",
  "ledger_sequence": 62080000,
  "sink": "ducklake",
  "mode": "quack"
}
```

Expected result:

```json
{
  "status": "passed",
  "ledger_sequence": 62080000,
  "transaction_rows": 354,
  "operation_rows": 723,
  "bronze_rows": 9168,
  "sink_events_sent": 1
}
```

### Materialize Index

```http
POST /api/ops/indexes/tx_hash_index/materialize
```

```json
{
  "start_ledger": 62079999,
  "end_ledger": 62080000
}
```

Index materialization should run through the existing `index-materializer`
component or the same SQL implementation behind it. The bridge should track the
command as an operation, stream progress, and report final row counts.

## Security

The first local version can bind to loopback. Production should require:

- authenticated callers
- role-based authorization for control actions
- allowlisted pipeline templates
- allowlisted environment overrides
- audit log for every command
- redaction for credentials and object store paths
- request IDs propagated into component logs where practical

The bridge should be considered privileged because it can start and stop data
pipelines.

## Local Development

A local development setup should look like:

```text
flowctl run pipelines/local-archive-ducklake-flowctl.yaml
quack-ducklake-server
flow-ops-bridge
FLOW dashboard
```

The bridge can start without every dependency available. Missing dependencies
should appear as degraded status, not as process startup failure, unless a
required dependency is explicitly configured.

## First Milestone

The first useful implementation should avoid broad orchestration and focus on
read-only observability plus one bounded command.

1. expose `GET /api/ops/health`
2. expose `GET /api/ops/pipelines`
3. expose `GET /api/ops/components`
4. expose `GET /api/ops/lake/status`
5. expose `GET /api/ops/indexes`
6. expose `GET /api/ops/events`
7. support `POST /api/ops/smoke/ledger` for one bounded ledger

After that, add start/stop controls and index materialization commands.

## Relationship to the Operator Input Bridge

The operator input bridge document should describe how hardware or simulator
events become FLOW UI actions.

Example:

```text
Stream Deck key
  -> flow-operator-bridge
  -> FLOW action: NAV.OPS
  -> FLOW UI switches to ops view
  -> FLOW UI fetches /api/ops/pipelines from flow-ops-bridge
```

For a command:

```text
Stream Deck key
  -> flow-operator-bridge
  -> FLOW action: EXE.RUN
  -> FLOW UI validates selected command
  -> FLOW UI POSTs to flow-ops-bridge
  -> flow-ops-bridge starts bounded flowctl operation
```

This keeps hardware optional. Every operational action must remain available
from the software UI without a Stream Deck attached.
