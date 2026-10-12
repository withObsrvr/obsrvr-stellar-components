# obsrvr-stellar-components modernization plan

**Role:** Canonical Stellar interpretation, normalized batches, sinks, DuckLake
ingest, maintenance, replication, and historical backfill  
**Local checkout:** `/home/tillman/Documents/obsrvr-stellar-components`  
**Source dependency:** `/home/tillman/Documents/stellar-raw-ledger-origin`

## Objective

Make the processor-to-durable-commit path diagnosable and safely resumable. An
operator must be able to distinguish source failure, processor failure, sink
failure, and storage failure from component status without restarting services.

This repository does not own raw-ledger acquisition. It consumes
`stellar.ledger.v1` from `raw-ledger-source`.

## Canonical topology

```text
raw-ledger-source
  -> stellar-ledger-processor
  -> ducklake-sink (ingest-rpc)
  -> quack-ducklake-server
  -> primary DuckLake + ingest_watermarks
```

Historical high-throughput backfill remains a separate
`ducklake-backfill-worker` path.

## Priority 1: correlated progress

Expose and preserve:

- pipeline/run/component/stream IDs;
- last received raw ledger;
- last transformed ledger;
- last batch sent by the sink;
- last batch acknowledged;
- last durably committed ledger;
- last successful commit time;
- current retry and terminal error class.

The source owns fetched/emitted claims. The processor owns received/transformed
claims. The sink owns sent/acknowledged claims. The server owns committed
claims.

## Priority 2: terminal-error behavior

Upgrade to the released error-aware `flowctl-sdk` only after the SDK and
flowctl runtime PRs land.

- processor errors must return through the runtime;
- sink write/ack failure must fail the component after bounded retries;
- no handler may turn invalid input into an empty successful batch;
- no sink may advance a watermark before durable commit;
- cancellation must remain distinct from failure.

Keep ledger-scoped idempotent replacement and the existing
`ingest_watermarks` transaction boundary.

## Priority 3: health and metrics

Standardize:

- `/live`: process responsiveness;
- `/ready`: dependencies and ability to accept work;
- `/health`: diagnostic state;
- `/metrics`: bounded Prometheus labels.

Add alerts for receive/commit divergence, stale commit age, repeated retry,
terminal component error, and pipeline ledger lag.

## Priority 4: dependency alignment

Use dedicated PRs to align:

- `flowctl-sdk` after its runtime release;
- `flow-proto` only when required;
- `go-stellar-sdk` from `v0.7.1` to the agreed supported release;
- `stellar-raw-ledger-origin/source` from its pseudo-version to an immutable
  tagged release.

Do not mix dependency alignment with storage schema or commit-semantics changes.

## Priority 5: live-tip production gate

Current evidence is primarily archive-backed. Prove an RPC/tip-capable pipeline:

1. bounded recent range parity;
2. transition to unbounded/tip behavior;
3. ledger-close-to-committed and ledger-close-to-queryable latency;
4. source disconnect/reconnect from last durable watermark;
5. zero gaps and convergent replay;
6. retained logs, metrics, image digests, and catalog queries.

## Pull-request sequence

1. Add correlation/progress model and processor metrics.
2. Add sink/server received-versus-committed metrics.
3. Adopt released SDK error propagation.
4. Add cross-component failure-injection E2E.
5. Align dependencies in isolated PRs.
6. Run and retain live-tip production-gate evidence.
7. Update Nomad jobs, alerts, dashboards, and runbook.

## Required acceptance test

For a bounded archive range:

1. inject source failure after ledger N;
2. show no downstream component claims N+1;
3. inject sink failure after receiving N but before commit;
4. show received N and committed N-1;
5. restart from the durable watermark;
6. verify exact requested min/max and zero gaps;
7. verify replay produces no divergent duplicates.

Use the documented `ingest_watermarks` gap query as the durable proof.

## Verification

```bash
cd /home/tillman/Documents/obsrvr-stellar-components
go test ./...
go vet ./...
scripts/validate-pipelines.sh
```

Run relevant chaos, crash-recovery, checkpoint, and telemetry gates for changes
touching DuckLake commit behavior.

## Non-goals

- implementing raw source backends here;
- giving the raw source ownership of sink checkpoints;
- routing live history through the file-backfill path;
- weakening one-network-per-catalog enforcement;
- migrating every stale `ttp-processor-demo` directory.

## Definition of done

For any stalled pipeline, operators can identify the last received,
transformed, acknowledged, and committed ledger and the failing component. A
restart resumes from durable state and the final catalog proves zero gaps.
