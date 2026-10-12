# Remaining modernization gates: operator checklist

Status: repository-local progress and health instrumentation is implemented; **the end-to-end failure injection and live-tip production gate have not been demonstrated**. Do not equate validation or synthetic RPC tests with production approval.

## Before a production gate

- Deploy immutable, recorded image digests; pin one catalog/network and one ingest stream. Do not use the file-backfill worker for tip traffic.
- Set `PROCESSOR_METRICS_ADDR` on a separate scrape port from SDK `HEALTH_PORT`. The local Nomad runner template includes one example. Scrape sink `/metrics` and primary server `/metrics` separately. Use bounded deployment labels (pipeline and component), not per-ledger labels.
- Check `/live` for HTTP availability and `/ready` for ability to accept work. A sink `/ready` TCP probe does not authenticate; a successful `/ready` does not mean a ledger committed. Use `/health` and `/metrics` for diagnostics.
- Record requested range, network, source backend, pipeline/run IDs, build commits, image digests, configuration, logs, gauge snapshots, and catalog queries in the same evidence directory. Avoid secrets in logs.

## Bounded archive failure injection (required, not yet run)

1. Choose a short range `[MIN, MAX]` and N strictly inside it. Stop the source immediately after emitting N; retain evidence that the processor has not transformed N+1, the sink has not sent N+1, and the server has not committed N+1. An empty downstream metric alone is insufficient if the component was never scraped.
2. Resume. After the server receives a later ledger, interrupt it **before commit** using a controlled hook or transaction fault. Prove `last_received_ledger=N` and `last_committed_ledger=N-1` from the server scrape (and the absence of N in the catalog). Do not use an ack mismatch as evidence of a pre-commit fault: commit can succeed even when the ack is lost.
3. Restart from the catalog's last durable watermark, not the sink's sent/ack gauges. Replay and compare the final catalog to a never-failed baseline using `EXCEPT ALL`; check exactly `MIN`, `MAX`, count `MAX-MIN+1`, and zero rows from the `ingest_watermarks` gap query in `components/ducklake-sink/README.md`.
4. Run `make test-ingest-chaos` as an additional kill/replay/parity regression (needs a built `bin/raw-ledger-source`, `flowctl`, `duckdb`, and archive access); its server kill is **not** a substitute for the precise pre-commit injection above. For commit/checkpoint changes also run the crash-recovery, checkpoint-failure, and telemetry gates listed in `docs/production-gate-runbook.md`.

## Live-tip gate (not yet run)

Run bounded recent-range parity first, then unbounded tip. Capture ledger-close-to-committed and ledger-close-to-queryable distributions, disconnect/reconnect from durable state, zero gaps and convergent replay, query results, source-tip/lag measurements, and all image/log/metric evidence. Archive-only runs cannot establish tip or reconnect behavior.

## Remaining external dependencies

`flowctl-sdk` is now pinned at `v0.1.4`, but that release changes only the SDK source-stream contract; it does **not** make the processor/consumer handlers error-aware. Do not remove the sink's non-cancellation process-exit safeguard or claim SDK-level error propagation until a later released runtime and compatible flowctl runtime pass the failure-injection gate. `go-stellar-sdk` is pinned at `v0.7.3` and the source module at tagged `v0.2.5`. When publishing, split these dependency alignments into separate PRs; this working tree includes all three upgrades to satisfy the requested migration. Wire the server alerts in `deploy/monitoring/quack-ducklake-alerts.yml` into the target monitoring pack and test them there. Pipeline lag requires an authoritative source tip metric; neither a server commit gauge nor an archive end ledger is a live tip.
