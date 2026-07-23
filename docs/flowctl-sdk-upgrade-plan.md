# flowctl-sdk Upgrade Plan

Date: 2026-07-02

Current dependency:

```text
github.com/withObsrvr/flowctl-sdk v0.1.2
```

The production-hardening cycles added component-side guards first. The SDK
upgrade should move root delivery/runtime behavior into `flowctl-sdk` so new
components inherit it without reimplementing the same protections.

## Target Release

Target: `flowctl-sdk v0.1.3`

Required changes:

1. Consumer handler errors must be propagated according to an explicit policy:
   retry with bounded backoff, crash the component, or return a nack once the
   control plane supports it.
2. Consumer dispatch must have a bounded worker pool or serial mode with
   backpressure. Unbounded goroutine-per-event dispatch should not be the
   default.
3. Health must reflect runtime state. `HealthCheck` should report unhealthy
   after the last handler error, stale successful write, failed registration,
   or failed dependency check.
4. Registration and heartbeat failures should retry with bounded backoff
   instead of calling `log.Fatalf` from background goroutines.
5. Runtime config parsing should fail on malformed ports and booleans instead
   of silently coercing bad input.

## Compatibility Contract

The first SDK release should be opt-in for behavior that could break existing
components:

- add options such as `ErrorPolicy`, `MaxConcurrentEvents`,
  `RetryBackoff`, and `HealthReporter`
- default existing components to current behavior until each component opts in
- make strict config parsing default for new constructors only, then migrate
  older helpers

`obsrvr-stellar-components` should opt in immediately for production sinks and
processors after the SDK release is tagged.

## Migration Steps

1. Patch `withObsrvr/flowctl-sdk`.
2. Add SDK-level tests for handler error propagation, bounded concurrency,
   registration retry, health state, and bad config parsing.
3. Tag `v0.1.3`.
4. In this repo:

   ```bash
   go get github.com/withObsrvr/flowctl-sdk@v0.1.3
   go mod vendor
   go test ./...
   make validate-pipelines
   make build
   ```

5. Opt in `ducklake-sink`, `postgres-sink`, `jsonl-sink`, and
   `stellar-ledger-processor` to the new SDK options.
6. Keep component-side crash-on-write-failure until the SDK policy is proven in
   the Quack chaos harness.
7. Remove duplicate component-side guards only after the harness passes with
   SDK-level retries/backpressure enabled.

## Acceptance

- A sink write failure cannot be logged and dropped by the SDK.
- A bounded ledger range preserves event order or applies explicit backpressure.
- A failed sink reports unhealthy through the SDK health surface.
- A transient control-plane registration failure does not kill an otherwise
  healthy component without retries.
- `go test ./...`, `make validate-pipelines`, `make build`, and
  `make test-quack-chaos` pass after the SDK bump.
