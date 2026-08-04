# flowctl-sdk Upgrade Plan

Date: 2026-07-02

Current dependency:

```text
github.com/withObsrvr/flowctl-sdk v0.1.3
```

`v0.1.3` aligns the SDK and standalone examples with `go-stellar-sdk v0.7.1`.
The exported `pkg/` runtime is unchanged from `v0.1.2`; this release does not
contain the delivery, backpressure, registration, or health behavior described
below.

The production-hardening cycles added component-side guards first. The SDK
upgrade should move root delivery/runtime behavior into `flowctl-sdk` so new
components inherit it without reimplementing the same protections.

## Target Release

Target: a future runtime release after `v0.1.3` (version not yet assigned)

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

1. Patch `withObsrvr/flowctl-sdk` after the dependency-only `v0.1.3` release.
2. Add SDK-level tests for handler error propagation, bounded concurrency,
   registration retry, health state, and bad config parsing.
3. Tag a new runtime release.
4. Upgrade this repository to that release, refresh `vendor/`, and run the full
   test, pipeline, build, and chaos gates.
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
