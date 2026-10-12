# Dependency alignment (working tree)

The requested dependency upgrades were applied without changing the ingest schema, storage transaction boundary, or flow-proto version. When preparing PRs, split the three module changes (with their corresponding `go.sum` and `vendor/` deltas) into separate PRs and re-run gates after each merge.

| Module | Before | After | Notes |
| --- | --- | --- | --- |
| `github.com/withObsrvr/flowctl-sdk` | `v0.1.3` | `v0.1.4` | `v0.1.4` modifies SDK **source** stream/error handling only; consumer/processor error propagation remains unavailable. Do not remove the sink exit-on-error fallback. |
| `github.com/stellar/go-stellar-sdk` | `v0.7.1` | `v0.7.3` | Brings `github.com/stellar/go-xdr` to `v0.0.0-20260806060815-dc590f17552a`. |
| `github.com/withObsrvr/stellar-raw-ledger-origin/source` | `v0.0.0-20260805204803-a92ba11cc452` | `v0.2.5` | Immutable tagged submodule release. |

`flow-proto` remains `v0.1.3`. Go modules and vendor were refreshed with `GOFLAGS=-mod=mod go mod tidy && go mod vendor`.

Local verification: `go test ./...`, `go vet ./...`, `make build`, and `scripts/validate-pipelines.sh` pass. A pipeline run still needs an external `bin/raw-ledger-source` and archive connectivity; no cross-component chaos or live-tip production-gate evidence is claimed by the dependency-only checks.
