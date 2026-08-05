.PHONY: build lint proto test test-local-pipeline test-quack-chaos test-ingest-chaos test-telemetry-gate test-manual-checkpoint-gate test-checkpoint-gate test-crash-recovery-gate test-kill-checkpoint-gate test-checkpoint-failure-gate test-checkpoint-controller-gate test-ingest-replay-smoke test-cadence-gate test-file-backfill-benchmark validate-pipelines validate-nomad docker-flowctl-runner tidy clean

GO ?= go
GOFMT ?= gofmt
PROTOC ?= protoc
CGO_ENABLED ?= 1

COMPONENTS := stellar-ledger-processor jsonl-sink postgres-sink ducklake-sink quack-ducklake-server index-materializer ducklake-replica-sync ducklake-maintenance ducklake-gatekeeper
TOOLS := ledger-smoke ledger-fixture-recorder ingest-replay ducklake-backfill-worker

build:
	@mkdir -p bin
	@for component in $(COMPONENTS); do \
		echo "building $$component"; \
		CGO_ENABLED=$(CGO_ENABLED) $(GO) build -o bin/$$component ./components/$$component/cmd/component; \
	done
	@for tool in $(TOOLS); do \
		echo "building $$tool"; \
		CGO_ENABLED=$(CGO_ENABLED) $(GO) build -o bin/$$tool ./cmd/$$tool; \
	done

lint:
	@$(GOFMT) -w $$(find . -name '*.go' -not -path './gen/go/*' -not -path './vendor/*')
	@$(GO) vet ./...

proto:
	@scripts/generate-proto.sh

test:
	@$(GO) test ./...

test-local-pipeline:
	@scripts/test-local-pipeline.sh

test-quack-chaos:
	@scripts/quack-chaos-harness.sh

test-ingest-chaos:
	@QUACK_CHAOS_SINK_MODE=ingest-rpc scripts/quack-chaos-harness.sh

test-telemetry-gate:
	@scripts/ducklake-telemetry-gate.sh

test-manual-checkpoint-gate:
	@TELEMETRY_GATE_MANUAL_CHECKPOINT=true \
		TELEMETRY_GATE_RUNTIME_DIR=/tmp/obsrvr-ducklake-manual-checkpoint-gate \
		scripts/ducklake-telemetry-gate.sh

test-checkpoint-gate:
	@scripts/ducklake-checkpoint-gate.sh

test-crash-recovery-gate:
	@scripts/ducklake-crash-recovery-gate.sh

test-kill-checkpoint-gate:
	@scripts/ducklake-kill-checkpoint-gate.sh

test-checkpoint-failure-gate:
	@scripts/ducklake-checkpoint-failure-gate.sh

test-checkpoint-controller-gate:
	@scripts/ducklake-controller-gate.sh

test-ingest-replay-smoke:
	@$(GO) test ./internal/ledgerfixture ./cmd/ledger-fixture-recorder ./cmd/ingest-replay

test-cadence-gate:
	@scripts/ducklake-cadence-gate.sh

test-file-backfill-benchmark:
	@scripts/ducklake-file-backfill-benchmark.sh

validate-pipelines:
	@scripts/validate-pipelines.sh

validate-nomad:
	@nomad fmt -check deploy/nomad/quack-ducklake-server.nomad
	@nomad job validate deploy/nomad/quack-ducklake-server.nomad
	@nomad fmt -check deploy/nomad/ducklake-maintenance.nomad
	@nomad job validate deploy/nomad/ducklake-maintenance.nomad

docker-flowctl-runner:
	@for bin in flowctl raw-ledger-source stellar-ledger-processor ducklake-sink postgres-sink jsonl-sink index-materializer; do \
		test -x bin/$$bin || (echo "missing bin/$$bin; build or copy it into bin/$$bin first" && exit 1); \
	done
	@docker build -f Dockerfile.flowctl-runner -t withobsrvr/obsrvr-flowctl-runner:latest .

tidy:
	@$(GO) mod tidy

clean:
	@rm -rf bin dist coverage.out
