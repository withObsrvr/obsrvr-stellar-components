.PHONY: build lint proto test test-local-pipeline tidy clean

GO ?= go
GOFMT ?= gofmt
PROTOC ?= protoc
CGO_ENABLED ?= 1

COMPONENTS := stellar-ledger-processor jsonl-sink postgres-sink ducklake-sink

build:
	@mkdir -p bin
	@for component in $(COMPONENTS); do \
		echo "building $$component"; \
		CGO_ENABLED=$(CGO_ENABLED) $(GO) build -o bin/$$component ./components/$$component/cmd/component; \
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

tidy:
	@$(GO) mod tidy

clean:
	@rm -rf bin dist coverage.out
