#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

protoc \
  -I proto \
  --go_out=. \
  --go_opt=module=github.com/withObsrvr/obsrvr-stellar-components \
  --go-grpc_out=. \
  --go-grpc_opt=module=github.com/withObsrvr/obsrvr-stellar-components \
  proto/stellar/components/v1/ledger_batch.proto \
  proto/stellar/components/v1/ingest_service.proto
