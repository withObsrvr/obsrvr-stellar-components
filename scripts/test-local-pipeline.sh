#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

make proto
make test
make build

cat <<'MSG'
Local static verification passed.

Runtime process-driver verification requires flowctl plus raw-ledger-source@0.2.2
and is documented in docs/nomad-migration.md and pipelines/local-jsonl.yaml.
MSG
