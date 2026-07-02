#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

for pipeline in pipelines/*.yaml; do
  echo "validating $pipeline"
  flowctl validate "$pipeline"
done
