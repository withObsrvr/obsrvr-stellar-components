#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

pipelines=(pipelines/*.yaml)
for pipeline in "${pipelines[@]}"; do
  echo "validating $pipeline"
  flowctl validate "$pipeline"
done
