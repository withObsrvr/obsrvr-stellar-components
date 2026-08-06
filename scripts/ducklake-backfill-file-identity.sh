#!/usr/bin/env bash
# Print the normalized file-identity digest for one or more shard result
# manifests. The digest covers every produced file's table, Parquet SHA-256,
# byte size, row count, ledger range, and schema fingerprint. Attempt-local
# fields (URI, worker ID, timestamps, job/shard IDs) are excluded, so two runs
# that produced logically identical artifacts share one digest.
set -euo pipefail

if (($# == 0)); then
  echo "usage: ${0##*/} <shard-result.manifest.json>..." >&2
  exit 2
fi

for manifest in "$@"; do
  if [[ ! -f "$manifest" ]]; then
    echo "missing manifest: $manifest" >&2
    exit 2
  fi
done

jq -s -j '
  [ .[].files[]
    | {table, sha256, bytes, rows, min_ledger, max_ledger, parquet_schema_fingerprint}
  ]
  | sort_by([.table, .min_ledger, .sha256])
  | tojson
' "$@" | sha256sum | awk '{print $1}'
