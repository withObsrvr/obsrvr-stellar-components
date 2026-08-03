#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 3 || "$#" -gt 5 ]]; then
  echo "usage: $0 CATALOG_PATH DATA_PATH OUTPUT_MANIFEST [MIN_LEDGER MAX_LEDGER]" >&2
  exit 2
fi

catalog_path="$(realpath -m "$1")"
data_path="$(realpath -m "$2")"
output_manifest="$(realpath -m "$3")"
min_ledger="${4:-}"
max_ledger="${5:-}"

command -v duckdb >/dev/null 2>&1 || { echo "duckdb CLI is required" >&2; exit 2; }
command -v sha256sum >/dev/null 2>&1 || { echo "sha256sum is required" >&2; exit 2; }
[[ -f "$catalog_path" ]] || { echo "catalog does not exist: $catalog_path" >&2; exit 2; }
[[ -d "$data_path" ]] || { echo "data path does not exist: $data_path" >&2; exit 2; }
if [[ -n "$min_ledger" || -n "$max_ledger" ]]; then
  [[ "$min_ledger" =~ ^[0-9]+$ && "$max_ledger" =~ ^[0-9]+$ ]] || {
    echo "MIN_LEDGER and MAX_LEDGER must both be non-negative integers" >&2
    exit 2
  }
  (( min_ledger <= max_ledger )) || { echo "MIN_LEDGER must be <= MAX_LEDGER" >&2; exit 2; }
fi
for path in "$catalog_path" "$data_path" "$output_manifest"; do
  [[ "$path" != *"'"* ]] || { echo "paths containing single quotes are unsupported" >&2; exit 2; }
done

work_dir="${output_manifest}.rows"
rm -rf "$work_dir"
mkdir -p "$work_dir" "$(dirname "$output_manifest")"
metadata="$work_dir/tables.txt"
statements="$work_dir/export.sql"
counts_sql="$work_dir/counts.sql"
counts="$work_dir/counts.csv"

attach_sql="INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:$catalog_path' AS stellar_lake (DATA_PATH '$data_path');"
duckdb :memory: -noheader -list -separator '|' -c "$attach_sql
  SELECT table_schema,
         table_name,
         string_agg(chr(34) || replace(column_name, chr(34), chr(34) || chr(34)) || chr(34), ',' ORDER BY ordinal_position),
         CASE
           WHEN bool_or(column_name = 'ledger_sequence') THEN 'ledger_sequence'
           WHEN table_schema = 'bronze' AND table_name = 'ledgers_row_v2' AND bool_or(column_name = 'sequence') THEN 'sequence'
           ELSE ''
         END AS ledger_column
  FROM information_schema.columns
  WHERE table_catalog = 'stellar_lake'
    AND data_type NOT LIKE 'TIMESTAMP%'
  GROUP BY table_schema, table_name
  ORDER BY table_schema, table_name;" >"$metadata"

printf '%s\n' "$attach_sql" >"$statements"
: >"$counts_sql"
first_count=true
while IFS='|' read -r schema table columns ledger_column; do
  [[ -n "$schema" && -n "$table" && -n "$columns" ]] || continue
  safe_name="${schema}__${table}"
  row_file="$work_dir/$safe_name.csv"
  where_clause=""
  if [[ -n "$ledger_column" && -n "$min_ledger" ]]; then
    where_clause=" WHERE \"$ledger_column\" BETWEEN $min_ledger AND $max_ledger"
  fi
  printf "COPY (SELECT %s FROM stellar_lake.\"%s\".\"%s\"%s ORDER BY ALL) TO '%s' (FORMAT CSV, HEADER false, NULL '\\\\N');\n" \
    "$columns" "$schema" "$table" "$where_clause" "$row_file" >>"$statements"
  if [[ "$first_count" == true ]]; then
    first_count=false
  else
    printf ' UNION ALL ' >>"$counts_sql"
  fi
  printf "SELECT '%s.%s' AS table_name, count(*) AS row_count FROM stellar_lake.\"%s\".\"%s\"%s" \
    "$schema" "$table" "$schema" "$table" "$where_clause" >>"$counts_sql"
done <"$metadata"

[[ "$first_count" == false ]] || { echo "catalog contained no logical tables" >&2; exit 1; }
printf ";\nCOPY (%s) TO '%s' (FORMAT CSV, HEADER false);\n" "$(cat "$counts_sql")" "$counts" >>"$statements"
duckdb :memory: <"$statements" >/dev/null

: >"$output_manifest"
while IFS=',' read -r table_name row_count; do
  safe_name="${table_name//./__}"
  row_file="$work_dir/$safe_name.csv"
  [[ -f "$row_file" ]] || { echo "missing exported rows for $table_name" >&2; exit 1; }
  hash="$(sha256sum "$row_file" | awk '{print $1}')"
  printf '%s|%s|%s\n' "$table_name" "$row_count" "$hash" >>"$output_manifest"
done <"$counts"
sort -o "$output_manifest" "$output_manifest"
rm -rf "$work_dir"
cat "$output_manifest"
