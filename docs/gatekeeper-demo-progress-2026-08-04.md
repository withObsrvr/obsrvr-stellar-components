# Gatekeeper demo progress — 2026-08-04

This records the first implementation slice from `docs/pitch-gatekeeper-demo.md`.
It is evidence for the promotion boundary, not a claim that the full agent demo
is complete.

## Implemented

- `ducklake-gatekeeper` CLI and Quack client
- strict `gatekeeper.obsrvr.dev/v1alpha1` YAML proposal contract
- canonical proposal hashing
- source reads pinned with DuckLake `AT (VERSION => snapshot_id)`
- proposal-hash-scoped private staging schemas
- reproducibility comparison with `EXCEPT ALL` in both directions
- manifest-declared scalar reconciliation invariants
- deterministic proposal-hash-seeded replay range
- structurally confined read-only `SELECT`/`WITH` transformations
- replacement-key publication and `governance.promotions` receipt in one
  transaction
- JSON reports for both promoted and rejected proposals
- first `silver.asset_daily_volume` manifest

Rejected proposals never enter the publication transaction. Their staging
tables remain available for diagnosis and are replaced on a rerun of the same
proposal. Successful staging schemas are removed after publication.

## Real pubnet corpus run

Input was the previously recorded, hash-verified fixture corpus:

```text
ledger range:       62,080,000–62,080,999
ledger batches:     1,000
fixture size:       approximately 11 GiB
ingest mode:        saturated, one ledger in flight
ingest elapsed:     308.818 seconds
acknowledged:       1,000
watermark gaps:     0
token-transfer rows: 879,574
```

RPC acknowledgement latency during the saturated run was:

```text
median: 280.547 ms
p95:    354.111 ms
p99:    495.588 ms
max:    919.092 ms
```

This is backfill evidence. The batches were deliberately SLO-exempt and the
numbers must not be presented as a live-cadence `<400ms` result.

The Bronze catalog was pinned at snapshot `1008` for both proposal attempts.

### Authentic rejection

The first proposal used `sum(DOUBLE)` for daily volume. Parallel aggregation
changed low-order floating-point bits between builds over the real corpus. The
gatekeeper rejected it:

```text
reproducibility:                         failed
reconciliation/unique_asset_day:        passed
reconciliation/volume_reconciles:       failed
replay:                                 failed
confinement:                            passed
published rows:                         0
```

This is the rejection beat requested by the pitch, discovered by the corpus
rather than planted with `random()` or `current_timestamp`.

### Corrected promotion

The transformation was changed to aggregate a fixed-precision
`DECIMAL(38, 18)`. The same source snapshot then passed every gate and promoted:

```text
source snapshot:       1008
published rows:        1,746
distinct assets:       1,746
published transfer sum: 879,574
promotion receipts:    1
```

The published transfer count reconciled exactly to all 879,574 Bronze transfer
rows. The receipt and Silver replacement were committed by the same promotion
transaction.

## DuckDB 1.5.5 compatibility baseline

Before publication, the repository runtime was upgraded as one compatible set:

```text
DuckDB CLI:                    1.5.5 (Variegata)
duckdb-go:                     v2.10505.0
duckdb-go-bindings:            v0.10505.0
stable DuckLake extension:     d8a1881e
stable Quack extension:        c154811
```

The Nix CLI artifact is pinned to the SHA-256 digest published on the official
DuckDB `v1.5.5` release. The server's `INSTALL ducklake` and `INSTALL quack`
resolved extensions from the DuckDB 1.5.5 extension namespace, so neither
extension was loaded across an engine ABI boundary.

The original saturated ingest timings above were collected before this upgrade
and remain DuckDB 1.5.4 evidence. The resulting full 1,000-ledger catalog was
then reopened under DuckDB 1.5.5, pinned again at source snapshot `1008`, and
the complete Gatekeeper suite passed with the same 1,746 promoted rows. A
separate fresh-catalog 1.5.5 smoke also promoted and verified a real Bronze row
and its `governance.promotions` receipt.

## Still required by the pitch

- provider-specific two-tool agent loop (`run_sql`, `submit_proposal`)
- repeatable clean-lake demo script using the external fixture manifest
- gated incremental promotion after additional live ledgers arrive
- visible time-travel rollback sequence
- chaos beat for killing Gatekeeper during promotion
- raw screen recording and customer/machinery cuts

The current slice is intentionally the deterministic core the agent will call;
none of its verdicts depend on a model.
