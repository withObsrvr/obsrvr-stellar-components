# Phase: Sub-400ms Bronze Ingest (bulk-ingest RPC + typed contract)

**Status:** BETTED 2026-07-23 — promoted from unbetted pitch when Tillman set
a hard 400ms upper limit on ledger-arrival → queryable. Two 1-week cycles.
The previously shaped overlap/delete-skip interim cycle was killed: its floor
(~0.9s client-side staging) cannot reach 400ms and the RPC deletes that
machinery; only the watermark-gated delete-skip survives, folded in here.

**Latency budget (measured floors, this hardware):** RPC transfer ~5–15ms;
row_json JSON decode ~150–250ms parallel (dies in Cycle B); rows into engine
~50–350ms depending on path; DuckLake multi-table catalog commit ~50–150ms.
Cycle A target ~0.6–0.8s (contract unchanged); Cycle B target **<400ms with
margin** (~150–350ms).

**Cycle A** — ingest RPC, contract unchanged: server-side handler decodes
row_json and inserts via in-process chunked prepared statements.
**Cycle B** — typed-columnar `LedgerBatch` v2 (processor ships typed columns,
no JSON anywhere); typed values unlock the native-table Appender path
server-side. Versioned migration: v1 accepted during transition.

## Cycle A progress (2026-07-23)

Implemented and measured end-to-end: `BronzeIngestService` on
quack-ducklake-server (INGEST_PORT, ordered stream, per-ledger acks,
watermark-gated delete-skip, replay-on-uncertainty), shared write machinery
extracted to `pkg/bronze`, sink `DUCKLAKE_MODE=ingest-rpc`.

Phase timings per mainnet ledger (server-side, instrumented): RPC ~30ms;
decode+native-staging ~0.9s; preface (network check + metadata + watermark)
~15ms; INSERT..SELECT transfer ~35ms; DuckLake commit — **depends on the
inline row limit**, the key discovery: inlined commits cost ~0.18ms/row in
the catalog DB (inlining is designed for small writes; DuckDB's default limit
is 10). Measured: limit 20000 → ~1.7s commit; 1024 → ~0.55s (~1 file/ledger);
256 → ~85ms (~7 files/ledger, maintenance merges). Server default now 1024.

Current total: ~1.0–1.2s ledger-arrival → queryable (was 2.7s staged, 13s
before). Remaining to <400ms is exactly Cycle B: kill row_json decode and use
the Appender on the native staging tables (~0.9s → ~0.1–0.15s), with the
inline limit tuned alongside the maintenance interval.

Still open in Cycle A: chaos-harness ingest-rpc scenario (kill/replay parity
in RPC mode), sink README documentation.

## Cycle B result (2026-07-23) — TARGET MET, contract unchanged

**Measured: 232–318ms server-side per ledger (244–349ms sink-observed) at
inline limit 256** — under the 400ms ceiling, sustained across a 6-ledger
run, exact count parity, XDR enrichment intact, watermark gap 0.

Cycle B shipped WITHOUT the planned proto v2 contract change. Measurements
showed the staging cost was (a) a double JSON parse per row in typedValues —
fixed by lazy fallback-map decoding — and (b) per-row statement Execs — fixed
by `duckdb.NewAppenderWithColumns` into the native memory staging tables
(staging 900ms → ~45ms; the appender accepted every table's value types,
covered by TestStageWithAppenderCoversAllTypedTables). Phase profile per
ledger: decode ~60ms, appender staging ~45ms, preface ~12ms, transfer ~33ms,
DuckLake commit ~95ms.

The typed-columnar proto v2 remains unbetted future work — its remaining
value is deleting the processor-side JSON encode and the sink/server decode
entirely (~60ms) plus contract cleanliness, not latency we need.

Sub-400ms operating configuration: `DUCKLAKE_MODE=ingest-rpc`,
`INGEST_PORT` set, `DUCKLAKE_INLINE_ROW_LIMIT=256` (~7 parquet files/ledger;
run ducklake-maintenance on a 1–5 min interval), server default remains 1024
(~0.55s commits, ~1 file/ledger) for deployments that prefer fewer files
over sub-400ms.

## Problem

With the Parquet-staged transport plus the overlap/delete-skip cycle, the
write path sustains ~1.3–1.7s per ledger. The remaining cost is structural to
shipping data through a *client-side* DuckDB and the Quack SQL surface:
staging encode (~0.9s of JSON decode + local inserts + COPY) and a remote
script execution. The engine-side floor measured on this hardware is
~100–200ms per ledger (data insert ~25ms + multi-table DuckLake catalog
commit ~50–150ms). Nothing between the sink and that floor is essential.

Quack-native remote table writes would be the clean fix but are beta-blocked
(verified 2026-07-23 on DuckDB 1.5.4): the client-side catalog mapping cannot
address the server's attached DuckLake catalog, and
`INSERT INTO remote.x SELECT FROM local` fails with "streaming scans + insert
not currently supported".

## Appetite

One 1-week cycle.

## Solution (fat marker)

No Arrow anywhere — the payload is the protobuf we already have.

- New streaming gRPC service on `quack-ducklake-server` (separate port from
  Quack): `IngestLedgerBatch(stellar.components.v1.LedgerBatch) → Ack`.
- Server-side handler does exactly what the sink does today, but in-process:
  typed-row decode (reuse `typedRowInsertValues` + specs — move the shared
  code to an internal package), 128-row chunked prepared inserts, one
  transaction per ledger: watermark-gated delete-skip; metadata + watermark;
  COMMIT. Normal insert planning, so data inlining applies — rows land in the
  catalog, immediately queryable, no parquet on the hot path.
- Ingest handler and Quack SQL traffic must serialize on the single write
  session — one writer mutex around the pinned connection; document that
  Quack-side writes (replica tooling, maintenance) queue behind ingest.
- `ducklake-sink` in this mode forwards batches over the RPC and keeps only
  health/retry logic. The staged-parquet path remains available as fallback
  (`DUCKLAKE_MODE=quack` unchanged; new mode `DUCKLAKE_MODE=ingest-rpc`).
- Expected: ~2ms RPC + ~100–200ms engine ≈ **~150–300ms ledger-arrival →
  queryable**.

## Rabbit holes (don't)

- No Arrow, no Quack protocol extension, no protobuf contract changes.
- No multi-writer ingest; one stream, ordered, one ledger per transaction.
- No TLS/authz design beyond reusing the existing token via gRPC metadata.
- Don't move XDR decode or typed-row production upstream in this cycle.

## No-gos

- Replacing the Quack SQL surface (replica-sync, maintenance, ops all stay).
- Object-store staging work (unrelated).

## Done

6-ledger pipeline run in `ingest-rpc` mode: sustained ledger-arrival →
queryable < 500ms measured from sink logs; zero parquet in the lake during
ingest; typed counts identical to the staged-transport baseline; kill/replay
chaos scenario green in ingest-rpc mode (crash mid-stream → replay produces a
catalog byte-identical to a never-failed baseline); staged-transport mode
still green as fallback.

## Supersession risk (why this is unbetted)

DuckDB's announced roadmap: "we are going to integrate Quack into DuckLake,
so that it becomes possible to use a remote DuckDB server as a DuckLake
catalog … greatly improve performance, especially with inlining" (DuckDB 2.0,
fall 2026). That integration makes clients attach the lake directly with
inlined writes streaming over Quack natively — strictly better than this RPC
and upstream-maintained. This pitch is the insurance policy, not the plan.
