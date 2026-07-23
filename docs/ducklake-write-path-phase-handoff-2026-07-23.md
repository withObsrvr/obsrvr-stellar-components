# DuckLake Write Path Phase Handoff — 2026-07-23

Three shipped cycles rebuilt the quack-mode DuckLake write path. Ingestion-to-
queryable went from ~25s per ledger to a sustained ~2.7s cycle, the lake stopped
accumulating small parquet files on the hot path, and DL-9 (major) is resolved.

## Where things stand

| Metric | Start of day | End of day |
|---|---|---|
| Write script per ledger | 31 MiB SQL text | ~20 KiB (data as Parquet) |
| Per-ledger commit | ~25s | ~1.6s |
| Sustained cycle (staging + commit) | ~25s | ~2.7s |
| Parquet files created per ledger ingested | ~18 | 0 (catalog-inlined) |
| Live tracking at 5–6s ledger cadence | impossible | yes, with margin |
| Future 2s cadence | — | close; see follow-ups |

Data is queryable (including via `table_changes` CDC) the instant a commit
lands; inlined rows are fully visible before any flush.

## Cycle 1 — Drop envelope persistence (Option A)

`ducklake-sink` no longer persists `payload_json` (NULL) or `bronze_rows` in
either mode. The upstream archive is the replay source. DELETEs for both
surfaces remain so replays clean catalogs written by older sinks. The in-memory
protobuf `BronzeRows` still feeds the typed tables. ~70% of write volume
removed (measured: 13.8 MiB payload + 8.3 MiB row_json of a 31.4 MiB script).

## Cycle 2 — Inline-first commits + maintenance

- `quack-ducklake-server` applies `DUCKLAKE_INLINE_ROW_LIMIT` (default 20000)
  idempotently at startup via `set_option('data_inlining_row_limit', …)`.
  Per-ledger inserts land in the catalog database, not parquet. `0` disables
  inlining; negative leaves the persisted setting untouched.
- New component `components/ducklake-maintenance`: on an interval (default 5m)
  runs `ducklake_flush_inlined_data` → `ducklake_merge_adjacent_files` →
  `ducklake_expire_snapshots` (retention default 48h) through Quack. `RUN_ONCE`
  mode for jobs/harness. Deliberately does NOT delete expired files
  (`cleanup_old_files`/`delete_orphaned_files`) — storage reclamation is a
  separate, riskier operation.
- Verified empirically: `table_changes` sees unflushed inlined rows, so
  replica-sync freshness is independent of flush cadence.
- **Constraint:** `SNAPSHOT_RETENTION` must exceed replica-sync's worst-case
  checkpoint lag or replicas fall back to full resync.

## Cycle 3 — Parquet-staged transport (Option B)

Quack-mode write flow per batch (`stageBatchParquet` + `remoteWriteSQL`):

1. Rows decode to typed values in parallel across cores (JSON+reflection).
2. Values insert into local in-memory DuckDB `bronze.*` tables in 128-row
   chunked prepared statements (`multiRowInsertSQL`) — per-row Execs cost
   ~0.4ms each through database/sql and were the dominant staging cost.
3. `COPY (SELECT <explicit columns>) TO '<staging>/<ledger>_<nano>/<table>.parquet'`
   per table, then rollback empties the local tables (COPY output survives).
4. A ~20 KiB script ships over Quack: BEGIN; replay DELETEs; metadata +
   watermark; `INSERT INTO bronze.X (cols) SELECT cols FROM read_parquet(…)`;
   COMMIT. Row data never travels as SQL text.
5. On success the sink removes the batch staging dir; stale dirs (>1h) are
   swept at startup.

Config: `DUCKLAKE_STAGING_PATH` (default `ducklake/staging`),
`DUCKLAKE_STAGING_REMOTE_PATH` (path as the server sees it; defaults to the
same). The server must be able to read the staging path — note this interacts
with `QUACK_DISABLED_FILESYSTEMS`/`QUACK_ENABLE_EXTERNAL_ACCESS`.

Perf lessons (measured, in order): SQL parse was ~11s of the original 13s;
prepared-per-row saved ~2s; parallel decode saved only ~0.4s (JSON was never
the bottleneck); chunked inserts collapsed staging 3.7s → 0.9s.

## Verification

- Every cycle: 3–6 ledger pubnet runs (62080000–62080005), watermark gap 0,
  typed counts identical across all runs (1,092 tx / 2,161 ops / 1,528 effects
  / 89 trades / 15,144 events / 2,777 transfers for the first 3 ledgers), XDR
  envelopes intact through the parquet path.
- `make test` green; chaos harness green after each cycle, including the new
  scenario running a full maintenance cycle during active replay ingest, with
  chaos-vs-baseline parity proving flush/merge is invisible to logical
  contents.
- Harness transport gates: staged-parquet floor `QUACK_CHAOS_MIN_STAGED_MIB`
  (default 1) and script ceiling `QUACK_CHAOS_MAX_SCRIPT_KIB` (default 256) —
  a breached ceiling means row data leaked back into SQL text.

## Relationship to obsrvr-lake (ttp-processor-demo)

obsrvr-lake is a hot/cold lambda: PG hot buffer (fresh, seconds) +
postgres-ducklake-flusher (lake lags ~10 minutes). This phase collapses that:
the lake itself is now the fresh surface (~2.7s), using DuckLake's catalog as
the hot store via inlining. The staged-parquet transport is the flusher's own
`INSERT … SELECT FROM postgres_scan(…)` pattern with `read_parquet` as the
reader. What obsrvr-lake's PG tier still provides that this does not: OLTP
point-lookup indexes for serving — that remains the replica +
index-materializer's job here.

## Follow-ups (not started, in priority order)

1. **Staging/commit overlap** — stage ledger N+1 while N's remote commit is in
   flight (commit order stays serial). Cycle drops to max(staging, commit)
   ≈ 1.6s, clearing the future 2s ledger cadence. Small, well-bounded bet.
2. **Tip-capable source** — pipelines all use `BACKEND_TYPE=ARCHIVE`; verify
   raw-ledger-source's RPC backend (ledger-smoke already exercises RPC in this
   repo) and measure archive publish lag. The source is now the freshness
   ceiling.
3. **Object-store staging** — current staging requires a shared filesystem
   between sink and server; S3/minio staging is the multi-host version.
4. **Storage reclamation** — scheduled `cleanup_old_files` once retention vs
   replica checkpoint interplay is settled.
5. **Commit-time regression watch** — sink logs `staging … decode+insert/copy`
   and `committed in …` per ledger; wire into monitoring.

## State of the tree

All three cycles are uncommitted in the working tree on
`feature/tmosley/production-gate-validation` as of this handoff. Touched:
`components/ducklake-sink` (write path + tests), `components/ducklake-maintenance`
(new), `components/quack-ducklake-server` (inline limit + tests), Makefile,
`scripts/quack-chaos-harness.sh`, `pipelines/local-archive-quack-ducklake-flowctl.yaml`,
READMEs, `docs/event-contracts.md`, `docs/production-hardening-plan.md` (DL-9).
