# Sub-400ms Bronze Ingest Phase Handoff — 2026-07-23

Second phase of the day (after the write-path phase in
`ducklake-write-path-phase-handoff-2026-07-23.md`). Goal, set by Tillman:
hard 400ms upper limit on ledger-arrival → queryable in the DuckLake catalog.

**Result: 232–318ms server-side per ledger (244–349ms sink-observed),
sustained over pubnet ledgers 62080000–62080005, with exact count parity,
XDR enrichment intact, watermark gap 0.** Day's full arc:
~25s → 2.7s → 1.1s → ~270ms per ledger.

## Architecture

```
raw-ledger-source → stellar-ledger-processor
    → ducklake-sink (DUCKLAKE_MODE=ingest-rpc)          # no local DuckDB
        → gRPC BronzeIngestService (INGEST_PORT, default off)
            quack-ducklake-server, in-process:
              decode row_json (parallel, lazy second parse)   ~60ms
              Appender → memory.bronze native staging tables  ~45ms
              one DuckLake txn: watermark-gated delete-skip;
                metadata + watermark; INSERT..SELECT per table ~45ms
              commit (inline limit 256: big tables → parquet,
                small tables → catalog-inlined)               ~95ms
            ack → sink logs "ingest-rpc committed"
```

Ordering invariant: one ledger in flight, per-ledger acks; the server fully
commits N before the sink sends N+1. Replay semantics: server tracks the
high watermark (initialized from `ingest_watermarks`); fresh ledgers skip
the ~20 replay DELETEs; any failed/uncertain commit forces the replay path
(delete-then-insert) on retry and on the next batch. Concurrent maintenance
can abort a transaction — the handler retries once, then errors the stream;
the sink resets the stream and retries (3×, then fatal), same crash contract
as the other modes.

## Key engineering findings (measured, this hardware)

1. **Inlined DuckLake commits cost ~0.18ms/row in the catalog database.**
   Inlining is designed for small writes (DuckDB default limit: 10 rows).
   `DUCKLAKE_INLINE_ROW_LIMIT` is a latency/file-count tiering knob:
   20000 → ~1.7s commit, 0 files; 1024 → ~0.55s, ~1 file/ledger;
   256 → ~85ms, ~7 files/ledger. Server default 1024; sub-400ms deployments
   use 256 paired with a 1–5 min maintenance interval (Nomad job uses 2m).
2. **Chunked inserts directly into DuckLake tables pay per-statement catalog
   overhead** (~2.5s/ledger). Stage into native memory tables, then one
   `INSERT..SELECT` per table (~45ms total). A DuckDB transaction writes at
   most one attached database, so staging materializes fully before the
   DuckLake transaction begins.
3. **The planned proto v2 typed contract was not needed.** The decode cost
   was a double JSON parse per row (`typedValues` re-parsed RowJson for
   fallback columns — now lazy) plus per-row statement Execs (now the
   Appender). `duckdb.NewAppenderWithColumns` accepted every table spec's
   value types (locked in by TestStageWithAppenderCoversAllTypedTables).
   Proto v2 remains unbetted: worth ~60ms and contract cleanliness only.

## Code map

- `pkg/bronze/` — typed table specs, row decoding (`DecodeTypedRows`),
  insert SQL builders, catalog-network pinning, migrations; shared by sink
  (embedded + staged-parquet modes) and server (ingest). Extracted from the
  sink; the sink aliases keep its call sites/tests unchanged.
- `proto/stellar/components/v1/ingest_service.proto` → generated client and
  server stubs (`make proto` now also runs protoc-gen-go-grpc).
- `components/quack-ducklake-server/cmd/component/ingest.go` — the service:
  schema bootstrap (works against a fresh catalog), staging tables,
  appender staging, phase-timing logs (`ingest phases ledger …`).
- `components/ducklake-sink/cmd/component/ingest_client.go` — the
  `ingest-rpc` mode: stream lifecycle, ack verification, reset-on-error.
- `pipelines/local-archive-ingest-rpc-flowctl.yaml` — working local config.
- `deploy/nomad/quack-ducklake-server.nomad` (ingest port + limit 256),
  `deploy/nomad/ducklake-maintenance.nomad` (2m interval, paired).
- Chaos harness: `QUACK_CHAOS_SINK_MODE=ingest-rpc` (`make
  test-ingest-chaos`) runs the same kill/replay/baseline parity gates over
  the RPC path; transport-size gates apply to quack mode only.

## Security posture

The ingest stream is plaintext gRPC + shared token (`x-ingest-token`, same
`QUACK_TOKEN`), mirroring the dev-mode Quack posture. Localhost/LAN only, or
front with a TLS-terminating proxy — same recommendation as the Quack
endpoint itself.

## Open items after this phase

1. Storage reclamation (`cleanup_old_files`) — own cycle; time-travel and
   replica-checkpoint interplay.
2. Source freshness — the write path is ~270ms, so archive publish lag +
   fetch/decode now dominate tip lag; verify raw-ledger-source's RPC backend
   and measure archive lag.
3. Production-gate runbook refresh (branch predates all of today).
4. DuckDB 2.0 watch: Quack–DuckLake catalog integration (fall) may supersede
   the ingest RPC; the appender/staging machinery survives either way.
5. Proto v2 typed contract — unbetted.
