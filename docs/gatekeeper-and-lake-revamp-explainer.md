# The Gatekeeper Demo & the Obsrvr Lake Revamp — the full picture

Written 2026-07-26. This is the explainer behind the terse artifacts: it
records *why* the gatekeeper demo was betted, what the obsrvr-lake revamp
is, how they connect to Prism's SCF tranches and the venture direction, and
the sequencing that ties them together. Companion documents are indexed at
the end.

---

## 1. The thesis (why any of this)

AI collapsed the cost of **writing** systems, not **judging** them.
Architecture taste and operational rigor — knowing which invariant matters,
measuring before believing, refusing to build the unnecessary thing — are
the scarce inputs now. Corollary: **simplicity compounds with AI.** A
legible stack (DuckDB, DuckLake, Nomad, single-binary components) is one an
agent can safely operate; a 40-service sprawl is not. Tillman's taste for
sharp simple tools is the same property that makes his infrastructure
agent-operable.

The market is currently naming a category — "agent-native data
infrastructure" — around the wrong half of the problem: **isolation**.
MotherDuck sells per-agent sandbox Ducklings; the meme is "give every agent
its own database." Isolation is the easy half and commoditizes instantly.
The unclaimed half is **safe convergence**: many writers (deterministic
components *and* LLM agents) merging into one durable system of record
under operational contracts — ordered commits, watermark/replay semantics,
provenance, and *verification gates*, so work is admitted because it passed
checks, not because an agent claimed success.

The precedent: GitHub never sold editors. Developers brought their own
tools, and that made GitHub bigger, because it sold the place where work
converges — repos, PRs, checks, protected branches. In an
everyone-brings-their-own-agent world, obsrvr sells **branch protection for
data**: snapshot = branch, gates = the PR check, promotion = the merge.

Demand evidence that predates the pitch: Leo Meng (Tokenpad orchestration
layer) asked in **March 2026** for "custom indexing logic (similar to
subgraphs or programmable pipelines)" — the Layer-2 product, requested
before it was designed. Jake/SDF (May 2025): "most clients would prefer the
service to host the data and expose an API… maybe webhooks." jcanneto (June
2025): plug-and-play transfer/balance APIs. All three quotes sit in the SCF
submission's own traction section.

## 2. The substrate (what this week built)

The stellar-components write path was rebuilt 2026-07-23/24, measured at
every step: **~25s → ~270ms per ledger, ledger-arrival → queryable**, via
envelope removal, inline-first tiering + a maintenance component,
Parquet-staged transport, and finally an in-process ingest RPC with
Appender staging. Key discovered facts: inlined DuckLake commits cost
~0.18ms/row (the inline limit is a latency/file tiering knob: 20000→1.7s,
1024→0.55s, 256→85ms); data must never travel as SQL text; `table_changes`
sees inlined rows (replica freshness is independent of flush cadence). The
crash contract — watermarks in the data transaction, delete-then-insert
replay, replay-on-uncertainty — is chaos-tested: kill the server
mid-stream and recovery must be byte-identical to a never-crashed baseline.

This matters strategically because the pipeline is already a **four-writer
safe-convergence deployment**: sink, maintenance, replica-sync, and
index-materializer all converge on one governed catalog under those
contracts. The gatekeeper adds a fifth writer — an LLM agent — governed the
same way. The venture is the write path, generalized.

## 3. The gatekeeper demo (what it is, for someone new)

**Plainly:** bronze is "everything that happened on Stellar, verbatim,"
fully automated, ~270ms fresh — no human or agent touches it. Customers
don't want bronze; they want *their answer* — "my protocol's daily volume,
TVL, unique users, since launch." The agent in the demo plays the data
engineer the customer would otherwise hire: it reads bronze, writes the
transformation, builds the answer table. The gates are why the result is
usable for money-adjacent numbers: before promotion, a deterministic
verifier (never a model) checks that the output is reproducible
(double-run at a pinned snapshot, `EXCEPT ALL` empty), reconciles against
raw chain totals, replays byte-identically on a sampled ledger range, and
touched nothing outside its declared target. Only then is it merged — one
transaction, with a provenance receipt.

**The customer outcome being sold:** *"I asked a question Monday morning;
Monday afternoon I had a live table answering it — backfilled, updating
within a second of each ledger, with a receipt proving the numbers
reconcile against the chain. I hired no one, and I didn't have to take an
AI's word for anything."*

**The core mechanic (most-misunderstood point):** what gets promoted is the
**transformation (SQL + invariants), not the table**. The dev-time snapshot
pin exists only so the gates are deterministic — scaffolding, expirable.
After promotion, an incremental runner (the index-materializer pattern:
delete-then-insert by range, per-table watermarks) re-applies the
transformation to each new ledger range with cheap per-increment gates. The
failure mode is a feature: a failed increment **freezes the table
stale-but-provably-correct** and alerts — "your dashboard stops before it
lies." Ad-hoc queries are free and unmaintained; **promotion is the paywall
moment.**

**The demo bet** (docs/pitch-gatekeeper-demo.md, BETTED, 1-week appetite,
first post-cool-down cycle): agent builds `silver.asset_daily_volume` from
`bronze.token_transfers_stream_v1` over a ~1k-ledger backfill with live
ingest underneath; a new `ducklake-gatekeeper` component runs the four
gates and promotes with provenance; scripted beats include a rejected
sloppy attempt with agent recovery, one gated incremental update, and a
time-travel rollback.

**The video is two cuts of one recording.** Customer cut: English request →
verified live table, same day. Machinery cut: the rejection loop, the
merge, the provenance row, the rollback — the venture argument made
visible. Production note: the agent driver is deliberately thin (two tools:
`run_sql`, `submit_proposal`) and connects through the same MCP surface a
customer's own agent would — the demo must read as "this could be your
agent."

## 4. The business model

Three layers:

1. **Data utility (wedge, not the business):** bronze/silver as read-only
   API + MCP. Differentiators vs Dune/Goldsky-style indexers: freshness
   (~270ms), single-box cost structure, reconciliation receipts. Free tier
   = acquisition.
2. **Governed workspace (THE business):** customers' own agents connect via
   MCP — read bronze at pinned snapshots, write in a private staging
   schema, submit proposals; the gatekeeper promotes only verified work
   into their continuously-fresh silver namespace. BYO-agent is the moat:
   heterogeneous agents make the trust boundary *more* valuable.
   **Consumption guarantee: promote once, consume five ways** — SQL
   (replica), auto REST per table, MCP resource, `table_changes`
   CDC/webhooks, parquet export/sync-out (deliberately anti-lock-in).
3. **Horizontal escape:** the merge contract generalized to any DuckLake
   (including MotherDuck's hosted) once the vertical proves it.

**Pricing hypotheses** (validate with design partners): Read free→~$79/mo;
**Builder ~$149–299/mo per promoted pipeline** (subscription, because gated
maintenance is a standing service); Protocol pack ~$999/mo (~8 pipelines);
Compliance ~$1.5–3k/mo/org (receipt bundles, attestations, SLA — priced
against audit cost); Dedicated box ~$3–8k/mo. A **Scale rung** (pack
pipelines + metered reads) is needed for B2B2C read-heavy customers like
Tokenpad, whose end-users generate the reads. Never charge for: agent
compute (BYO = their model bills), data export, seats. COGS is a box;
capacity is ~hundreds of pipelines per box; margins 90%+; the binding
constraint is trust and onboarding, which the receipts and published
write-ups attack for free.

**Competitive frame:** Stellar Indexer (Creit Tech) is a fixed-endpoint
menu — good DX, tracked-contracts-only, lookup API (≤200 records/call), no
analytics, no verification story; its ~$170–400/package prices independently
validate the Builder bracket. RPC v2 full-history is substrate maturing,
not competition: retrieval ("what happened") vs answers ("what it means,
daily, provably") — Ethereum's decade of free archive RPC *created* Dune
and The Graph. One requirement surfaced by the Tokenpad shape: workspaces
need **customer-supplied reference tables** (e.g., watchlists of user
addresses joined against bronze).

## 5. The Obsrvr Lake revamp (what it is)

Current obsrvr-lake (~/Documents/ttp-processor-demo/obsrvr-lake) is a
hot/cold lambda: `stellar-postgres-ingester` → PG hot buffer →
`postgres-ducklake-flusher` (10-minute cadence) → DuckLake, with silver
processors and serving projections on top, and Prism reading from it.

**The revamp is a collapse, not a rewrite:**

- The ingester + flusher + hot/cold seam are replaced by the
  stellar-components ingest-RPC bronze path (~270ms, one catalog, no
  seam, no PG operational burden, no dual-store reconciliation).
- The silver processors (`silver-realtime-transformer`,
  `silver-current-state-projector`, serving projections) become
  **transformations over bronze — i.e., the gatekeeper's first promoted
  pipelines.** Obsrvr is its own first workspace tenant. "Prism runs on
  promoted tables" becomes literally true and is the platform's standing
  proof.
- Prism itself (Go/Templ/htmx read layer) barely changes; only the lake
  under it does.
- The single-box end goal (Galexie archives + stellar-rpc + the lake stack
  under local Nomad) collapses the remaining source-lag term: local
  archive/RPC reads make sub-second ledger-close→queryable realistic end
  to end, and it is the hardware story behind Dedicated-tier pricing.

**Sequencing dependency:** the revamp starts *after* the gatekeeper demo,
because the demo builds the promotion machinery the revamp's silver layer
then adopts.

### 5.1 What a manifest is

A manifest is the declarative unit the gatekeeper consumes — the "pull
request" for a data pipeline: what to build, from what, and what must hold
true. Illustrative shape:

```yaml
# manifests/prism/asset_stats.yaml
target: prism.asset_stats            # namespace(schema) . table
incremental_key: ledger_range        # how increments are bounded
source_snapshot: 4182                # pinned only during verification
transformation: |
  SELECT asset, date_trunc('day', closed_at) AS day,
         count(*) AS transfers, sum(amount) AS volume,
         count(DISTINCT "from") AS unique_senders
  FROM bronze.token_transfers_stream_v1
  WHERE ledger_sequence BETWEEN $start AND $end
  GROUP BY 1, 2
invariants:
  - name: volume_reconciles_to_bronze
    sql: SELECT (SELECT sum(volume) FROM {target} WHERE …)
              = (SELECT sum(amount) FROM bronze.… WHERE …)
```

The gatekeeper runs the universal gates (reproducibility, replay,
confinement) plus the manifest's own invariants; the incremental runner
re-executes the transformation per new ledger range forever. **The manifest
replaces a Go service** — the transformation logic that today lives in
compiled projector code becomes range-parameterized SQL. The contract,
however, is *deterministic reproducible output*, not SQL specifically: a
genuinely procedural transform (swap heuristics, semantic classification)
can remain a Go program and still be a gated tenant — stage its output,
verify, promote. SQL manifests are v1 because they cover most of the
surface and agents write them fluently.

### 5.2 Migration mechanics: one concept, today vs. after

Today, "account balances" exists as 2–3 implementations across two stores:
`serving-projection-processor`'s `account_balances` projector (Go, reads
bronze_hot PG, writes serving PG, checkpoints in
`sv_projection_checkpoints`); `silver-history-loader`'s `balance_changes`
(Bronze DuckLake → Silver DuckLake, bookkeeping in `silver_load_manifest`);
`silver-current-state-projector` rebuilding `address_balances_current`
(its own chunk planner, manifest file, failure classifier). Three
bookkeeping systems, no verification that the implementations agree, and
change = multi-service redeploy + manual re-backfill. Notably, the
current-state projector already implements "deterministic chunks +
idempotent replacement SQL + durable manifests + resume" — the incremental
runner's skeleton, reinvented per component because the lambda forced every
transformation to exist per-store and per-temperature.

After: one bronze (no hot/cold split), the concept defined once as a
manifest, one generic runner (backfill = big ranges, live = small ranges —
same machinery), one bookkeeping system (watermarks +
`governance.promotions`), gates on promotion and every increment,
freeze-don't-lie instead of silent drift, change = proposal → gates →
promote with provenance and time-travel rollback.

| | Today | Promoted pipelines |
|---|---|---|
| Definitions per concept | 2–3 (hot Go, cold Go, current-state Go) | 1 manifest |
| Bookkeeping | 3 systems | watermarks + promotions |
| Backfill vs live | separate components | same runner, different ranges |
| Wrong-but-successful runs | undetectable | gated; freeze + alert |
| Logic change | multi-service redeploy | proposal → gates → promote |
| Ops surface | 4 bespoke services | gatekeeper + runner + manifests |

**Migration recipe (per-table, not big-bang):** pick one projector →
re-express over the new bronze as a manifest → gate → promote → point
Prism's page at the promoted table → retire the old code path → repeat.
Every migrated table is revamp progress *and* a dogfooding rep on the
customer-facing machinery.

### 5.3 Serving after the revamp: no per-customer Postgres

Today's `serving` PostgreSQL existed for one reason: the old lake was
10-minutes stale and cold, so low-latency reads *needed* a hot store.
**That reason is gone** — the new lake is ~270ms fresh with CDC-visible
rows. Serving PG was a symptom of the lambda; the revamp removes its cause.

The multi-tenant serving model, with no per-customer databases:

- **One catalog, schema-per-customer** (`prism.*`, `tokenpad.*`, …, each
  with a private staging schema). DuckLake snapshots are whole-catalog, so
  gates and provenance span tenants naturally.
- **Read replicas** (ducklake-replica-sync + replica Quack servers) serve
  all read traffic. Builder-tier tenants share replicas; scale-out = add
  replicas; Dedicated tier = your own box/catalog (hard isolation).
- **All five access surfaces sit on the replica, statelessly, fronted by
  obsrvr-gateway** (~/Documents/obsrvr-gateway — it already has API-key
  auth + key store, Prometheus metering, multi-backend load balancing, and
  early DuckLake integration work). The gateway is the product boundary:
  key → tenant → schema + tier + quotas. REST translates to SQL against
  replicas (reusing the existing LB across replica Quack servers); MCP is
  a thin service the gateway fronts for auth/metering; SQL is a scoped
  HTTP endpoint whose executing session pins the tenant schema (read-only,
  timeouts, row caps) — native Quack protocol access is Dedicated-tier
  only until Quack's authz callback matures; webhooks are a dispatcher
  polling `table_changes`; parquet export is native. Gateway metering
  events land in the lake; billing rollups become promoted pipelines.
- **stellar-query-api survives with its purpose reallocated.** Its hardest
  current job — transparent hot/cold merging + RPC freshness fallback — is
  the lambda's tax and is deleted (one store, 270ms fresh). It keeps the
  endpoint catalog, pagination/cursors, response shaping, and the
  Horizon-compat layer (a wedge product for Horizon-deprecation migrants).
  It gains: the tenant-scoped SQL session layer (USE schema, read-only,
  timeouts, row caps — gateway passes tenant context down), optionally the
  MCP transport over the same shape→SQL engine, and auto-registration of
  `/v1/{tenant}/tables/{table}` routes by watching
  `governance.promotions` — promotion creates the endpoint, no deploys.
  Instance multiplicity collapses from per-store-per-layer to per-network
  (one catalog + replica set + query-api per network). Request flow:
  client → Traefik → gateway (identity/tier/metering) → query-api
  (meaning: shape→SQL) → replica Quack servers → replica catalog.
- **Postgres appears in exactly one place: the customer's side, optionally.**
  The CDC sync-out surface can push a promoted table into the customer's
  own Postgres/DuckDB. Anyone who wants PG gets it — in their infra, at
  their cost. Obsrvr never operates per-tenant databases.
- Latency honesty: materialized indexes + inlined-hot rows put replica
  point-queries comfortably inside the <400ms API commitments (and
  typically single-digit ms). If a future workload genuinely demands
  sub-millisecond OLTP serving, it becomes one shared consumer fed by
  sync-out — one derivation source, many serving materializations — never
  a parallel truth.

### 5.4 Where materialized indexes live (and which ones survive)

An index is a DuckLake table like any other: `index.tx_hash_index` etc.,
built by delete-then-insert per range, replicated to serving replicas.
After the revamp, index-materializer's rebuild loop merges into the
incremental runner — **an index is a promoted pipeline whose SQL is
trivial** (a key-sorted copy or pre-joined lookup shape). "Silver table,"
"serving projection," and "index" collapse into one concept: manifests
with different SQL.

The old serving projectors existed for three entangled reasons, only one
of which survives: (1) *analytics on a row store* — PG needed
pre-aggregation; the store is leaving. (2) *Sort-order physics* — a table
has one physical order (bronze = ledger order), so point lookups on
orthogonal keys (account, tx hash, contract) cannot zone-map-prune and
genuinely need key-sorted copies. This need is permanent; it just costs a
manifest now. (3) *Staleness* — the `*_recent` projector family existed
because the lake was 10 minutes cold; at 270ms fresh with ledger pruning,
that family is deleted, not migrated. A fourth hidden tax — small-file
explosion and unsorted flushes — is removed by the maintenance job, so
some old materializations may simply be unnecessary now.

Decision rule: run the query on the replica; if slow, check whether the
filter fights the table's sort order; if it does and the query is
hot-path, add an index manifest (minutes, gated, deletable when
measurement says so). Materializations are earned by measurement, not
built speculatively — the write path's method applied to the read path.

### 5.5 Which silver tables are shared vs. per-customer

Three rings, not two:

1. **Bronze** — shared facts, including standards-decoded rows. Note: the
   new bronze already absorbed part of the old silver (token transfers,
   effects, trades are bronze tables now); the boundary moved to
   "what happened (incl. standards decoding)" vs. "derived state/
   aggregation."
2. **Canonical silver** — shared, deliberately tiny, obsrvr-maintained
   `obsrvr.*` manifests promoted through the gatekeeper with public
   receipts. Membership test: exactly one right answer AND two unrelated
   customers would write byte-identical SQL (balances-current,
   trustlines-current, trade/volume rollups). This ring powers the
   free-tier catalog's gravity (market evidence: Stellar Indexer charges
   $299/mo for balances alone). Every entry is a forever-maintenance
   promise — keep it small.
3. **Tenant silver** — every table containing an opinion (TVL definitions,
   watchlists, semantic/swap classification). **Prism's opinionated tables
   live in `prism.*` — Prism is tenant #1, not a special case** — which
   makes the dogfooding claim structural.

Migration rule: default every migrated table to `prism.*`; a table earns
canonical status only when a second consumer needs the same definition
(demand-driven canonicalization). Test at that moment: "does the
definition contain a choice someone could reasonably make differently?" —
if yes it stays tenant regardless of popularity. Expected steady state:
bronze + ~5–8 canonical silver tables + tenant schemas, versus today's
~19 silver tables and 12 projectors.

## 6. Prism / SCF tranche connection

Prism is SCF #41 ($80K, three tranches). The Tranche 1 (MVP) video is being
recorded **on the current stack** — deliberately, so tranche evidence never
blocks on migration. The new stack beats every tranche *outcome* metric
(<30s tip latency committed vs ~270ms measured; <400ms API via the
replica; T3's zero-gap verification exceeded by watermark gap query +
byte-identical replay proofs).

Two cautions, recorded so they aren't lost: **(a)** Tranche 1's measurement
text names PostgreSQL in D1/D2 — written tranche updates must narrate the
architecture evolution as outcome improvement *before* Tranches 2/3 are
delivered on the new stack; **(b)** backfill honesty — 270ms is a streaming
number; the 61M-ledger backfill (T2D4's ≥90% target) is a batch problem:
idempotent delete-then-insert by range makes it embarrassingly parallel,
but a throughput spike (parallel range workers + bulk batches) should be
shaped when the revamp starts.

## 7. The sequence, end to end

1. **MVP tranche video** — now, current stack (structure in the 2026-07-24
   session; evidence-per-deliverable, criteria on screen).
2. **Cool-down** — mandatory after five shipped cycles.
3. **Gatekeeper demo** — betted, 1 week. Records the two-cut video; first
   viewer: Leo Meng (design-partner candidate — his orchestration/strategy
   workloads exercise freeze-don't-lie harder than dashboards).
4. **Obsrvr Lake revamp** — the collapse; silver becomes promoted
   pipelines; backfill spike shaped at start.
5. **Tranches 2/3** delivered on the new stack, evolution narrated in
   updates.
6. **Design partners → workspace beta** — 2–3 Soroban teams on their own
   agents; pricing discovery.
7. **Essays throughout:** the sub-400ms write-up (credibility; draft at
   docs/blog-draft-sub-400ms-ducklake.md) then "isolation is the easy
   half" (the category argument).
8. **Watch:** DuckDB 2.0 (fall 2026) Quack–DuckLake catalog integration —
   may replace the custom ingest RPC; the contracts and workspace survive
   either way.

## 8. Document index

- `docs/pitch-gatekeeper-demo.md` — the betted demo shape (gates, scope,
  Done).
- `docs/pitch-quack-bulk-ingest-rpc.md` — the sub-400ms phase pitch +
  measured results.
- `docs/sub-400ms-ingest-phase-handoff-2026-07-23.md` and
  `docs/ducklake-write-path-phase-handoff-2026-07-23.md` — the technical
  handoffs behind the numbers cited here.
- `docs/blog-draft-sub-400ms-ducklake.md` — publishable write-up (needs
  Tillman's review pass).
- `docs/production-gate-runbook.md` — the repeatable gates (quack +
  ingest-rpc chaos harnesses).
- `docs/ducklake-snapshots-explainer.md` — the snapshot mechanism the
  gatekeeper stands on: validity intervals, whole-lake atomicity, derived
  CDC, retention-as-reverifiability, and the linear-history boundary.
- Session memory (assistant-side): venture-threads-2026-07,
  prism-scf-and-lake-revamp, single-box-stellar-lakehouse-goal — terse
  mirrors of this document for cross-session recall.
