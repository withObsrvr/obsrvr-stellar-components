# Pitch: The Gatekeeper Demo (agent as gate-checked fifth writer)

**Status:** IMPLEMENTATION STARTED 2026-08-04 — the promotion core and first
1,000-ledger corpus run are recorded in
`docs/gatekeeper-demo-progress-2026-08-04.md`.

## Problem

The venture thesis — *safe convergence*: agents' work merges into a governed
lake only through deterministic gates — is currently an argument in a memory
file. The "agent-native data" category is being framed around isolation
(sandboxes, per-agent databases) right now; the counter-position needs to be
*visible*, not described. One artifact serves two audiences: a customer cut
("I asked in English Monday morning; Monday afternoon I had a verified,
backfilled, continuously-fresh table") and a machinery cut (rejection loop,
gated merge, provenance, rollback) for the venture argument.

## Appetite

1 week.

## Solution (fat marker)

**The customer task:** "daily transfer volume and unique senders per asset,
since the start of my range" → `silver.asset_daily_volume`, built from
`bronze.token_transfers_stream_v1` over a ~1k-ledger pubnet backfill, with
the live ingest pipeline (~270ms/ledger) running underneath.

**New component `ducklake-gatekeeper`** (same shape/size as
`ducklake-maintenance`): consumes a *proposal* — a manifest of transformation
SQL + declared target + invariant SQL + pinned source snapshot — runs the
gates through Quack, and on all-green promotes staging → `silver.*` in one
delete-then-insert transaction, recording a provenance row in
`governance.promotions` (agent id, proposal hash, source snapshot, gate
results).

**Gates v1** (all deterministic, all existing patterns):
1. *Reproducibility* — run the SQL twice at the pinned snapshot;
   `EXCEPT ALL` both ways must be empty (kills `now()`, `random()`,
   unstable ordering).
2. *Reconciliation* — manifest invariants, e.g. silver daily totals must
   equal bronze sums over the same ledger range (the watermark-gap-query
   family of checks).
3. *Replay* — rebuild a randomly chosen ledger sub-range; rows must be
   byte-identical (the crash-contract property, applied to agent output).
4. *Confinement* — the agent wrote only inside its staging schema.

**Minimal agent driver:** an Anthropic-API loop with exactly two tools —
`run_sql` (restricted to the staging schema plus bronze reads
`AT (VERSION => S)`) and `submit_proposal`. On gate failure it receives the
gate's error text and retries. Deliberately not a framework.

**The scripted scenario:** task in English → agent explores and builds →
one sloppy attempt (planted if it doesn't happen organically, e.g.
`current_timestamp` in the SQL) → gate rejection with legible reason →
agent fixes → promotion + provenance row → new ledgers arrive → one gated
*incremental* promotion proves "stays current" → rollback via time travel.
Deliverable: repeatable run script + raw screen recording covering both cuts.

## Rabbit holes (don't)

- No agent framework — two tools, one loop, one task.
- No real server-side authorization: client-side statement allowlist plus
  the post-hoc confinement gate is enough for the demo; Quack's auth
  callback is the future answer, noted not built.
- No dashboard/frontend — tables and SQL output carry the demo (a Grafana
  panel is a COULD).
- No gate DSL — invariants are SQL strings in the manifest.
- No Soroban contract-event decoding — that's the sequel variant, not this.

## No-gos

Multi-agent concurrency; product packaging/hosting; publishing the recording
before the venture positioning is settled; any changes to the production
write-path components.

## Scope line

```
COULD ───────  Soroban contract-events variant; Grafana panel; narrated
               machinery cut
NICE ────────  chaos beat (kill gatekeeper mid-merge → catalog unharmed);
               scheduled continuous re-promotion; snapshot-diff blast-radius
               gate
MUST ════════  agent builds + backfills the table end-to-end; at least one
               gate rejection with agent recovery; promotion with provenance
               row; one gated incremental update; time-travel rollback;
               repeatable run script + raw recording
```

## Done

A clean-lake run script replays the whole scenario deterministically
(modulo the agent's own wording), and a raw screen recording exists showing:
the English request, the rejection and recovery, the verified promotion with
its receipt, the incremental update landing, and the rollback. Every gate
verdict is produced by deterministic SQL, never by a model.

## Why this is days, not months

Already built: bronze at ~270ms/ledger, snapshots + time travel, Quack as
the agent's SQL surface, `EXCEPT ALL` parity and gap-query gate patterns,
the delete-then-insert promotion idiom, provenance-table habits, chaos
methodology. New: the gatekeeper runner, the manifest format, the two-tool
agent driver, and the script.
