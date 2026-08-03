# DuckLake Snapshots — how they work and why the product stands on them

Written 2026-07-26. Companion to `gatekeeper-and-lake-revamp-explainer.md`.
Mechanism as verified against DuckDB 1.5.4 + stable ducklake during the
2026-07-23/24 work; empirical claims below were observed directly.

## 1. The mechanism

DuckLake is MVCC with the multi-version bookkeeping kept in a regular
relational database (the catalog — our `stellar.ducklake` DuckDB file;
Postgres/SQLite/MySQL also supported) instead of in metadata files.

- **A snapshot is a row** in `ducklake_snapshot`:
  `(snapshot_id, snapshot_time, schema_version, changes)`. Committing an
  insert moved `current_snapshot()` from 34 → 35 — that row *is* the event.
- **Everything carries a validity interval.** Data files
  (`ducklake_data_file`), delete files, and inlined rows all carry
  `begin_snapshot` / `end_snapshot`:

  ```
  file_042.parquet   begin=31  end=NULL   alive
  file_017.parquet   begin=12  end=31     superseded at 31
  inlined row 889041 begin=35  end=NULL   alive, stored in catalog
  tombstones for file_017 rows 100–250    begin=28
  ```

- **Parquet files are immutable** — never edited in place. A commit, inside
  one ACID transaction on the catalog DB: write new files/inlined rows,
  stamp intervals (begin on new, end on superseded), insert the snapshot
  row. That transaction is the commit point (~46–150ms measured for our
  multi-table ledger commits, fsync included).

Reading `AT (VERSION => S)` = select files/rows whose interval contains S,
scan those. No copies are ever made; old snapshots keep *referencing* the
old file set. Time travel is the ordinary read path with a different
number.

## 2. What falls out of the design

- **Whole-lake atomicity.** A snapshot spans the entire catalog, not one
  table (unlike Iceberg's per-table snapshots). Our per-ledger transaction
  touches ~18 tables and yields ONE snapshot — readers can never see half a
  ledger, and reconciliation gates can compare bronze vs. staging *at the
  same instant* meaningfully.
- **Deletes are bookkeeping.** Parquet-backed deletes write positional
  tombstone files; inlined-row deletes just set `end_snapshot`. The
  delete-then-insert replay idiom maps exactly: a replayed ledger ends one
  row generation and begins another; both stay time-travelable.
- **`table_changes` is derived, not logged.** Changes between snapshots A
  and B are an interval query (insertions: begin ∈ (A,B]; deletions:
  end ∈ (A,B]). No CDC log to lag. **Verified: inlined, not-yet-flushed
  rows appear in `table_changes` immediately** — replica freshness is
  independent of maintenance/flush cadence.
- **Optimistic concurrency, arbitrated by the catalog.** Readers pin a
  snapshot at start and see a stable world mid-scan (why
  maintenance-during-ingest passes parity in the chaos harness).
  Conflicting writers: one catalog transaction aborts — the conflict the
  ingest handler retries on.
- **Expiry is the other half of "millions of snapshots are fine."**
  Snapshot rows are nearly free to keep; the files they reference are
  reclaimable only when no live snapshot needs them.
  `expire_snapshots(older_than)` marks generations dead;
  `cleanup_old_files` (deliberately not automated here) deletes
  newly-unreferenced files.

## 3. Product mapping (why the gatekeeper stands on this)

- **Branch** = pin snapshot S for reads + write in a private staging
  schema. Gates can demand determinism because the world cannot move under
  the agent.
- **Merge** = one promotion transaction → one new snapshot.
- **Rollback** = the pre-merge state was never destroyed; restore is a
  time-travel read or range replay.
- **Audit** = a receipt pins a snapshot id — an address into history anyone
  can dereference and re-run the gates against.
- **Retention is the re-verifiability window.** Receipts are re-checkable
  only while retention keeps their snapshot's files. `SNAPSHOT_RETENTION`
  is therefore a *product* parameter (compliance tier), not only an ops
  one — and it must also exceed replica-sync's worst-case checkpoint lag.

## 4. Honest boundaries

- **History is linear.** One chain of snapshots; no native named branches
  or divergent merges. Our "branch" is a pattern (pinned read + private
  schema), not a catalog fork. Sufficient for proposals (frozen reads +
  atomic merges); do not promise literal "git for data." If DuckDB ships
  true forking (Quack–DuckLake integration territory), the workspace
  absorbs it as an upgrade.
- **Inlined commits cost ~0.18ms/row** in the catalog DB — inlining is a
  hot buffer for small writes (DuckDB's default limit is 10), not a bulk
  path. The inline limit tiers latency vs. file count (measured:
  20000→1.7s, 1024→0.55s, 256→85ms per ledger).
- **Snapshot ids are catalog-global.** Per-tenant "history" is a filtered
  view of one shared chain; tenant isolation is schema + access-layer
  scoping, not per-tenant chains.

## 5. The one-sentence version

Databases have always had MVCC internally and vacuumed the old versions
away; DuckLake keeps them and makes them addressable in SQL. The product
insight built on top: **an addressable history plus deterministic checks
equals trust you can sell.**

Related: `gatekeeper-and-lake-revamp-explainer.md` (product architecture),
`quack-ducklake-architecture.md` (snapshot-as-WAL replication strategy),
`sub-400ms-ingest-phase-handoff-2026-07-23.md` (measured commit costs),
`ducklake-maintenance` README (flush/merge/expire and the retention
constraint).
