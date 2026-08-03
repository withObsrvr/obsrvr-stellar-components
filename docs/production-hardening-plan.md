# Production Hardening Plan

**Date:** 2026-07-02
**Status:** Proposed — awaiting cycle approval
**Scope:** `obsrvr-stellar-components` (all components) + delivery-semantics issues rooted in vendored `flowctl-sdk v0.1.2`

## Context

A production-readiness review (2026-07-02) covered every component, the shared
packages, and the vendored flowctl-sdk consumer runtime. Runtime verification
passed: unit tests, all 7 component builds, a live-mainnet `ledger-smoke`, two
full flowctl process-driver E2E runs (jsonl and ducklake sinks), and a replay
idempotency check against a real DuckLake catalog (zero duplicates).

**Verdict: the architecture is sound; the failure paths are not.** The design —
raw source / semantic processor / materializing sinks, `LedgerBatch` as the
commit boundary, deterministic IDs, ledger-range delete-then-insert — is
correct and verified working. Every blocker below is about what happens when
something fails, drifts, or replays after an upgrade.

**The production topology is Quack/replica mode.** There is currently no use
case for the embedded DuckLake path; it remains only as a dev/test fallback.
That means the beta Quack protocol, the Quack server, and the replica-sync /
materializer components are on the critical path and are hardened first —
not deferred.

```text
raw-ledger-source -> stellar-ledger-processor -> ducklake-sink (DUCKLAKE_MODE=quack)
                                                      |
                                            quack-ducklake-server (primary)
                                                      |
                    index-materializer ---- server-side range rebuilds
                                                      |
                    ducklake-replica-sync -> quack-ducklake-server (replica)
                                                      |
                                     stellar-query-api / obsrvr-gateway reads
```

The fix work is organized as three Shape Up cycles plus a backlog. Fixed time,
variable scope: if a cycle runs long, cut from its scope line — do not extend.

---

## Findings Register

Stable IDs for traceability. Severity: **blocker** = can silently lose or
corrupt data in production; **major** = breaks under realistic operational
events (restart, upgrade, misconfig); **minor** = quality/operability.

### DEL — Delivery semantics (rooted in vendored flowctl-sdk v0.1.2)

| ID | Sev | Finding | Where |
| --- | --- | --- | --- |
| DEL-1 | blocker | Consumer runtime logs handler errors and continues — no retry, nack, or crash. At-most-once delivery; any transient sink failure = permanent silent data loss. | `vendor/.../consumer/service.go:133-146` |
| DEL-2 | blocker | One goroutine per event, no concurrency cap or backpressure; destroys ordering and can exhaust downstream connections. | `vendor/.../consumer/service.go:133-146` |
| DEL-3 | blocker | No checkpoint/watermark anywhere in the pipeline; `pkg/checkpoints` has zero importers (dead code). Crash = unknowable gap. | repo-wide; `pkg/checkpoints/` |
| DEL-4 | blocker | `checkpoints.Store` truncate-in-place write, no temp+rename, no fsync — torn checkpoint bricks resume if ever adopted. | `pkg/checkpoints/checkpoints.go:30-38` |
| DEL-5 | major | `HealthCheck` unconditionally returns HEALTHY; documented `HEALTH_PORT` is never actually served on sinks. | `vendor/.../consumer/service.go:65-70`, `consumer.go:125,150-153` |
| DEL-6 | major | `log.Fatalf` inside registration goroutine kills a healthy sink on a transient control-plane blip; no retry/backoff. | `vendor/.../consumer/consumer.go:171-200` |
| DEL-7 | minor | Processor handler errors are logged and the event dropped (`return nil`) — DEL-1 mirrored at the processor stage. | `vendor/.../processor/processor.go:265-269` |
| DEL-8 | minor | `parseInt` ignores `Sscanf` errors — `HEALTH_PORT=8O88` silently becomes port 0. | `vendor/.../stellar/stellar.go:246-250` |

### PG — postgres-sink

| ID | Sev | Finding | Where |
| --- | --- | --- | --- |
| PG-1 | major | `ON CONFLICT (id)` doesn't arm secondary unique constraints (`network, seq, tx_index`); divergent replay (testnet reset, changed hash) = poison event that can never be written. | `components/postgres-sink/cmd/component/main.go:77-91,155` |
| PG-2 | major | Partial-column `DO UPDATE` (skips `ledger_hash`, `closed_at_unix`, `transaction_hash`, …) leaves internally inconsistent rows on divergent replay. | `main.go:66-69,82-86` |
| PG-3 | major | Empty `POSTGRES_DSN` silently falls back to `postgres://postgres:postgres@localhost`; no startup `PingContext`. Misconfigured deploy runs green while writing nothing. | `main.go:18-21` |
| PG-4 | major | No `SetMaxOpenConns`/`SetMaxIdleConns`/`SetConnMaxLifetime` (lib/pq default unlimited) — combined with DEL-2, backfills exhaust `max_connections`. | `main.go:22-26` |
| PG-5 | minor | `$6::jsonb` rejects ` ` — one NUL string in an extract row = poison batch forever. | `main.go:112` |
| PG-6 | minor | `panic(err)` in main; runtime `ensureSchema` needs DDL privileges and races across replicas. | `main.go:24,28` |
| PG-7 | minor | One round-trip per row; no prepared statements or COPY — poor throughput on operation-heavy ledgers. | `main.go:60-118` |
| PG-8 | major | `stellar_bronze_rows` has no secondary unique constraint and no per-ledger delete — combined with NM-1, replay-after-upgrade doubles every bronze table. | `main.go:170-177` |
| PG-9 | major | Zero test files — nothing exercises upsert/replay/rollback semantics. | component-wide |

### NM — pkg/normalize, pkg/ids, pkg/ledgerdecode

| ID | Sev | Finding | Where |
| --- | --- | --- | --- |
| NM-1 | major | Bronze row ID = sha256(network:seq:table:**index:rowJSON**) — content/position-hashed, so any stellar-extract upgrade re-IDs every row on replay. Violates the stated deterministic-ID contract. | `pkg/normalize/normalize.go:170-173` |
| NM-2 | major | `normalizeJSON` silently replaces a failed `json.Marshal` with `"{}"` — durable destruction of a row with zero signal. | `normalize.go:175-181` |
| NM-3 | minor | Missing envelope silently becomes `""`; LCM accessors panic on unsupported future versions → processor crash-loop on poison ledger. | `normalize.go:79-85,53-73` |
| NM-4 | minor | Dead code: `jsonString` duplicate; `pkg/ledgerdecode` has zero callers and zero tests. | `normalize.go:191-197`, `pkg/ledgerdecode/` |

### DL — ducklake-sink

| ID | Sev | Finding | Where |
| --- | --- | --- | --- |
| DL-1 | blocker | Reflection mapper NULL-fills unknown struct fields: **31 typed columns are silently always NULL** (23 on `transactions_row_v2` incl. `tx_envelope`/`tx_result`/`tx_meta`/soroban fees; 8 on `operations_row_v2`). History-loader parity fails invisibly; count-only tests pass CI. Affects both embedded and quack modes. | `components/ducklake-sink/cmd/component/main.go:617-620,775-780` |
| DL-2 | major | Typed-table deletes keyed on ledger sequence only, not network — a shared testnet+pubnet catalog self-corrupts on replay. | `main.go:561-574,362-373` |
| DL-3 | major | `CREATE TABLE IF NOT EXISTS` only — no migration mechanism; adding a column to `bronze_schema.sql` no-ops on existing catalogs, then every INSERT fails (and is dropped per DEL-1). | `main.go:224-233` |
| DL-4 | major | Remote (Quack) mode: `context.Background()` writes (hang forever holding the mutex), ATTACH once at init, no reconnect after server restart. | `main.go:467-469,189-210` |
| DL-5 | major | Remote atomicity rests on unverified beta multi-statement script semantics; compensating ROLLBACK is best-effort and may land on a new session. Zero tests on the remote path. | `main.go:340-465,457-461` |
| DL-6 | minor | `sqlValue` swallows `json.Marshal` errors → NULL cell, silent. | `main.go:646-651` |
| DL-7 | minor | Dead no-op `strings.ReplaceAll(sqlText, "bronze.", "bronze.")`; fragile full-line-only comment stripping. | `main.go:741` |
| DL-8 | minor | `INSTALL ducklake/quack` at startup needs extension-repo egress from a distroless container; verify the `quack` extension actually resolves from the intended repo. | `main.go:166-167,191` |
| DL-9 | major* | Remote mode inlines whole batch (incl. `payload_json`) into one unbounded SQL script — a large mainnet ledger (measured 17.3MB protobuf) approaches the 50MB gRPC cap and unknown Quack limits. *Upgraded from minor: quack mode is now the production write path.* *Mitigated 2026-07-23: `payload_json` and `bronze_rows` are no longer persisted; scripts carry typed rows only (~9 MiB for a heavy mainnet ledger, ~70% smaller).* *Resolved 2026-07-23: quack transport now stages typed rows as Parquet and ships a KB-scale script referencing `read_parquet`; script size no longer scales with ledger content, and the chaos harness gates both a staged-bytes floor and a script-size ceiling.* | `main.go:398-449` |
| DL-10 | minor | If the single pinned connection is recycled (`ErrBadConn`), the fresh session lacks `USE stellar_lake` — persistent failures, dropped per DEL-1. | `main.go:174` |

### QK — quack-ducklake-server

| ID | Sev | Finding | Where |
| --- | --- | --- | --- |
| QK-1 | blocker | Remote arbitrary SQL execution, plaintext by default (`QUACK_DISABLE_SSL=true`, `QUACK_ALLOW_OTHER_HOSTNAME=true`), single static token, no `enable_external_access=false`/`disabled_filesystems`/`lock_configuration`/memory/thread limits. Token holder can read host files or OOM the lake owner. | `components/quack-ducklake-server/cmd/component/main.go:43-44,69-87` |
| QK-2 | major | Pool never pinned (`SetMaxOpenConns(1)`) although `SET home_directory`/`USE` are connection-scoped — a recycle between init and `quack_serve` serves clients the wrong catalog. | `main.go:63-99` |
| QK-3 | major* | No health endpoint, metrics, or liveness surface — orchestrator cannot detect a wedged catalog owner. *Upgraded from minor: this process owns the production lake.* | component-wide |
| QK-4 | minor | SIGTERM handler installed after `CALL quack_serve`; if it blocks, shutdown never reaches `quack_stop`. | `main.go:97-103` |
| QK-5 | major | Zero test files. | component-wide |

### RS — ducklake-replica-sync

| ID | Sev | Finding | Where |
| --- | --- | --- | --- |
| RS-1 | blocker | No handling for expired snapshots: after routine `ducklake_expire_snapshots`, `table_changes` hard-errors (**empirically confirmed**), checkpoint never moves, and the first failing table halts all later tables in `SOURCE_TABLES`. First run against a mature catalog fails the same way. No full-resync fallback; recovery is manual SQL surgery. | `components/ducklake-replica-sync/cmd/component/main.go:240,348-382,132-137,231-234` |
| RS-2 | major | `loadCheckpoint` no-row branch never consults `rows.Err()` — a transient read failure is indistinguishable from "no checkpoint" → accidental full-history rescan/rebuild of a serving table. | `main.go:336-338` |
| RS-3 | major | Target created once from primary's then-current shape; copies are positional `INSERT … SELECT *` — schema drift breaks sync, or worse silently writes values into wrong columns. | `main.go:408-413,464-467` |
| RS-4 | major | Changed rows with NULL ledger are filtered out, then checkpoint advances — that row class can never reach the replica, silently, even after upstream fixes. | `main.go:373,244-250` |
| RS-5 | major | Primary write token embedded as literal in SQL sent to the target server; unredacted server error text (which can echo statements) persisted to `replica.sync_checkpoints.error_message` and logs. | `main.go:462-483,253` |
| RS-6 | minor | Quack-path rollbacks use unbounded `context.Background()` (hang risk); embedded rollback receives the already-canceled ctx (stillborn ROLLBACK). | `main.go:451,506,421-437,529-534` |
| RS-7 | minor | `ATTACH IF NOT EXISTS … AS replica_primary` pins a fixed alias — stale URI/token silently kept; two sync jobs sharing a server collide. | `main.go:461-471` |
| RS-8 | minor | `USE <catalog>; USE <schema>` mutates shared server session state; catalog-name-first resolution can be hijacked. Fully qualify instead. | `main.go:371-382` |
| RS-9 | minor | All distinct changed ledgers accumulate in one client-side slice — unbounded memory on large catch-ups. | `main.go:357-368` |
| RS-10 | minor | Relative default paths (`ducklake/serving.ducklake`) — each Nomad alloc gets a fresh cwd → silent empty-replica resync, host-local split-brain. | `main.go:75-76` |
| RS-11 | minor | `checkpoint > current` (primary catalog rebuilt) is a silent "already current" no-op — should be an error. | `main.go:235-238` |
| RS-12 | minor | README claims the rebuild script "commits the checkpoint"; code (correctly) checkpoints separately. Fix the doc. | `README.md:59-63` |

### IM — index-materializer

| ID | Sev | Finding | Where |
| --- | --- | --- | --- |
| IM-1 | major | `START_LEDGER=0` / `END_LEDGER=max` defaults + no chunking: env-template bug = full-table rebuild in one hours-long remote transaction. Range must be mandatory or chunked. | `components/index-materializer/cmd/component/main.go:41-42` |
| IM-2 | minor | `getenv` doesn't trim whitespace (replica-sync's does) — Vault-templated token with trailing newline fails auth confusingly. | `main.go:206-211` |
| IM-3 | minor | `sanitizeIdentifier` silently rewrites invalid identifiers (`my-lake`→`my_lake`) — error far from the misconfiguration. Shared with RS. | shared helper |

### JS / OPS — jsonl-sink, pipelines, cross-cutting

| ID | Sev | Finding | Where |
| --- | --- | --- | --- |
| JS-1 | minor | Blind append = not idempotent, not ordered. Fine as a debug tool — document it as such, excluded from the sink replay contract. | `components/jsonl-sink/cmd/component/main.go:48-64` |
| JS-2 | minor | Open/close per event, no fsync, torn-line risk; inverted `MkdirAll` error logic. | `main.go:52-62` |
| OPS-1 | major | `pipelines/local-postgres.yaml` and `fanout-postgres-ducklake.yaml` omit required `NETWORK_PASSPHRASE` (and `POSTGRES_DSN`) — processor `log.Fatalf`s; both crash-loop out of the box. | `pipelines/` |
| OPS-2 | minor | Sink and server default to the same relative catalog path — easy dual-attach misconfiguration with no guard. | both components |
| OPS-3 | major | Test posture: zero tests on postgres-sink, jsonl-sink, quack server, and the processor; happy-path-only tests on normalize. | repo-wide |

---

## Strategic decisions (made up front so cycles don't relitigate)

1. **Quack/replica mode is the production topology.** There is no current use
   case for the embedded DuckLake path; it stays as a dev/test fallback only.
   Quack client, Quack server, replica-sync, and index-materializer are the
   critical path and get hardened first. The corollary: **the beta Quack
   protocol must be proven by an integration harness in Cycle 1** — we no
   longer have the option of hiding behind embedded mode. If the harness
   exposes a protocol defect we cannot work around, that is an immediate
   ship/kill conversation, not a silent scope slip.
2. **Crash-on-write-failure is the delivery fix, not an ack protocol.**
   Replay is idempotent and the orchestrator restarts components. A sink that
   cannot write must exit non-zero. This converts silent loss into loud,
   self-healing restarts. A full ack/nack/redelivery protocol in flowctl-sdk
   is explicitly out of scope for this plan.
3. **The SDK is ours (`withObsrvr/flowctl-sdk`), so fix root causes there —
   but land component-side guards first.** Component-side changes ship this
   cycle without coordinating a release; SDK v0.1.3 (error propagation option,
   bounded worker pool, honest health) follows and lets us delete the guards.
4. **Bronze ID scheme change (NM-1) is a contract change** — it re-IDs
   existing rows. It ships together with the Postgres per-ledger
   delete-then-insert (PG-8) so both stores converge on "replace the ledger
   range" semantics and old IDs stop mattering. This rides in Cycle 3 with the
   rest of the Postgres path, which is not on the Quack critical path.
5. **One network per catalog/database is enforced, not assumed** (DL-2):
   startup records the network passphrase in a metadata table and refuses a
   mismatch.

---

## Cycle goals

Use these cards to build the actual Shape Up cycles. Each card names the
production capability the cycle must create, the implementation sequence to
shape around, the acceptance signals that prove it, and the explicit cut line.
The detailed scope lines later in the document remain the source of truth for
finding-level work; these goals are the cycle-builder inputs.

### Goal 1 — Quack write path is loss-loud and replayable

**Cycle:** Cycle 1 — "The Quack write path never loses a ledger silently"

**Production capability:** `ducklake-sink` in Quack mode can be trusted as the
primary write path. A ledger batch either commits completely with a durable
watermark or the process fails loudly enough for the orchestrator to restart and
replay it.

**Build the cycle around this sequence:**

1. **Make failure impossible to hide.** Sink write errors get bounded retry and
   then `os.Exit(1)`; handler concurrency is serialized or capped; normalize
   errors propagate instead of becoming empty rows.
2. **Make replay auditable.** Write `ingest_watermarks` in the same batch
   transaction/script as bronze and typed rows, and document the exact gap
   query operators use after restarts.
3. **Prove the Quack transaction boundary.** Add the chaos harness first enough
   to reproduce kill-mid-ingest, partial-write detection, restart, and replay
   convergence. Keep extending the harness as fixes land.
4. **Harden the remote session.** Add deadlines, reconnect/re-attach, session
   re-init, bad-connection recovery, and large-ledger script-size measurement.
5. **Remove silent schema corruption.** Replace reflection NULL-fill with
   column/field coverage validation and explicit mappings; enforce one network
   per catalog/database before any write can proceed.
6. **Make the Quack owner production-shaped.** Fail closed on plaintext unless
   explicitly opted into insecure mode, lock down DuckDB external access and
   resource limits, pin the server connection, move signal handling before
   serving, and expose minimal liveness for Nomad.

**Acceptance signals:**

- Chaos run: kill Quack server mid-ingest, restart sink, replay, and show no
  partial ledger plus exact parity with a never-failed run.
- Watermark run: a 1k-ledger ingest has an empty gap query after restart.
- Data-shape run: new ingests populate typed transaction columns that were
  previously silently NULL.
- Large-ledger run: a mainnet ledger with a batch payload of at least 15MB
  succeeds through Quack mode.
- Security/liveness run: plaintext startup fails without explicit insecure
  opt-out, lockdown SQL is applied, and Nomad can distinguish live from wedged.

**Cut line:** Stop at crash-and-replay, not ack/nack. Do not patch Quack
internals, build a metrics stack, fix the Postgres path, change bronze IDs, or
backfill historical NULL columns. If the chaos harness proves Quack cannot
provide the needed transaction boundary, stop the cycle for a ship/kill
decision.

### Goal 2 — Serving replica survives routine operations

**Cycle:** Cycle 2 — "The serving replica survives operations"

**Production capability:** The read replica can recover from normal DuckLake
maintenance, schema drift, and operator mistakes without silently wedging,
corrupting data, leaking the primary token, or rebuilding unbounded ranges.

**Build the cycle around this sequence:**

1. **Make snapshot expiry recoverable.** Detect missing snapshots, run a
   bounded chunked full resync for the affected table, checkpoint to current,
   and keep other tables isolated from that failure.
2. **Make checkpoint handling conservative.** Treat checkpoint read errors as
   hard failures; never convert uncertainty into full-history rebuilds.
3. **Make schema drift explicit.** Compare primary and target columns at
   startup and replicate by column name, not position. Fail with a concise diff
   when reconcile-or-fail cannot safely proceed.
4. **Remove token exposure.** Redact the primary token from generated SQL,
   logs, persisted checkpoint errors, and server-returned error text before it
   can cross the replica boundary.
5. **Protect checkpoint correctness.** Detect NULL-ledger change rows and fail
   or emit an operator-visible signal before advancing the table checkpoint.
6. **Bound rebuild blast radius.** Require explicit materializer ranges, chunk
   large ranges into bounded transactions, and reject relative embedded target
   paths that would create a new empty replica after reschedule.

**Acceptance signals:**

- Snapshot-expiry run: expire snapshots past a checkpoint, then show
  replica-sync performs a chunked full resync, checkpoints, and continues later
  tables.
- Schema-drift run: add or remove a primary column and show the reconcile /
  fail output names the exact drift.
- Secret-safety run: grep logs and `replica.sync_checkpoints.error_message`
  and show no primary token material.
- Materializer safety run: omit `START_LEDGER`/`END_LEDGER` and show the
  materializer refuses to start.
- Path-safety run: configure a relative embedded replica path and show startup
  fails before any catalog is created.

**Cut line:** Do not build generic CDC, point-in-time-consistent serving views,
multi-primary sync, or automatic schema evolution. The contract is bounded
per-batch convergence plus clear reconcile-or-fail behavior.

### Goal 3 — Make replay safe across extractor and schema upgrades

**Cycle:** Cycle 3 — "Replay correctness across upgrades"

**Production capability:** Replaying a ledger after an extractor or schema
change deterministically replaces that ledger's derived rows in Postgres and
DuckLake instead of duplicating, poisoning, or leaving inconsistent state.

**Build the cycle around this sequence:**

1. **Make Postgres replay match the batch contract.** Replace bronze rows by
   `(network, ledger, table)` before insert, move bronze IDs to the v2
   position-based scheme, and update `docs/event-contracts.md`.
2. **Make typed replay convergent.** Align typed-table conflict targets with
   natural keys and update every non-key column so divergent replay refreshes
   rows instead of poison-pilling or mixing old and new content.
3. **Make Postgres startup fail closed.** Empty `POSTGRES_DSN` is fatal,
   `PingContext` verifies connectivity, connection pool limits are bounded, and
   schema setup is tested.
4. **Make DuckLake schema changes apply once.** Introduce ordered migrations and
   a `schema_migrations` table; convert `bronze_schema.sql` into the first
   migration so existing catalogs can evolve.
5. **Make shipped configs runnable.** Fix broken pipeline YAMLs and add a CI /
   script check that validates required env wiring for every shipped pipeline.
6. **Prove replay failure modes.** Add tests for replay-no-duplicate,
   changed-hash replay, rollback on mid-batch failure, poison-batch surfacing,
   and checkpoint package deletion or atomic persistence.

**Acceptance signals:**

- Replay run: ingest ledger N, simulate an extractor/schema change, replay N,
  and show row counts remain stable while updated content is visible.
- Constraint run: replay changed hashes and show no secondary-constraint poison
  errors.
- Migration run: restart an existing DuckLake catalog with a new migration and
  show the schema applies exactly once.
- Startup/config run: missing `POSTGRES_DSN` and missing pipeline env fail
  before any write starts.
- Test run: `postgres-sink` replay/rollback/poison cases and migration tests
  pass locally.

**Cut line:** Do not redesign `LedgerBatch`, add new event types, build the
row-fanout processor, or turn this into a Postgres performance project. Batch
inserts are allowed only if they fit inside the one-week appetite.

### Cool-down goals

Use each cool-down to remove drag created by the previous cycle, not to start
new feature work:

1. Delete or wire obvious dead code that was touched by the cycle.
2. Land documentation fixes discovered during implementation.
3. Update architecture and component READMEs so they describe the shipped
   Quack/replica topology, not the old embedded-first posture.
4. Clear small config-parsing and wording issues that do not affect the main
   cycle demo.

### Production-gate goal

After Cycles 1 and 2, the production gate is a single release decision: Quack
primary plus serving replica may ship only if the chaos, large-ledger,
watermark, typed-column, TLS/lockdown, liveness, snapshot-expiry, schema-drift,
and token-redaction demos all pass. Cycle 3 remains mandatory before using the
Postgres path in production or replaying history after an extractor upgrade.

---

## Cycle 1 — "The Quack write path never loses a ledger silently"

**Appetite:** 2 weeks.
**Problem:** The production write path (`ducklake-sink` in quack mode →
`quack-ducklake-server`) silently drops every batch during a server restart,
hangs forever on a wedged connection, ships 31 always-NULL typed columns, and
the server itself is plaintext arbitrary SQL with no resource limits and no
liveness surface. Its transactional semantics under failure have never been
tested.
**Done looks like:** An integration harness (script in `scripts/`, runnable in
the nix shell) that: kills the Quack server for 60s mid-ingest → sink retries,
then exits non-zero; supervisor restarts it; replay heals; final tables match
a never-failed run exactly, with no partial ledgers and an empty gap query.
Ingest of a large mainnet ledger (≥15MB batch) succeeds through quack mode.
`SELECT count(*) FROM bronze.transactions_row_v2 WHERE tx_envelope IS NULL`
returns 0 on new ingests. The server refuses plaintext startup without an
explicit opt-out flag.

### Scope line

```
MUST HAVE ══════════════
  DEL-1  Sink-side: WriteBatch error => bounded retry (3 attempts, backoff),
         then log + os.Exit(1). Applies to both quack and embedded modes.
  DEL-2  Sink-side worker cap: serialize (or small bounded pool) in the
         handler; per-ledger delete-then-insert makes ordering safe.
  DEL-3  Watermark: `ingest_watermarks` table (network, ledger_seq,
         written_at) written inside the same batch transaction/script.
         Gap-check SQL documented in the sink README.
  DL-4   Remote-mode timeouts (context deadlines on every remote call) +
         automatic re-ATTACH and session re-init on connection loss.
  DL-5   Quack integration harness: kill-mid-script => no partial ledger
         persists; server restart => sink reconnects, replay heals;
         atomicity of the BEGIN..COMMIT script verified empirically.
  DL-1   Replace NULL-fill with startup validation: every spec'd column must
         resolve to a struct field or the component refuses to start. Add the
         missing field mappings (or drop columns deliberately, documented).
         Test asserts full column<->field coverage against stellar-extract.
  DL-2   Network scoping: startup network check (decision #5); typed deletes
         scoped or one-network-per-catalog enforced.
  QK-1   Server lockdown: TLS on by default (fail closed without a cert
         unless QUACK_INSECURE=true is set explicitly);
         enable_external_access=false, disabled_filesystems,
         lock_configuration, memory_limit, thread cap in init SQL.
  QK-2   SetMaxOpenConns(1) on the server; QK-4 signal handling moved ahead
         of quack_serve (verify its blocking behavior while in there).
  QK-3   Minimal HTTP liveness endpoint on the server (catalog attach +
         trivial query check) so Nomad can detect a wedged lake owner.
  NM-2   normalizeJSON returns the error; processor fails the ledger loudly.
  QK-5   First server tests: init SQL assembly, config validation, TLS
         fail-closed behavior.

NICE TO HAVE ───────────
  DL-9   Measure real script sizes in the harness; chunk oversized remote
         scripts (bronze-row inserts split across statements) if a large
         ledger gets anywhere near limits.
  DEL-5  Serve a real HTTP health listener on HEALTH_PORT in the sink,
         reporting last-write success/age.
  DL-10  Detect ErrBadConn/session reset and re-run session init (USE).
  DL-8   Pre-bundle/pin DuckDB extensions in the image; verify `quack`
         extension provenance; document air-gapped install.
  DEL-6  Component-side flowctl registration retry with backoff.

COULD HAVE ─────────────
  OPS-2  Startup guard: refuse embedded attach when a server owns the
         catalog (metadata marker).
  DL-6   sqlValue propagates marshal errors.
  Per-client tokens/ACLs on the server.
```

**Rabbit holes (do not enter):**
- Do not fork or patch Quack itself; work around protocol limits or escalate
  to the ship/kill conversation (decision #1).
- Do not design an ack/nack redelivery protocol or dead-letter queue;
  crash-and-replay is the model.
- Do not build a connection-pooling proxy in front of the server.
- Do not backfill DL-1's historical NULL columns this cycle — fix forward;
  backfill is a replay task once Cycle 1 ships.

**No-gos:** Postgres-path fixes (Cycle 3). Bronze ID change (Cycle 3).
Metrics stack beyond the liveness endpoints.

---

## Cycle 2 — "The serving replica survives operations"

**Appetite:** 2 weeks.
**Problem:** The read side of the production topology is the replica. Routine
DuckLake maintenance (`ducklake_expire_snapshots`) permanently wedges
replica-sync (RS-1, empirically confirmed); schema drift breaks or silently
corrupts the replica (RS-3); a transient read error triggers accidental full
rebuilds of a serving table (RS-2); the primary's write token leaks to the
target server and persisted error logs (RS-5); a missing env var makes the
materializer rebuild an entire index in one hours-long transaction (IM-1).
**Done looks like:** Expire snapshots past a checkpoint → next run detects it,
logs loudly, performs a bounded chunked full resync of that table, checkpoints,
and continues to later tables. Add a column on the primary → sync reconciles
or fails with an actionable error naming the column. Grep of the target
checkpoint table and logs finds no token material. Running the materializer
with no range env vars refuses to start.

### Scope line

```
MUST HAVE ══════════════
  RS-1   Missing-snapshot detection => per-table full resync fallback
         (ledger-range chunked), then checkpoint to current. Per-table error
         isolation: one failing table no longer halts the rest (aggregate
         exit status instead).
  RS-2   Consult rows.Err() in the no-row branch; transient read error =
         run failure, never "no checkpoint".
  RS-3   Column-explicit or BY NAME inserts; startup schema reconciliation
         (compare primary vs target column lists; fail with a diff).
  RS-5   Redact token in all outbound SQL logging and in persisted
         error_message (extend the existing initStepName redaction to the
         batch path); never store raw server error text unredacted.
  IM-1   Make START_LEDGER/END_LEDGER mandatory (fail if unset) and chunk
         large ranges into bounded per-chunk transactions.
  RS-4   Count NULL-ledger change rows; fail (or loudly warn + metric)
         instead of silently advancing the checkpoint past them.
  RS-10  Require absolute paths for the embedded target (fail on relative) —
         a Nomad reschedule must never silently start an empty replica.

NICE TO HAVE ───────────
  RS-6   Bounded rollback contexts (10s, detached from the failed ctx).
  RS-7   Detach/re-attach or verify alias URI on each run instead of
         IF NOT EXISTS.
  RS-8   Fully qualify table_changes; drop session USE on the shared server.
  RS-11  checkpoint > current = hard error.
  IM-2/3 getenv trims; sanitizeIdentifier rejects instead of rewriting.

COULD HAVE ─────────────
  RS-9   Stream/batch the changed-ledger discovery query.
  RS-12  README atomicity wording fix (do anyway if touching the file).
```

**Rabbit holes:** Do not build a generic CDC framework. Do not attempt
point-in-time-consistent replica views — document per-batch convergence as
the contract instead.

**No-gos:** Multi-primary sync; automatic schema evolution beyond
reconcile-or-fail.

---

## Cycle 3 — "Replay correctness across upgrades" (Postgres path + contracts)

**Appetite:** 1 week. **Not on the Quack critical path** — schedule when the
Postgres replacement (rebuild-plan Goal 3) becomes active, or as the next
cycle after 2 if capacity allows.
**Problem:** Replaying history after a stellar-extract upgrade duplicates
every Postgres bronze row (NM-1 + PG-8); divergent replays poison-pill on
secondary unique constraints (PG-1/PG-2); existing DuckLake catalogs can't
receive schema changes (DL-3).
**Done looks like:** Ingest ledger N, bump extractor version, replay N →
row counts unchanged in both stores, updated content visible, no constraint
errors. A new column added to `bronze_schema.sql` applies to an existing
catalog on restart.

### Scope line

```
MUST HAVE ══════════════
  NM-1 + PG-8  Bronze replay = per-(network, ledger, table) delete-then-insert
         in postgres-sink, mirroring ducklake-sink. Bronze ID simplified to
         position-based (network:seq:table:ordinal), documented as v2 in
         docs/event-contracts.md. Old content-hash IDs become irrelevant.
  PG-1   Align upsert conflict targets with the real natural keys
         (network, seq, tx_index) and make id a regular column, or add
         ON CONFLICT handling for the secondary constraints. Poison-replay
         test proves a changed-hash replay succeeds.
  PG-2   DO UPDATE sets every non-key column.
  PG-3   Fail fast: empty POSTGRES_DSN is fatal; PingContext at startup.
  PG-4   SetMaxOpenConns/SetMaxIdleConns/SetConnMaxLifetime.
  DL-3   Versioned migrations for the DuckLake catalog: schema_migrations
         table + ordered SQL files; bronze_schema.sql becomes migration 001.
  OPS-1  Fix both broken pipeline YAMLs; add a CI check that every shipped
         pipeline passes flowctl config validation with required env present.
  PG-9   Tests: writeBatch happy path, replay-no-dup, mid-batch failure
         rollback, poison-batch surfacing (dockerized Postgres or sqlmock).
  DEL-4  pkg/checkpoints: atomic temp+rename+fsync — or delete the package
         (zero importers) if Cycle 1 watermarks cover the need.

NICE TO HAVE ───────────
  PG-5   Strip/escape   before ::jsonb (with a test).
  PG-6   Replace panic with fatal log; document DDL privileges; advisory
         lock around ensureSchema.
  PG-7   Batch inserts (single multi-row INSERT or COPY) for bronze rows.
  JS-1/2 Document jsonl-sink as debug-only; fix MkdirAll logic; atomic line
         writes.
  NM-4   Delete dead code (jsonString, pkg/ledgerdecode or wire it in).

COULD HAVE ─────────────
  Historical backfill runbook: replay range to repopulate DL-1's NULL columns.
```

**Rabbit holes:** Do not redesign the LedgerBatch proto. Do not add COPY +
partitioning + a perf suite in one go — batching only if it fits the week.

**No-gos:** New event types; row-fanout processor.

---

## Cool-downs

Per Shape Up: 2–3 days between cycles. Use them for: deleting dead code
(NM-4), doc fixes (RS-12, JS-1), the DEL-8/IM-2 class of trivia, and updating
`docs/event-contracts.md`, `docs/quack-ducklake-architecture.md` (embedded is
now the fallback, not the caveat-holder), and component READMEs to match
shipped behavior. No new feature work in cool-downs.

## Production gate (after Cycles 1–2)

The Quack/replica topology may ship to production when:

- [x] Quack chaos harness passes: kill server mid-ingest → no partial
      ledgers, sink restart, replay heals, gap query empty (Cycle 1 demo).
- [x] Large-ledger (≥15MB batch) ingest through quack mode succeeds.
- [x] `tx_envelope`/`tx_result`/soroban columns non-NULL on new ingests.
- [ ] Server refuses plaintext startup without explicit insecure opt-out;
      external access, filesystems, memory, and threads locked down.
- [ ] Server liveness endpoint wired into the Nomad job spec.
- [x] Snapshot-expiry resync demo passes (Cycle 2 demo; live local two-Quack
      gate 2026-08-03).
- [x] Schema-drift reconcile-or-fail demo passes (Cycle 2 demo; live local
      two-Quack gate 2026-08-03).
- [x] No token material in replica checkpoint table or logs (live local
      two-Quack gate 2026-08-03).
- [x] Watermark gap query returns empty after a 1k-ledger backfill (ingest-RPC
      gate rerun 2026-08-03).
- [x] flowctl-sdk upgrade plan written (even if v0.1.3 not yet shipped).

Local production-gate evidence and remaining run commands are captured in
`docs/production-gate-runbook.md`. The SDK follow-up plan is captured in
`docs/flowctl-sdk-upgrade-plan.md`.

The Postgres path additionally requires Cycle 3 before any production use or
history replay after an extractor upgrade.

## Explicitly deferred (recorded, not forgotten)

- Full ack/redelivery protocol in flowctl-sdk.
- DEL-7 processor-side redelivery (crash-on-error may suffice; evaluate after
  Cycle 1 in the SDK work).
- Metrics/observability stack (Prometheus counters for batches written,
  retries, watermark lag) — natural follow-on after Cycle 1's health work.
- PG-7 COPY-based bulk loading if batching proves insufficient.
- Row-fanout / projection processors from the original rebuild plan.
- Per-client tokens/ACLs on the Quack server.
