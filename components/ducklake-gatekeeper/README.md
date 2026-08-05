# ducklake-gatekeeper

`ducklake-gatekeeper` is the promotion boundary between proposed transformations
and published DuckLake tables. It reads a strict manifest, pins every source
read to the declared DuckLake snapshot, builds output in a generated private
schema, and promotes only after all deterministic gates pass.

The first slice implements the non-agent trust boundary. A model may author a
manifest later, but it cannot decide whether its own output is correct.

## Gates

1. **Reproducibility:** build the full range twice and compare with `EXCEPT ALL`
   in both directions.
2. **Reconciliation:** require every manifest-declared scalar invariant to
   return `true`.
3. **Replay:** choose a proposal-hash-seeded ledger subrange, rebuild it twice,
   and require byte-equivalent rows.
4. **Confinement:** accept only one read-only `SELECT`/`WITH` transformation and
   execute it as `CREATE TABLE AS` inside a generated staging schema.

After all gates pass, one remote transaction creates the target when needed,
replaces rows matching `target.replace_keys`, inserts the candidate rows, and
records a receipt in `governance.promotions`. A rejected proposal never enters
that transaction, so the published table freezes at its last verified version.

## Manifest contract

Ledger ranges use `(start_exclusive, end_inclusive]`. Transformations must use
the `{{source}}`, `{{start_ledger}}`, and `{{end_ledger}}` placeholders. The
gatekeeper expands `{{source}}` to an `AT (VERSION => snapshot_id)` relation;
the proposal cannot silently move to the latest catalog state while gates run.

Invariants are scalar boolean queries and must reference `{{candidate}}`. They
may also use the source and ledger placeholders.

See [`../../manifests/gatekeeper/asset-daily-volume.yaml`](../../manifests/gatekeeper/asset-daily-volume.yaml)
for the first Silver proposal.

## Run

Set `source.snapshot_id` in the manifest to the snapshot containing the desired
Bronze ledger range, then run:

```bash
make build

QUACK_TOKEN=dev_secret \
QUACK_URI=quack:127.0.0.1:9494 \
QUACK_DISABLE_SSL=true \
bin/ducklake-gatekeeper \
  --manifest manifests/gatekeeper/asset-daily-volume.yaml \
  --report gatekeeper-report.json
```

Configuration:

- `QUACK_URI`, default `quack:127.0.0.1:9494`
- `QUACK_TOKEN`, required
- `QUACK_REMOTE_DB`, default `remote_lake`
- `QUACK_DISABLE_SSL`, default `true`
- `--manifest`, required
- `--report`, default stdout
- `--timeout`, default `10m`

This component deliberately has no provider-specific model loop. That loop is
the next demo layer and will have only `run_sql` and `submit_proposal` tools.
