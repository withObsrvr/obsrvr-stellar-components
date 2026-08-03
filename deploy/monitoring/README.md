# Quack/DuckLake monitoring

- `quack-ducklake-alerts.yml`: initial Prometheus rule group.
- `grafana/quack-ducklake-production-gate.json`: importable Grafana dashboard.

Validate the rule file with the Nix CLI package:

```bash
nix shell nixpkgs#prometheus.cli -c \
  promtool check rules deploy/monitoring/quack-ducklake-alerts.yml
```

The recovery/parity gates selected 64 MiB soft and 512 MiB hard WAL candidates
for the disabled controller/cadence experiment. The 10-minute checkpoint age
and four-second idle budget remain experimental. These values are not a
production controller policy or hard latency-SLO proof.

Every rule is scoped to
`nomad_service="quack-ducklake-metrics"`. The Prometheus Nomad discovery
configuration must copy `__meta_nomad_service` into the retained
`nomad_service` target label before loading these rules. This prevents the hard
400ms alerts from evaluating saturated backfill or unrelated targets with a
different latency contract.

The Latitude Prometheus packs discover Nomad services and filter a service-name
drop list; they do not keep only the `prometheus` tag. The incremental infra
change explicitly drops `quack-ducklake-primary` and
`quack-ducklake-ingest`, maps the retained service name to `nomad_service`, and
leaves only `quack-ducklake-metrics` eligible on the health port. A future
tag-based allowlist can replace this service-specific exception after all
intended metrics services are consistently tagged.

The checkpoint collectors are intentionally present before the checkpoint
controller. Their series remain zero until Workstreams 2–3 execute coordinated
checkpoints. The rules pass Prometheus 3.13.1 `promtool check rules`; loading
and evaluating them against the retained `nomad_service` label remains a
deployment gate.
