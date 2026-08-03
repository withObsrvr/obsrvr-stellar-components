# Quack/DuckLake monitoring

- `quack-ducklake-alerts.yml`: initial Prometheus rule group.
- `grafana/quack-ducklake-production-gate.json`: importable Grafana dashboard.

Validate the rule file with the Nix CLI package:

```bash
nix shell nixpkgs#prometheus.cli -c \
  promtool check rules deploy/monitoring/quack-ducklake-alerts.yml
```

The 64 MiB WAL soft limit, 512 MiB hard limit, 10-minute checkpoint age, and
four-second idle budget are experimental candidates from the production-gate
plan. Change them only after the WAL-size gate produces retained evidence.

The ingest latency rules currently select the metric across all matching scrape
targets. Before enabling notifications, add the target labels used by the
production Prometheus deployment and scope hard 400ms alerts to the live-ingest
target; saturated backfill has a different latency contract.

The Latitude Prometheus packs discover Nomad services and filter a service-name
drop list; they do not keep only the `prometheus` tag. The incremental infra
change explicitly drops `quack-ducklake-primary` and
`quack-ducklake-ingest`, leaving only `quack-ducklake-metrics` eligible on the
health port. A future tag-based allowlist can replace this service-specific
exception after all intended metrics services are consistently tagged.

The checkpoint collectors are intentionally present before the checkpoint
controller. Their series remain zero until Workstreams 2–3 execute coordinated
checkpoints. The rules pass Prometheus 3.13.1 `promtool check rules`; loading
and evaluating them with production target labels remains a deployment gate.
