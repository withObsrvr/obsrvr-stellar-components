variable "quack_image" {
  type    = string
  default = "withobsrvr/quack-ducklake-server:latest"
}

job "obsrvr-stellar-ducklake-primary" {
  datacenters = ["dc1"]
  type        = "service"

  group "quack-primary" {
    count = 1

    network {
      port "quack" {
        to = 9494
      }

      port "health" {
        to = 8088
      }

      port "ingest" {
        to = 9495
      }
    }

    volume "ducklake-primary" {
      type      = "host"
      source    = "ducklake-primary"
      read_only = false
    }

    service {
      name     = "quack-ducklake-primary"
      provider = "nomad"
      port     = "quack"

      check {
        name     = "quack-ducklake-health"
        type     = "http"
        port     = "health"
        path     = "/healthz"
        interval = "10s"
        timeout  = "2s"
      }
    }

    service {
      name     = "quack-ducklake-ingest"
      provider = "nomad"
      port     = "ingest"
    }

    service {
      name     = "quack-ducklake-metrics"
      provider = "nomad"
      port     = "health"

      tags = ["prometheus"]

      check {
        name     = "quack-ducklake-metrics"
        type     = "http"
        port     = "health"
        path     = "/metrics"
        interval = "30s"
        timeout  = "5s"
      }
    }

    task "server" {
      driver = "docker"

      config {
        image = var.quack_image
        ports = ["quack", "health", "ingest"]
      }

      volume_mount {
        volume      = "ducklake-primary"
        destination = "/var/lib/obsrvr/ducklake"
        read_only   = false
      }

      env {
        DUCKLAKE_ATTACH_NAME  = "stellar_lake"
        DUCKLAKE_CATALOG_PATH = "/var/lib/obsrvr/ducklake/stellar.ducklake"
        DUCKLAKE_DATA_PATH    = "/var/lib/obsrvr/ducklake/data"
        QUACK_URI             = "quack:0.0.0.0:${NOMAD_PORT_quack}"
        QUACK_HEALTH_ADDR     = "0.0.0.0:${NOMAD_PORT_health}"
        QUACK_MEMORY_LIMIT    = "8GB"
        QUACK_DUCKDB_THREADS  = "4"
        # DuckDB's 16 MiB default auto-checkpoints the catalog on ingest
        # commits, creating multi-second tail spikes under sustained load.
        # This defers, but does not eliminate, checkpoint work; replace with an
        # explicit off-hot-path checkpoint policy before claiming a hard SLO.
        DUCKDB_CHECKPOINT_THRESHOLD = "1GB"
        # Coordinated manual checkpoints are disabled for the telemetry-only
        # rollout. Enabling requires CHECKPOINT_ADMIN_TOKEN from a Nomad
        # template; never place that token directly in this jobspec.
        CHECKPOINT_ENABLED            = "false"
        CHECKPOINT_CONTROLLER_ENABLED = "false"
        CHECKPOINT_TIMEOUT            = "30s"
        CHECKPOINT_SOFT_WAL_BYTES     = "67108864"
        CHECKPOINT_HARD_WAL_BYTES     = "536870912"
        CHECKPOINT_POLL_INTERVAL      = "1s"
        CHECKPOINT_IDLE_DURATION      = "2s"
        QUACK_LOCK_CONFIGURATION      = "true"
        QUACK_INSECURE                = "true"
        QUACK_DISABLE_SSL             = "true"
        QUACK_ALLOW_OTHER_HOSTNAME    = "true"
        QUACK_ENABLE_EXTERNAL_ACCESS  = "true"
        QUACK_DISABLED_FILESYSTEMS    = "none"

        # BronzeIngestService: sub-400ms commits need the 256 inline limit,
        # which produces ~7 small parquet files per ledger — the
        # ducklake-maintenance job's interval (2m) is paired with this value.
        INGEST_PORT               = "${NOMAD_PORT_ingest}"
        DUCKLAKE_INLINE_ROW_LIMIT = "256"
      }

      template {
        data        = <<EOF
{{ with nomadVar "nomad/jobs/obsrvr-stellar-ducklake" }}
QUACK_TOKEN={{ .quack_token }}
{{ end }}
EOF
        destination = "${NOMAD_SECRETS_DIR}/quack.env"
        env         = true
      }

      resources {
        cpu    = 2000
        memory = 8192
      }
    }
  }
}
