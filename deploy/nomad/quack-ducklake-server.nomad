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
        DUCKLAKE_ATTACH_NAME         = "stellar_lake"
        DUCKLAKE_CATALOG_PATH        = "/var/lib/obsrvr/ducklake/stellar.ducklake"
        DUCKLAKE_DATA_PATH           = "/var/lib/obsrvr/ducklake/data"
        QUACK_URI                    = "quack:0.0.0.0:${NOMAD_PORT_quack}"
        QUACK_HEALTH_ADDR            = "0.0.0.0:${NOMAD_PORT_health}"
        QUACK_MEMORY_LIMIT           = "8GB"
        QUACK_DUCKDB_THREADS         = "4"
        QUACK_LOCK_CONFIGURATION     = "true"
        QUACK_INSECURE               = "true"
        QUACK_DISABLE_SSL            = "true"
        QUACK_ENABLE_EXTERNAL_ACCESS = "true"
        QUACK_DISABLED_FILESYSTEMS   = "none"

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
