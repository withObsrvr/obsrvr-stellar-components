variable "maintenance_image" {
  type    = string
  default = "withobsrvr/ducklake-maintenance:latest"
}

job "obsrvr-stellar-ducklake-maintenance" {
  datacenters = ["dc1"]
  type        = "service"

  group "maintenance" {
    count = 1

    network {
      port "health" {
        to = 8090
      }
    }

    service {
      name     = "ducklake-maintenance"
      provider = "nomad"
      port     = "health"

      check {
        name     = "maintenance-health"
        type     = "http"
        port     = "health"
        path     = "/healthz"
        interval = "30s"
        timeout  = "2s"
      }
    }

    task "maintenance" {
      driver = "docker"

      config {
        image = var.maintenance_image
        ports = ["health"]
      }

      env {
        DUCKLAKE_ATTACH_NAME = "stellar_lake"
        QUACK_DISABLE_SSL    = "true"
        HEALTH_PORT          = "${NOMAD_PORT_health}"

        # 2m flush/merge interval is paired with the ingest path's
        # DUCKLAKE_INLINE_ROW_LIMIT=256 (~7 small parquet files per ledger);
        # widen both together, never independently. SNAPSHOT_RETENTION must
        # exceed ducklake-replica-sync's worst-case checkpoint lag.
        MAINTENANCE_INTERVAL = "2m"
        SNAPSHOT_RETENTION   = "48h"
        MERGE_ADJACENT_FILES = "true"
      }

      template {
        data        = <<EOF
{{ range nomadService "quack-ducklake-primary" }}
QUACK_URI=quack:{{ .Address }}:{{ .Port }}
{{ end }}
{{ with nomadVar "nomad/jobs/obsrvr-stellar-ducklake" }}
QUACK_TOKEN={{ .quack_token }}
{{ end }}
EOF
        destination = "${NOMAD_SECRETS_DIR}/maintenance.env"
        env         = true
      }

      resources {
        cpu    = 500
        memory = 1024
      }
    }
  }
}
