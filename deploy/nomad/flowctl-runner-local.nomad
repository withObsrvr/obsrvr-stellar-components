variable "repo_dir" {
  type    = string
  default = "/home/tillman/Documents/obsrvr-stellar-components"
}

variable "flowctl_bin" {
  type    = string
  default = "/home/tillman/Documents/flowctl/bin/flowctl"
}

variable "start_ledger" {
  type    = string
  default = "62080000"
}

variable "end_ledger" {
  type    = string
  default = "62080000"
}

job "obsrvr-flowctl-runner-local" {
  datacenters = ["dc1"]
  type        = "service"

  meta {
    runtime  = "flowctl"
    pipeline = "local-archive-ducklake"
  }

  group "pipeline" {
    count = 1

    network {
      port "control" {}
      port "source_grpc" {}
      port "source_health" {}
      port "processor_grpc" {}
      port "processor_health" {}
      port "sink_grpc" {}
      port "sink_health" {}
    }

    service {
      name     = "flowctl-runner-local"
      provider = "nomad"
      port     = "control"

      tags = [
        "runtime=flowctl",
        "pipeline=local-archive-ducklake",
      ]

      check {
        name     = "flowctl-control-plane-tcp"
        type     = "tcp"
        port     = "control"
        interval = "10s"
        timeout  = "2s"
      }
    }

    service {
      name     = "flowctl-raw-ledger-source-local"
      provider = "nomad"
      port     = "source_health"

      tags = [
        "component=raw-ledger-source",
        "component_type=source",
      ]

      check {
        name     = "raw-ledger-source-health-tcp"
        type     = "tcp"
        port     = "source_health"
        interval = "10s"
        timeout  = "2s"
      }
    }

    service {
      name     = "flowctl-stellar-ledger-processor-local"
      provider = "nomad"
      port     = "processor_health"

      tags = [
        "component=stellar-ledger-processor",
        "component_type=processor",
      ]

      check {
        name     = "stellar-ledger-processor-health-tcp"
        type     = "tcp"
        port     = "processor_health"
        interval = "10s"
        timeout  = "2s"
      }
    }

    service {
      name     = "flowctl-ducklake-sink-local"
      provider = "nomad"
      port     = "sink_health"

      tags = [
        "component=ducklake-sink",
        "component_type=sink",
        "prometheus",
      ]

      check {
        name     = "ducklake-sink-health-tcp"
        type     = "tcp"
        port     = "sink_health"
        interval = "10s"
        timeout  = "2s"
      }
    }

    task "flowctl-run" {
      driver = "raw_exec"

      config {
        command = var.flowctl_bin
        args = [
          "run",
          "--orchestrator",
          "process",
          "--control-plane-address",
          "0.0.0.0",
          "--control-plane-port",
          "${NOMAD_PORT_control}",
          "--db-path",
          "${NOMAD_ALLOC_DIR}/flowctl.db",
          "--log-dir",
          "${NOMAD_ALLOC_DIR}/logs",
          "${NOMAD_TASK_DIR}/pipeline.yaml",
        ]
      }

      env {
        HOME = "${NOMAD_ALLOC_DIR}"
      }

      template {
        destination = "${NOMAD_TASK_DIR}/pipeline.yaml"
        change_mode = "restart"
        data        = <<EOF
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: nomad-local-archive-ducklake
  description: Nomad local test pipeline from public Stellar archive to embedded DuckLake.

spec:
  driver: process

  sources:
    - id: raw-ledger-source
      type: source
      command: ["${var.repo_dir}/bin/raw-ledger-source"]
      env:
        FLOWCTL_COMPONENT_ID: "raw-ledger-source"
        BACKEND_TYPE: "ARCHIVE"
        ARCHIVE_STORAGE_TYPE: "S3"
        ARCHIVE_BUCKET_NAME: "aws-public-blockchain"
        ARCHIVE_PATH: "v1.1/stellar/ledgers/pubnet"
        AWS_REGION: "us-east-2"
        LEDGERS_PER_FILE: "1"
        FILES_PER_PARTITION: "64000"
        NETWORK_PASSPHRASE: "Public Global Stellar Network ; September 2015"
        START_LEDGER: "${var.start_ledger}"
        END_LEDGER: "${var.end_ledger}"
        GRPC_PORT: "${NOMAD_PORT_source_grpc}"
        HEALTH_PORT: "${NOMAD_PORT_source_health}"
        FLOWCTL_ENDPOINT: "127.0.0.1:${NOMAD_PORT_control}"

  processors:
    - id: stellar-ledger-processor
      type: processor
      command: ["${var.repo_dir}/bin/stellar-ledger-processor"]
      inputs: ["raw-ledger-source"]
      env:
        COMPONENT_ID: "stellar-ledger-processor"
        NETWORK_PASSPHRASE: "Public Global Stellar Network ; September 2015"
        PORT: ":${NOMAD_PORT_processor_grpc}"
        HEALTH_PORT: "${NOMAD_PORT_processor_health}"
        ENABLE_FLOWCTL: "true"
        FLOWCTL_ENDPOINT: "127.0.0.1:${NOMAD_PORT_control}"

  sinks:
    - id: ducklake-sink
      type: sink
      command: ["${var.repo_dir}/bin/ducklake-sink"]
      inputs: ["stellar-ledger-processor"]
      env:
        COMPONENT_ID: "ducklake-sink"
        DUCKLAKE_CATALOG_PATH: "${NOMAD_ALLOC_DIR}/ducklake/stellar.ducklake"
        DUCKLAKE_DATA_PATH: "${NOMAD_ALLOC_DIR}/ducklake/data"
        PORT: ":${NOMAD_PORT_sink_grpc}"
        HEALTH_PORT: "${NOMAD_PORT_sink_health}"
        ENABLE_FLOWCTL: "true"
        FLOWCTL_ENDPOINT: "127.0.0.1:${NOMAD_PORT_control}"
EOF
      }

      resources {
        cpu    = 2000
        memory = 4096
      }
    }
  }
}
