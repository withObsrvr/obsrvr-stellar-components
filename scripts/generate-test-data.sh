#!/usr/bin/env bash
set -euo pipefail

# Test data generation script for obsrvr-stellar-components

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

log "Generating test data for component testing..."

TEST_DATA_DIR="${PROJECT_ROOT}/test/data"
LEDGER_DIR="${TEST_DATA_DIR}/ledgers"

# Create directories
mkdir -p "${LEDGER_DIR}"
mkdir -p "${TEST_DATA_DIR}/output"/{parquet,json,csv}

# Generate mock ledger XDR files
log "Generating mock ledger XDR files..."

for i in $(seq 1 100); do
    ledger_seq=$(printf "%08d" $i)
    filename="${LEDGER_DIR}/${ledger_seq}.xdr"
    
    # Generate mock XDR data (base64 encoded for realism)
    # In a real implementation, this would be actual Stellar XDR
    mock_data=$(cat <<EOF | base64 -w 0
{
  "ledger_sequence": ${i},
  "close_time": "$(date -d "+${i} seconds" --iso-8601=seconds)",
  "protocol_version": 19,
  "transactions": [
    {
      "hash": "$(openssl rand -hex 32)",
      "operations": [
        {
          "type": "payment",
          "from": "GABC123DEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOP",
          "to": "GDEF456GHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOPQR",
          "asset": "native",
          "amount": "$((RANDOM % 10000 + 1))0000000"
        }
      ]
    }
  ]
}
EOF
)
    
    echo "${mock_data}" > "${filename}"
done

log "Generated 100 mock ledger files in ${LEDGER_DIR}"

# Generate test configuration files
log "Generating test configuration files..."

# Development config for stellar-arrow-source
cat > "${TEST_DATA_DIR}/stellar-source-test.yaml" <<EOF
source_type: datalake
storage_backend: filesystem
storage_path: ${LEDGER_DIR}
start_ledger: 1
end_ledger: 10
batch_size: 5
buffer_size: 100
flight_port: 18815
health_port: 18088
log_level: debug
EOF

# Development config for ttp-arrow-processor
cat > "${TEST_DATA_DIR}/ttp-processor-test.yaml" <<EOF
source_endpoint: localhost:18815
event_types:
  - payment
  - path_payment_strict_receive
  - path_payment_strict_send
processor_threads: 2
batch_size: 10
buffer_size: 100
flight_port: 18816
health_port: 18089
log_level: debug
EOF

# Development config for arrow-analytics-sink
cat > "${TEST_DATA_DIR}/analytics-sink-test.yaml" <<EOF
processor_endpoint: localhost:18816
output_formats:
  - parquet
  - json
  - csv
parquet_path: ${TEST_DATA_DIR}/output/parquet
json_path: ${TEST_DATA_DIR}/output/json
csv_path: ${TEST_DATA_DIR}/output/csv
buffer_size: 100
writer_threads: 2
flight_port: 18817
health_port: 18090
log_level: debug
EOF

log "Generated test configuration files in ${TEST_DATA_DIR}"

# Create test environment variables file
cat > "${TEST_DATA_DIR}/test.env" <<EOF
# Test environment variables for obsrvr-stellar-components

# Common
LOG_LEVEL=debug
METRICS_ENABLED=true

# Stellar Arrow Source
STELLAR_ARROW_SOURCE_PORT=18815
STELLAR_ARROW_SOURCE_HEALTH_PORT=18088
STELLAR_ARROW_SOURCE_SOURCE_TYPE=datalake
STELLAR_ARROW_SOURCE_STORAGE_BACKEND=filesystem
STELLAR_ARROW_SOURCE_STORAGE_PATH=${LEDGER_DIR}

# TTP Arrow Processor  
TTP_ARROW_PROCESSOR_PORT=18816
TTP_ARROW_PROCESSOR_HEALTH_PORT=18089
TTP_ARROW_PROCESSOR_SOURCE_ENDPOINT=localhost:18815

# Arrow Analytics Sink
ARROW_ANALYTICS_SINK_PORT=18817
ARROW_ANALYTICS_SINK_HEALTH_PORT=18090
ARROW_ANALYTICS_SINK_WEBSOCKET_PORT=18080
ARROW_ANALYTICS_SINK_DATA_PATH=${TEST_DATA_DIR}/output
ARROW_ANALYTICS_SINK_PROCESSOR_ENDPOINT=localhost:18816
EOF

log "Generated test environment file: ${TEST_DATA_DIR}/test.env"

# Generate Docker Compose file for testing
cat > "${TEST_DATA_DIR}/docker-compose.test.yml" <<EOF
version: '3.8'

services:
  stellar-arrow-source:
    build:
      context: ${PROJECT_ROOT}
      dockerfile: components/stellar-arrow-source/Dockerfile
    ports:
      - "18815:8815"
      - "18088:8088"
    environment:
      - LOG_LEVEL=debug
      - STELLAR_ARROW_SOURCE_SOURCE_TYPE=datalake
      - STELLAR_ARROW_SOURCE_STORAGE_BACKEND=filesystem
      - STELLAR_ARROW_SOURCE_STORAGE_PATH=/data/ledgers
    volumes:
      - ${LEDGER_DIR}:/data/ledgers:ro
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8088/health"]
      interval: 10s
      timeout: 5s
      retries: 3

  ttp-arrow-processor:
    build:
      context: ${PROJECT_ROOT}
      dockerfile: components/ttp-arrow-processor/Dockerfile
    ports:
      - "18816:8816"
      - "18089:8088"
    environment:
      - LOG_LEVEL=debug
      - TTP_ARROW_PROCESSOR_SOURCE_ENDPOINT=stellar-arrow-source:8815
    depends_on:
      stellar-arrow-source:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8088/health"]
      interval: 10s
      timeout: 5s
      retries: 3

  arrow-analytics-sink:
    build:
      context: ${PROJECT_ROOT}
      dockerfile: components/arrow-analytics-sink/Dockerfile
    ports:
      - "18817:8817"
      - "18090:8088"
      - "18080:8080"
    environment:
      - LOG_LEVEL=debug
      - ARROW_ANALYTICS_SINK_PROCESSOR_ENDPOINT=ttp-arrow-processor:8816
      - ARROW_ANALYTICS_SINK_DATA_PATH=/data/output
    volumes:
      - ${TEST_DATA_DIR}/output:/data/output
    depends_on:
      ttp-arrow-processor:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8088/health"]
      interval: 10s
      timeout: 5s
      retries: 3

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "19090:9090"
    volumes:
      - ${PROJECT_ROOT}/monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'
      - '--web.enable-lifecycle'

volumes:
  prometheus_data:
EOF

log "Generated Docker Compose test file: ${TEST_DATA_DIR}/docker-compose.test.yml"

# Create test pipeline configuration
cat > "${TEST_DATA_DIR}/test-pipeline.yaml" <<EOF
# Test pipeline configuration for obsrvr-stellar-components
apiVersion: flowctl.io/v1
kind: Pipeline
metadata:
  name: obsrvr-stellar-test-pipeline
  description: Test pipeline for component validation

spec:
  components:
    - name: stellar-source
      type: stellar-arrow-source
      config:
        source_type: datalake
        storage_backend: filesystem
        storage_path: ${LEDGER_DIR}
        start_ledger: 1
        end_ledger: 10
        batch_size: 5

    - name: ttp-processor
      type: ttp-arrow-processor
      config:
        source_endpoint: stellar-source:8815
        event_types:
          - payment
          - path_payment_strict_receive
        processor_threads: 2

    - name: analytics-sink
      type: arrow-analytics-sink
      config:
        processor_endpoint: ttp-processor:8816
        output_formats:
          - parquet
          - json
        parquet_path: /data/parquet
        json_path: /data/json

  connections:
    - from: stellar-source
      to: ttp-processor
      protocol: arrow-flight
    - from: ttp-processor
      to: analytics-sink
      protocol: arrow-flight
EOF

log "Generated test pipeline configuration: ${TEST_DATA_DIR}/test-pipeline.yaml"

log "Test data generation complete!"
log "Test data directory: ${TEST_DATA_DIR}"
log "Ledger files: ${LEDGER_DIR} (100 files)"
log "Configuration files: ${TEST_DATA_DIR}/*.yaml"
log "Environment file: ${TEST_DATA_DIR}/test.env"