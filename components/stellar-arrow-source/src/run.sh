#!/usr/bin/env bash

# Set up Arrow library path
export LD_LIBRARY_PATH="/nix/store/bi90gd0fbr5icl03scxylsdv4ba9sc8d-arrow-cpp-20.0.0/lib:$LD_LIBRARY_PATH"

# Set up GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS=$HOME/.config/gcloud/application_default_credentials.json

# FlowCtl Configuration (optional)
export ENABLE_FLOWCTL=${ENABLE_FLOWCTL:-false}
export FLOWCTL_ENDPOINT=${FLOWCTL_ENDPOINT:-localhost:8080}
export FLOWCTL_HEARTBEAT_INTERVAL=${FLOWCTL_HEARTBEAT_INTERVAL:-10s}

# Component-specific FlowCtl overrides (optional)
export STELLAR_ARROW_SOURCE_ENABLE_FLOWCTL=${STELLAR_ARROW_SOURCE_ENABLE_FLOWCTL:-}
export STELLAR_ARROW_SOURCE_FLOWCTL_ENDPOINT=${STELLAR_ARROW_SOURCE_FLOWCTL_ENDPOINT:-}
export STELLAR_ARROW_SOURCE_FLOWCTL_HEARTBEAT_INTERVAL=${STELLAR_ARROW_SOURCE_FLOWCTL_HEARTBEAT_INTERVAL:-}

# Advanced FlowCtl Features (Phase 4)
export CONNECTION_POOL_MAX_CONNECTIONS=${CONNECTION_POOL_MAX_CONNECTIONS:-10}
export CONNECTION_POOL_MIN_CONNECTIONS=${CONNECTION_POOL_MIN_CONNECTIONS:-2}
export CONNECTION_POOL_IDLE_TIMEOUT=${CONNECTION_POOL_IDLE_TIMEOUT:-5m}
export MONITORING_INTERVAL=${MONITORING_INTERVAL:-5s}

echo "Starting stellar-arrow-source with Data Lake (GCS) configuration..."
echo "Configuration: config.yaml"
echo "Arrow Flight server: localhost:8815"
echo "Health endpoint: http://localhost:8088/health"
echo "FlowCtl Integration: $ENABLE_FLOWCTL"
if [ "$ENABLE_FLOWCTL" = "true" ]; then
    echo "FlowCtl Endpoint: $FLOWCTL_ENDPOINT"
    echo "Heartbeat Interval: $FLOWCTL_HEARTBEAT_INTERVAL"
    echo "Connection Pool: ${CONNECTION_POOL_MIN_CONNECTIONS}-${CONNECTION_POOL_MAX_CONNECTIONS} connections"
fi
echo ""

# Run the component
exec ./stellar-arrow-source