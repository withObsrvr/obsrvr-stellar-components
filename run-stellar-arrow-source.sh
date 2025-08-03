#!/usr/bin/env bash

# Set up Arrow library path for Nix environment
export LD_LIBRARY_PATH="/nix/store/bi90gd0fbr5icl03scxylsdv4ba9sc8d-arrow-cpp-20.0.0/lib:$LD_LIBRARY_PATH"

# Configure for Data Lake mode (GCS) - similar to your stellar-live-source-datalake
export SOURCE_TYPE=datalake
export STORAGE_BACKEND=gcs
export BUCKET_NAME="obsrvr-stellar-ledger-data-testnet-data"
export STORAGE_PATH="landing/ledgers/testnet"
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"

# GCP Authentication
export GOOGLE_APPLICATION_CREDENTIALS=$HOME/.config/gcloud/application_default_credentials.json

# Data lake configuration 
export LEDGERS_PER_FILE=1
export FILES_PER_PARTITION=64000

# Server ports
export FLIGHT_PORT=8815  # Arrow Flight server
export HEALTH_PORT=8088

# Processing configuration
export START_LEDGER=0     # 0 = latest
export END_LEDGER=0       # 0 = continuous
export BATCH_SIZE=1000
export BUFFER_SIZE=10000
export CONCURRENT_READERS=4

# Arrow configuration
export ARROW_MEMORY_POOL=default
export COMPRESSION=none

# Monitoring
export METRICS_ENABLED=true
export LOG_LEVEL=info

echo "Starting stellar-arrow-source with Data Lake (GCS) configuration..."
echo "Arrow Flight server will be available on port 8815"
echo "Health check available on port 8088"
echo ""

# Run the component
cd /home/tillman/Documents/obsrvr-stellar-components/components/stellar-arrow-source/src
exec ./stellar-arrow-source