#!/bin/bash

# Set up Arrow library path for Nix environment
export LD_LIBRARY_PATH="/nix/store/bi90gd0fbr5icl03scxylsdv4ba9sc8d-arrow-cpp-20.0.0/lib:$LD_LIBRARY_PATH"

# Configure for RPC mode (real-time from Stellar testnet)
export SOURCE_TYPE=rpc
export BACKEND_TYPE=rpc
export RPC_ENDPOINT="https://soroban-testnet.stellar.org"
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"

# Server ports
export FLIGHT_PORT=8815  # Arrow Flight server
export HEALTH_PORT=8088

# Processing configuration - start from latest available ledger
export START_LEDGER=658805  # Current oldest available on RPC (adjust as needed)
export END_LEDGER=0         # 0 = continuous
export BATCH_SIZE=500
export BUFFER_SIZE=10000

# Arrow configuration
export ARROW_MEMORY_POOL=default
export COMPRESSION=none

# Monitoring
export METRICS_ENABLED=true
export LOG_LEVEL=info

echo "Starting stellar-arrow-source with RPC configuration..."
echo "Will start from ledger $START_LEDGER (adjust if needed based on RPC availability)"
echo "Arrow Flight server will be available on port 8815"
echo "Health check available on port 8088"
echo ""

# Run the component
cd /home/tillman/Documents/obsrvr-stellar-components/components/stellar-arrow-source/src
exec ./stellar-arrow-source