# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is obsrvr-stellar-components, a comprehensive collection of Apache Arrow-native components for processing Stellar blockchain data with flowctl. The project uses Apache Arrow for 10x performance improvement with zero-copy data operations and columnar processing.

### Architecture

The system consists of three main components in a pipeline:

```
Stellar Network → stellar-arrow-source → ttp-arrow-processor → arrow-analytics-sink
                  (XDR → Arrow)          (Arrow Compute)     (Parquet/JSON/WS)
```

- **stellar-arrow-source**: Arrow-native Stellar ledger data source supporting RPC and data lake ingestion
- **ttp-arrow-processor**: Token Transfer Protocol event extraction processor using Arrow compute  
- **arrow-analytics-sink**: Multi-format output sink supporting Parquet, JSON, CSV, and real-time WebSocket streaming

## Build System & Commands

### Primary Build Commands

Use the Makefile for all build operations:

```bash
# Build all components
make build

# Build specific components  
make stellar-arrow-source
make ttp-arrow-processor
make arrow-analytics-sink

# Build schemas first (required dependency)
make build-schemas
```

### Testing Commands

```bash
# Run all tests
make test

# Run unit tests only
make test-unit

# Run integration tests
make test-integration

# Run with coverage reports
make test-coverage

# Run benchmarks
make benchmark
```

### Code Quality Commands

```bash
# Run linters (golangci-lint)
make lint

# Format code
make fmt

# Run go vet
make vet

# Security scan
make security-scan
```

### Development Workflow

```bash
# Set up development environment
make dev-setup

# Start development mode
make dev

# Run RPC pipeline for testing
make run-rpc-pipeline

# Run analytics pipeline  
make run-analytics-pipeline

# Generate test data
make test-data
```

### Nix-based Development

This project uses Nix flakes for reproducible development:

```bash
# Enter development shell (includes all dependencies)
nix develop

# Build all components with Nix
build-all

# Run all tests with Nix
test-all

# Build Docker images
docker-build

# Generate vendor files for Nix builds
generate-vendor
```

## Component Structure

Each component follows this structure:
- `components/{component-name}/src/` - Go source code
- `components/{component-name}/component.yaml` - Component configuration
- `components/{component-name}/examples/` - Usage examples

### Key Files by Component

**stellar-arrow-source**:
- `main.go` - Entry point and service setup
- `service.go` - Core service implementation  
- `rpc_client.go` - Stellar RPC client implementation
- `datalake_reader.go` - Data lake storage reader
- `xdr_processor.go` - XDR data processing and validation
- `flight_server.go` - Arrow Flight server implementation
- `security.go` - TLS, authentication, authorization (Phase 3)
- `resilience.go` - Circuit breakers, retries, rate limiting (Phase 3)

**ttp-arrow-processor**:
- `main.go` - Entry point and service setup
- `service.go` - Core TTP event processing
- `flight_server.go` - Arrow Flight server implementation
- `compute_engine.go` - Vectorized compute operations (Phase 3)

**arrow-analytics-sink**:
- `main.go` - Entry point and service setup
- `service.go` - Core analytics sink implementation
- `writers.go` - Multi-format output writers
- `parquet_writer.go` - Native Arrow-to-Parquet conversion (Phase 3)
- `realtime.go` - WebSocket streaming
- `websocket_manager.go` - Production WebSocket management (Phase 3)
- `data_lifecycle.go` - Data retention and lifecycle management (Phase 3)

**schemas**:
- `stellar_schemas.go` - Arrow schema definitions
- `schema_utils.go` - Schema utilities and migration

## Key Dependencies

- **Go 1.23+** - Primary language
- **Apache Arrow Go v17.0.0** - Columnar data processing
- **Stellar Go SDK v0.15.0** - Stellar blockchain integration
- **gRPC** - Arrow Flight protocol
- **Prometheus** - Metrics collection
- **Viper** - Configuration management
- **Zerolog** - Structured logging

## Configuration

### Environment Variables

Key environment variables used across components:

```bash
# Stellar Network Configuration  
STELLAR_NETWORK=testnet|mainnet
STELLAR_RPC_URL=https://soroban-testnet.stellar.org
NETWORK_PASSPHRASE="Test SDF Network ; September 2015"

# Data Storage
DATA_DIR=./data
LOG_LEVEL=info

# Performance Tuning
BATCH_SIZE=1000
BUFFER_SIZE=10000
PROCESSOR_THREADS=4

# Arrow Configuration
ARROW_BATCH_SIZE=1000
ARROW_MEMORY_POOL=default
```

### Component Ports

- stellar-arrow-source: 8815 (Arrow Flight), 8088 (Health)
- ttp-arrow-processor: 8816 (Arrow Flight), 8088 (Health)  
- arrow-analytics-sink: 8817 (Arrow Flight), 8080 (WebSocket), 8081 (REST), 8088 (Health)

## Pipeline Templates

The project includes pre-configured pipeline templates in `bundle.yaml`:

- **rpc-pipeline**: Real-time TTP events from Stellar RPC
- **datalake-pipeline**: Historical TTP events from archived data
- **analytics-pipeline**: Real-time analytics with multiple outputs

## Development Environment Setup

### Nix (Recommended)

```bash
# Enter development shell
nix develop

# Environment automatically includes:
# - Go 1.23, Apache Arrow C++, all build dependencies
# - Development tools (golangci-lint, gosec, etc.)
# - Container tools (Docker, kubectl)
# - Cloud tools (AWS CLI, GCP SDK)
```

### Manual Setup

```bash
# Install dependencies
make deps

# Set up development environment
make dev-setup

# Required environment setup for Arrow
export CGO_ENABLED=1
export PKG_CONFIG_PATH="${ARROW_CPP_PATH}/lib/pkgconfig:$PKG_CONFIG_PATH"
export CGO_CFLAGS="-I${ARROW_CPP_PATH}/include"
export CGO_LDFLAGS="-L${ARROW_CPP_PATH}/lib -larrow -larrow_flight"
```

## Testing Strategy

### Test Types

1. **Unit Tests**: Component-level functionality (`make test-unit`)
2. **Integration Tests**: End-to-end pipeline testing (`make test-integration`)  
3. **Docker Tests**: Container integration testing (`make test-docker`)
4. **Benchmarks**: Performance validation (`make benchmark`)

### Test Data

- Use `make test-data` to generate synthetic Stellar ledger data
- Integration tests use both synthetic and real Stellar network data
- Test data stored in `test/data/` directory

## Phase 3 Production Features

Phase 3 added critical production capabilities:

### Security (security.go)
- TLS 1.2+ encryption with modern cipher suites
- API key, JWT, and mutual TLS authentication
- Role-based authorization (admin, reader, processor roles)
- CORS protection and security headers

### Resilience (resilience.go)  
- Circuit breakers with configurable thresholds
- Exponential backoff retry logic with jitter
- Bulkhead pattern for resource isolation
- Token bucket rate limiting

### Enhanced Processing
- **XDR Processing**: Comprehensive Stellar XDR validation and error recovery
- **Parquet Writer**: Native Arrow-to-Parquet conversion with compression
- **Compute Engine**: Vectorized operations with pre-compiled expressions
- **WebSocket Management**: Production-grade connection pooling and management
- **Data Lifecycle**: Automated retention, archival, and cleanup policies

## Implementation Status

According to PHASE_3_HANDOFF.md, the following Phase 3 features are **completed**:
- ✅ Enhanced Stellar XDR processing with validation
- ✅ Production Parquet writer with Arrow integration  
- ✅ Comprehensive security layer (TLS, auth, authz)
- ✅ Resilience patterns (circuit breakers, retries, rate limiting)
- ✅ Real Stellar data integration and testing
- ✅ Arrow compute integration for vectorized processing
- ✅ Enhanced WebSocket connection management
- ✅ Data retention and lifecycle management

## Known Implementation Shortcuts

See IMPLEMENTATION_SHORTCUTS.md for detailed tracking of remaining shortcuts. Critical items completed in Phase 3, but some optimization opportunities remain:

- Advanced schema evolution (basic compatibility only)
- Performance optimizations (connection pooling, adaptive batching)
- Advanced analytics features (statistical analysis, ML integration)

## Troubleshooting

### Common Issues

1. **Arrow CGO Build Issues**: Ensure Arrow C++ libraries are properly installed and CGO environment variables are set
2. **Component Communication**: Verify Arrow Flight ports (8815, 8816, 8817) are available
3. **Stellar Network Access**: Check RPC endpoint URLs and network connectivity
4. **Memory Usage**: Large ledger batches can consume significant memory; adjust BATCH_SIZE

### Debugging

- Use `LOG_LEVEL=debug` for detailed processing information
- Health endpoints: `http://localhost:8088/health` 
- Metrics endpoints: `http://localhost:9090/metrics`
- Component logs in `logs/` directory

## Monitoring

### Metrics

Key Prometheus metrics tracked:
- `ledgers_processed_total{source_type, status}`
- `processing_duration_seconds{source_type, operation}`  
- `files_written_total{format}`
- `circuit_breaker_state{name}`

### Health Checks

Each component exposes health endpoints:
- `/health` - Overall component health
- `/health/live` - Liveness probe
- `/health/ready` - Readiness probe

## Performance Characteristics

- **Source**: ~50 ledgers/sec (RPC), ~200 ledgers/sec (DataLake)
- **Processor**: ~1000 TTP events/sec transformation
- **Sink**: ~5000 events/sec Parquet write, ~10000 events/sec WebSocket streaming

Resource requirements:
- Minimum: 2 cores, 1GB RAM, 10GB storage
- Recommended: 8 cores, 8GB RAM, 100GB SSD