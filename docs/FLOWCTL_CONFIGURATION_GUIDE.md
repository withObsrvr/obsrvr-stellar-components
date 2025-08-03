# FlowCtl Configuration Guide
## obsrvr-stellar-components

**Document Version**: 1.0  
**Date**: August 3, 2025  
**Phase**: 5 (Configuration & Environment Variables)

## Overview

This guide provides comprehensive documentation for configuring flowctl integration across all obsrvr-stellar-components. FlowCtl provides centralized service discovery, health monitoring, flow control, and metrics collection for the entire pipeline.

## Quick Start

### Basic FlowCtl Integration

```bash
# Enable flowctl for all components
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=localhost:8080

# Start the pipeline
./run-pipeline.sh flowctl
```

### Development Mode

```bash
# Fast monitoring and smaller connection pools
./run-pipeline.sh development
```

### Production Mode

```bash
# Optimized for production workloads
export FLOWCTL_ENDPOINT=flowctl-prod.example.com:8080
./run-pipeline.sh production
```

## Environment Variables Reference

### Core FlowCtl Variables

These variables control basic flowctl integration across all components:

| Variable | Description | Default | Type | Required |
|----------|-------------|---------|------|----------|
| `ENABLE_FLOWCTL` | Master switch for flowctl integration | `false` | boolean | No |
| `FLOWCTL_ENDPOINT` | Control plane endpoint address | `localhost:8080` | string | No |
| `FLOWCTL_HEARTBEAT_INTERVAL` | Health reporting frequency | `10s` | duration | No |

**Example:**
```bash
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=flowctl.example.com:8080
export FLOWCTL_HEARTBEAT_INTERVAL=15s
```

### Component-Specific Overrides

Each component supports individual configuration overrides:

#### stellar-arrow-source
```bash
export STELLAR_ARROW_SOURCE_ENABLE_FLOWCTL=true
export STELLAR_ARROW_SOURCE_FLOWCTL_ENDPOINT=source-flowctl.example.com:8080
export STELLAR_ARROW_SOURCE_FLOWCTL_HEARTBEAT_INTERVAL=5s
```

#### ttp-arrow-processor
```bash
export TTP_ARROW_PROCESSOR_ENABLE_FLOWCTL=true
export TTP_ARROW_PROCESSOR_FLOWCTL_ENDPOINT=processor-flowctl.example.com:8080
export TTP_ARROW_PROCESSOR_FLOWCTL_HEARTBEAT_INTERVAL=8s
```

#### arrow-analytics-sink
```bash
export ARROW_ANALYTICS_SINK_ENABLE_FLOWCTL=true
export ARROW_ANALYTICS_SINK_FLOWCTL_ENDPOINT=sink-flowctl.example.com:8080
export ARROW_ANALYTICS_SINK_FLOWCTL_HEARTBEAT_INTERVAL=12s
```

### Advanced Features (Phase 4)

These variables control advanced flowctl features like connection pooling and enhanced monitoring:

| Variable | Description | Default | Min | Max | Type |
|----------|-------------|---------|-----|-----|------|
| `CONNECTION_POOL_MAX_CONNECTIONS` | Maximum connections per endpoint | `10` | `1` | `50` | integer |
| `CONNECTION_POOL_MIN_CONNECTIONS` | Minimum connections to maintain | `2` | `1` | `10` | integer |
| `CONNECTION_POOL_IDLE_TIMEOUT` | Idle connection cleanup timeout | `5m` | - | - | duration |
| `MONITORING_INTERVAL` | Pipeline monitoring frequency | `5s` | - | - | duration |

**Example Advanced Configuration:**
```bash
# High-throughput production environment
export CONNECTION_POOL_MAX_CONNECTIONS=25
export CONNECTION_POOL_MIN_CONNECTIONS=8
export CONNECTION_POOL_IDLE_TIMEOUT=15m
export MONITORING_INTERVAL=3s
```

## Configuration Profiles

### Profile: Local Development

**Use Case:** Local development and testing without flowctl
```bash
export ENABLE_FLOWCTL=false
# All other flowctl variables ignored
```

**Characteristics:**
- No external dependencies
- Standalone component operation
- Direct component-to-component connections
- Minimal overhead

### Profile: FlowCtl Basic

**Use Case:** Basic flowctl integration with default settings
```bash
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=localhost:8080
export FLOWCTL_HEARTBEAT_INTERVAL=10s
```

**Characteristics:**
- Service registration and discovery
- Basic health monitoring
- Default connection pooling (2-10 connections)
- 10-second heartbeat intervals

### Profile: Development

**Use Case:** Active development with fast feedback
```bash
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=localhost:8080
export FLOWCTL_HEARTBEAT_INTERVAL=5s
export CONNECTION_POOL_MAX_CONNECTIONS=5
export CONNECTION_POOL_MIN_CONNECTIONS=1
export CONNECTION_POOL_IDLE_TIMEOUT=2m
export MONITORING_INTERVAL=3s
```

**Characteristics:**
- Fast monitoring updates (3-5 seconds)
- Smaller connection pools (1-5 connections)
- Quick idle timeouts (2 minutes)
- Ideal for debugging and testing

### Profile: Production

**Use Case:** Production deployments with optimized performance
```bash
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=flowctl-prod.example.com:8080
export FLOWCTL_HEARTBEAT_INTERVAL=15s
export CONNECTION_POOL_MAX_CONNECTIONS=20
export CONNECTION_POOL_MIN_CONNECTIONS=5
export CONNECTION_POOL_IDLE_TIMEOUT=10m
export MONITORING_INTERVAL=5s
```

**Characteristics:**
- Conservative heartbeat intervals (15 seconds)
- Large connection pools (5-20 connections)
- Long idle timeouts (10 minutes)
- Optimized for stability and performance

## Pipeline Runner Usage

The `run-pipeline.sh` script provides convenient configuration management:

### Starting the Pipeline

```bash
# Local standalone mode
./run-pipeline.sh local

# Basic flowctl integration
./run-pipeline.sh flowctl

# Development mode
./run-pipeline.sh development

# Production mode  
./run-pipeline.sh production
```

### Pipeline Management

```bash
# Check pipeline status
./run-pipeline.sh status

# Health check all components
./run-pipeline.sh health

# Stop all components
./run-pipeline.sh stop

# Follow logs
./run-pipeline.sh logs                    # All components
./run-pipeline.sh logs stellar-arrow-source # Specific component
```

### Pipeline Runner Configuration

The pipeline runner automatically configures environment variables based on the selected mode:

| Mode | FlowCtl | Heartbeat | Pool Size | Idle Timeout | Monitoring |
|------|---------|-----------|-----------|--------------|------------|
| `local` | disabled | - | - | - | - |
| `flowctl` | enabled | 10s | 2-10 | 5m | 5s |
| `development` | enabled | 5s | 1-5 | 2m | 3s |
| `production` | enabled | 15s | 5-20 | 10m | 5s |

## Component-Specific Configuration

### stellar-arrow-source

**Standard Variables:**
```bash
export STELLAR_ARROW_SOURCE_PORT=8815
export STELLAR_ARROW_SOURCE_HEALTH_PORT=8088
export STELLAR_ARROW_SOURCE_LOG_LEVEL=info
export STELLAR_ARROW_SOURCE_BATCH_SIZE=1000
```

**FlowCtl Integration:**
- **Service Type:** `SOURCE`
- **Output Event Types:** `["stellar_ledger_service.StellarLedger"]`
- **Metrics:** Ledgers processed, bytes processed, processing latency
- **Connection Pools:** Not applicable (source component)

**Example Configuration:**
```bash
# Source with flowctl integration
export ENABLE_FLOWCTL=true
export STELLAR_ARROW_SOURCE_ENABLE_FLOWCTL=true
export STELLAR_ARROW_SOURCE_FLOWCTL_HEARTBEAT_INTERVAL=8s
```

### ttp-arrow-processor

**Standard Variables:**
```bash
export TTP_ARROW_PROCESSOR_PORT=8816
export TTP_ARROW_PROCESSOR_HEALTH_PORT=8088
export TTP_ARROW_PROCESSOR_SOURCE_ENDPOINT=localhost:8815
export TTP_ARROW_PROCESSOR_THREADS=4
export TTP_ARROW_PROCESSOR_LOG_LEVEL=info
```

**FlowCtl Integration:**
- **Service Type:** `PROCESSOR`
- **Input Event Types:** `["stellar_ledger_service.StellarLedger"]`
- **Output Event Types:** `["ttp_event_service.TTPEvent"]`
- **Metrics:** Events extracted, processing rate, inflight count, throttle rate
- **Connection Pools:** To stellar-arrow-source (8815)

**Example Configuration:**
```bash
# Processor with enhanced connection pooling
export ENABLE_FLOWCTL=true
export TTP_ARROW_PROCESSOR_ENABLE_FLOWCTL=true
export CONNECTION_POOL_MAX_CONNECTIONS=15
export CONNECTION_POOL_MIN_CONNECTIONS=3
```

### arrow-analytics-sink

**Standard Variables:**
```bash
export ARROW_ANALYTICS_SINK_PORT=8817
export ARROW_ANALYTICS_SINK_HEALTH_PORT=8088
export ARROW_ANALYTICS_SINK_WEBSOCKET_PORT=8080
export ARROW_ANALYTICS_SINK_DATA_PATH=/data
export ARROW_ANALYTICS_SINK_LOG_LEVEL=info
```

**FlowCtl Integration:**
- **Service Type:** `SINK`
- **Input Event Types:** `["ttp_event_service.TTPEvent"]`
- **Output Event Types:** `[]` (terminal sink)
- **Metrics:** Events consumed, files written, WebSocket connections
- **Connection Pools:** To ttp-arrow-processor (8816)

**Example Configuration:**
```bash
# Sink with real-time monitoring
export ENABLE_FLOWCTL=true
export ARROW_ANALYTICS_SINK_ENABLE_FLOWCTL=true
export MONITORING_INTERVAL=2s
```

## Advanced Configuration Scenarios

### Multi-Instance Deployment

For horizontal scaling, use component-specific overrides:

```bash
# Instance 1
export TTP_ARROW_PROCESSOR_ENABLE_FLOWCTL=true
export TTP_ARROW_PROCESSOR_FLOWCTL_ENDPOINT=flowctl-cluster-1.example.com:8080

# Instance 2  
export TTP_ARROW_PROCESSOR_ENABLE_FLOWCTL=true
export TTP_ARROW_PROCESSOR_FLOWCTL_ENDPOINT=flowctl-cluster-2.example.com:8080
```

### High-Availability Setup

```bash
# Primary instance
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=flowctl-primary.example.com:8080
export CONNECTION_POOL_MAX_CONNECTIONS=30
export CONNECTION_POOL_MIN_CONNECTIONS=10

# Backup instance
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=flowctl-backup.example.com:8080
export CONNECTION_POOL_MAX_CONNECTIONS=15
export CONNECTION_POOL_MIN_CONNECTIONS=5
```

### Performance Tuning

#### High-Throughput Configuration
```bash
export ENABLE_FLOWCTL=true
export FLOWCTL_HEARTBEAT_INTERVAL=30s
export CONNECTION_POOL_MAX_CONNECTIONS=50
export CONNECTION_POOL_MIN_CONNECTIONS=15
export CONNECTION_POOL_IDLE_TIMEOUT=30m
export MONITORING_INTERVAL=10s
```

#### Low-Latency Configuration
```bash
export ENABLE_FLOWCTL=true
export FLOWCTL_HEARTBEAT_INTERVAL=3s
export CONNECTION_POOL_MAX_CONNECTIONS=8
export CONNECTION_POOL_MIN_CONNECTIONS=4
export CONNECTION_POOL_IDLE_TIMEOUT=1m
export MONITORING_INTERVAL=1s
```

#### Resource-Constrained Configuration
```bash
export ENABLE_FLOWCTL=true
export FLOWCTL_HEARTBEAT_INTERVAL=60s
export CONNECTION_POOL_MAX_CONNECTIONS=3
export CONNECTION_POOL_MIN_CONNECTIONS=1
export CONNECTION_POOL_IDLE_TIMEOUT=15m
export MONITORING_INTERVAL=30s
```

## Validation and Patterns

### Environment Variable Validation

FlowCtl configuration includes built-in validation patterns:

| Variable Type | Pattern | Example |
|---------------|---------|---------|
| Endpoint | `^[a-zA-Z0-9.-]+:[0-9]+$` | `localhost:8080` |
| Duration | `^[0-9]+(s\|m\|h)$` | `10s`, `5m`, `2h` |
| Boolean | `true\|false` | `true` |
| Integer | Range validation | `1-50` for connections |

### Common Validation Errors

**Invalid Endpoint Format:**
```bash
# ❌ Invalid
export FLOWCTL_ENDPOINT=localhost

# ✅ Valid
export FLOWCTL_ENDPOINT=localhost:8080
```

**Invalid Duration Format:**
```bash
# ❌ Invalid
export FLOWCTL_HEARTBEAT_INTERVAL=10

# ✅ Valid
export FLOWCTL_HEARTBEAT_INTERVAL=10s
```

**Out of Range Values:**
```bash
# ❌ Invalid (exceeds maximum)
export CONNECTION_POOL_MAX_CONNECTIONS=100

# ✅ Valid
export CONNECTION_POOL_MAX_CONNECTIONS=50
```

## Troubleshooting

### Common Configuration Issues

#### FlowCtl Not Connecting

**Symptoms:**
- Components start but don't appear in flowctl dashboard
- "Failed to connect to flowctl" warnings in logs

**Solutions:**
1. Verify `FLOWCTL_ENDPOINT` is correct and accessible
2. Check network connectivity to flowctl control plane
3. Ensure flowctl is running and accepting connections
4. Verify firewall rules allow gRPC connections

**Debug Commands:**
```bash
# Test connectivity
telnet flowctl.example.com 8080

# Check DNS resolution
nslookup flowctl.example.com

# Verify flowctl is running
curl http://flowctl.example.com:8080/health
```

#### Connection Pool Issues

**Symptoms:**
- High connection establishment latency
- "No available connections" errors
- Frequent reconnection events

**Solutions:**
1. Increase `CONNECTION_POOL_MIN_CONNECTIONS`
2. Reduce `CONNECTION_POOL_IDLE_TIMEOUT`
3. Check network stability between components
4. Monitor connection pool metrics

**Debug Configuration:**
```bash
# Increase pool size and reduce timeouts
export CONNECTION_POOL_MAX_CONNECTIONS=20
export CONNECTION_POOL_MIN_CONNECTIONS=8
export CONNECTION_POOL_IDLE_TIMEOUT=2m
```

#### Performance Issues

**Symptoms:**
- High CPU usage from monitoring
- Excessive network traffic
- Slow component startup

**Solutions:**
1. Increase monitoring intervals
2. Reduce heartbeat frequency
3. Optimize connection pool settings
4. Check component resource allocation

**Performance Configuration:**
```bash
# Reduce monitoring overhead
export FLOWCTL_HEARTBEAT_INTERVAL=30s
export MONITORING_INTERVAL=15s
export CONNECTION_POOL_MAX_CONNECTIONS=10
```

### Configuration Validation

Use the pipeline runner to validate configuration:

```bash
# Check current configuration
./run-pipeline.sh status

# Validate health endpoints
./run-pipeline.sh health

# Monitor startup process
./run-pipeline.sh development
```

### Log Analysis

Component logs include flowctl configuration details:

```bash
# Check flowctl integration status
grep "FlowCtl" components/*/src/*.log

# Monitor connection pool events
grep "pool" components/*/src/*.log

# Check heartbeat status
grep "heartbeat" components/*/src/*.log
```

## Best Practices

### Development Environment

1. **Use development profile** for fast feedback
2. **Enable debug logging** during development
3. **Use smaller connection pools** to reduce resource usage
4. **Monitor logs actively** during development

```bash
export ENABLE_FLOWCTL=true
export FLOWCTL_HEARTBEAT_INTERVAL=5s
export CONNECTION_POOL_MAX_CONNECTIONS=3
export MONITORING_INTERVAL=2s
export LOG_LEVEL=debug
```

### Production Environment

1. **Use conservative heartbeat intervals** (15-30 seconds)
2. **Size connection pools** based on expected load
3. **Monitor flowctl control plane** health
4. **Implement proper alerting** on configuration issues

```bash
export ENABLE_FLOWCTL=true
export FLOWCTL_HEARTBEAT_INTERVAL=20s
export CONNECTION_POOL_MAX_CONNECTIONS=25
export CONNECTION_POOL_MIN_CONNECTIONS=8
export MONITORING_INTERVAL=10s
```

### Configuration Management

1. **Use environment-specific configurations**
2. **Version control configuration files**
3. **Document custom configurations**
4. **Test configuration changes** in staging environment

### Security Considerations

1. **Secure flowctl endpoints** with TLS
2. **Use appropriate network policies**
3. **Monitor connection attempts**
4. **Implement authentication** where required

```bash
# Secure production configuration
export FLOWCTL_ENDPOINT=flowctl-secure.example.com:8443
export FLOWCTL_TLS_ENABLED=true
export FLOWCTL_AUTH_TOKEN_FILE=/etc/flowctl/token
```

## Configuration Examples

### Complete Development Setup

```bash
#!/bin/bash
# development-config.sh

# Core flowctl configuration
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=localhost:8080
export FLOWCTL_HEARTBEAT_INTERVAL=5s

# Development-optimized settings
export CONNECTION_POOL_MAX_CONNECTIONS=5
export CONNECTION_POOL_MIN_CONNECTIONS=1
export CONNECTION_POOL_IDLE_TIMEOUT=2m
export MONITORING_INTERVAL=3s

# Component-specific optimizations
export TTP_ARROW_PROCESSOR_THREADS=2
export ARROW_ANALYTICS_SINK_WEBSOCKET_PORT=3000

# Debug settings
export LOG_LEVEL=debug

echo "Development configuration loaded"
./run-pipeline.sh development
```

### Complete Production Setup

```bash
#!/bin/bash
# production-config.sh

# Production flowctl configuration
export ENABLE_FLOWCTL=true
export FLOWCTL_ENDPOINT=flowctl-prod.example.com:8080
export FLOWCTL_HEARTBEAT_INTERVAL=20s

# Production-optimized settings
export CONNECTION_POOL_MAX_CONNECTIONS=30
export CONNECTION_POOL_MIN_CONNECTIONS=10
export CONNECTION_POOL_IDLE_TIMEOUT=15m
export MONITORING_INTERVAL=10s

# High-throughput component settings
export TTP_ARROW_PROCESSOR_THREADS=8
export STELLAR_ARROW_SOURCE_BATCH_SIZE=2000
export ARROW_ANALYTICS_SINK_BUFFER_SIZE=20000

# Production logging
export LOG_LEVEL=info

echo "Production configuration loaded"
./run-pipeline.sh production
```

---

**Next Steps:**
- Review [Phase 4 Handoff Document](FLOWCTL_INTEGRATION_PHASE4_HANDOFF.md) for advanced features
- See [Original Integration Plan](FLOWCTL_INTEGRATION_PLAN.md) for complete implementation details
- Monitor Phase 5 completion in [Pipeline Status](../README.md#pipeline-status)

**Support Contact:** obsrvr-stellar-components development team