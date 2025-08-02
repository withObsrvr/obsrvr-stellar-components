# Pipeline Templates

This directory contains pre-configured pipeline templates for common Stellar data processing use cases using the obsrvr-stellar-components bundle.

## Available Templates

### 1. RPC Pipeline (`rpc-pipeline.yaml`)
**Use Case**: Real-time processing of TTP events from Stellar RPC endpoints

**Features**:
- Real-time ledger ingestion from Stellar Horizon/Soroban
- Live TTP event extraction and processing
- Multi-format output (Parquet, JSON, WebSocket)
- Real-time analytics and monitoring
- Development, staging, and production configurations

**Best For**:
- Live monitoring and alerting
- Real-time dashboards
- Streaming analytics
- Development and testing

### 2. Data Lake Pipeline (`datalake-pipeline.yaml`)
**Use Case**: Batch processing of historical TTP events from archived ledger data

**Features**:
- Bulk processing from S3, GCS, or filesystem storage
- High-throughput batch processing
- Optimized for historical analysis
- Quality assurance checks
- Batch job scheduling support
- Data retention policies

**Best For**:
- Historical data analysis
- Backfilling missing data
- Large-scale analytics jobs
- Research and compliance reporting

### 3. Analytics Pipeline (`analytics-pipeline.yaml`)
**Use Case**: Advanced real-time analytics with dashboards and monitoring

**Features**:
- Multi-window time-series analytics
- Real-time dashboards and visualizations
- Custom metrics and alerting
- Machine learning integration
- Asset-specific filtering
- Advanced monitoring and observability

**Best For**:
- Production monitoring
- Business intelligence
- Risk management
- Market surveillance
- Trading analytics

## Usage

### Quick Start

```bash
# Run RPC pipeline with default settings
flowctl pipeline run --template templates/rpc-pipeline.yaml

# Run with custom configuration
flowctl pipeline run --template templates/rpc-pipeline.yaml \
  --config STELLAR_NETWORK=mainnet \
  --config WEBSOCKET_PORT=9090

# Run in development mode with hot reload
flowctl dev --template templates/rpc-pipeline.yaml \
  --env development --watch
```

### Using Bundle Templates

```bash
# Install the bundle first
flowctl bundle install obsrvr/obsrvr-stellar-suite@v1.0.0

# Run using bundle template reference
flowctl pipeline run --bundle obsrvr/obsrvr-stellar-suite@v1.0.0 \
  --template rpc-pipeline \
  --config stellar.network=testnet

# List available templates in bundle
flowctl bundle templates obsrvr/obsrvr-stellar-suite@v1.0.0
```

### Environment Configuration

All templates support environment-specific overrides:

```bash
# Development environment
flowctl pipeline run --template templates/analytics-pipeline.yaml \
  --env development

# Production environment with custom config
flowctl pipeline run --template templates/analytics-pipeline.yaml \
  --env production \
  --config STELLAR_RPC_URL=https://your-rpc-endpoint.com \
  --config ENABLE_ALERTS=true
```

## Configuration

### Common Environment Variables

| Variable | Description | Default | Templates |
|----------|-------------|---------|-----------|
| `STELLAR_NETWORK` | Stellar network (mainnet/testnet) | testnet | All |
| `STELLAR_RPC_URL` | RPC endpoint URL | Auto-detected | RPC, Analytics |
| `NETWORK_PASSPHRASE` | Network passphrase | Auto-detected | All |
| `DATA_DIR` | Data output directory | ./data | All |
| `LOG_LEVEL` | Logging level | info | All |
| `EXECUTION_MODE` | Execution mode (native/container) | native | All |

### RPC Pipeline Specific

| Variable | Description | Default |
|----------|-------------|---------|
| `START_LEDGER` | Starting ledger sequence | 0 (latest) |
| `WEBSOCKET_PORT` | WebSocket server port | 8080 |
| `OUTPUT_FORMATS` | Output formats | parquet,json,websocket |
| `BATCH_SIZE` | Processing batch size | 1000 |
| `PROCESSOR_THREADS` | Processing threads | 4 |

### Data Lake Pipeline Specific

| Variable | Description | Default |
|----------|-------------|---------|
| `STORAGE_BACKEND` | Storage backend | s3 |
| `BUCKET_NAME` | Storage bucket name | Required |
| `AWS_REGION` | AWS region | us-west-2 |
| `START_LEDGER` | Starting ledger sequence | 1000000 |
| `END_LEDGER` | Ending ledger sequence | 0 (continuous) |
| `CONCURRENT_READERS` | Concurrent readers | 4 |
| `COMPRESSION` | Output compression | snappy |

### Analytics Pipeline Specific

| Variable | Description | Default |
|----------|-------------|---------|
| `WEBSOCKET_PORT` | WebSocket server port | 8080 |
| `API_PORT` | REST API server port | 8081 |
| `ENABLE_DASHBOARDS` | Enable dashboards | true |
| `ENABLE_ALERTS` | Enable alerting | false |
| `ALERT_WEBHOOK_URL` | Alert webhook URL | - |
| `ENABLE_ML` | Enable ML features | false |

## Examples

### Example 1: Testnet Development Setup

```bash
# Create environment file
cat > .env << EOF
STELLAR_NETWORK=testnet
STELLAR_RPC_URL=https://soroban-testnet.stellar.org
DATA_DIR=./test-data
LOG_LEVEL=debug
WEBSOCKET_PORT=8080
OUTPUT_FORMATS=json,websocket
EOF

# Run RPC pipeline
flowctl pipeline run --template templates/rpc-pipeline.yaml \
  --env-file .env --env development
```

### Example 2: Production Analytics

```bash
# Production configuration  
export STELLAR_NETWORK=mainnet
export STELLAR_RPC_URL=https://horizon.stellar.org
export DATA_DIR=/data/stellar
export ENABLE_ALERTS=true
export ALERT_WEBHOOK_URL=https://hooks.slack.com/...
export WEBSOCKET_PORT=8080
export API_PORT=8081

# Run analytics pipeline
flowctl pipeline run --template templates/analytics-pipeline.yaml \
  --env production
```

### Example 3: Historical Data Processing

```bash
# Process specific ledger range from S3
export STORAGE_BACKEND=s3
export BUCKET_NAME=stellar-ledger-archive
export AWS_REGION=us-west-2
export START_LEDGER=45000000
export END_LEDGER=45100000
export BATCH_SIZE=5000
export PROCESSOR_THREADS=16

# Run data lake pipeline
flowctl pipeline run --template templates/datalake-pipeline.yaml
```

## Customization

### Creating Custom Templates

1. Copy an existing template:
```bash
cp templates/rpc-pipeline.yaml templates/my-custom-pipeline.yaml
```

2. Modify configuration:
```yaml
metadata:
  name: my-custom-pipeline
  description: "My custom Stellar processing pipeline"

spec:
  config:
    # Add custom configuration
    custom:
      feature_enabled: true
      custom_setting: value
```

3. Run custom template:
```bash
flowctl pipeline run --template templates/my-custom-pipeline.yaml
```

### Template Inheritance

You can extend existing templates:

```yaml
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: extended-rpc-pipeline
  extends: templates/rpc-pipeline.yaml

spec:
  # Override specific components
  sinks:
    - name: custom-sink
      component: myorg/custom-sink@v1.0.0
      inputs: [ttp-processor]
      config:
        custom_option: true
```

## Monitoring

### Health Checks

All templates include health check endpoints:

```bash
# Check component health
curl http://localhost:8088/health

# Check metrics
curl http://localhost:9090/metrics
```

### WebSocket Monitoring

For templates with WebSocket support:

```javascript
// Connect to real-time event stream
const ws = new WebSocket('ws://localhost:8080/ws');
ws.onmessage = (event) => {
  const ttpEvent = JSON.parse(event.data);
  console.log('TTP Event:', ttpEvent);
};
```

### REST API

For templates with API support:

```bash
# Get recent events
curl http://localhost:8081/api/events?limit=100

# Get events by asset
curl http://localhost:8081/api/events?asset_code=USDC&limit=50

# Get analytics summary
curl http://localhost:8081/api/analytics/summary
```

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify RPC endpoint is accessible
   - Check network connectivity
   - Confirm credentials for storage backends

2. **Performance Issues**
   - Adjust batch sizes based on available memory
   - Increase processor threads for CPU-bound workloads
   - Monitor memory usage and adjust limits

3. **Storage Issues**
   - Ensure sufficient disk space for output
   - Check write permissions for data directories
   - Verify storage backend credentials

### Debug Mode

Enable debug logging:

```bash
flowctl pipeline run --template templates/rpc-pipeline.yaml \
  --config LOG_LEVEL=debug \
  --verbose
```

### Component Logs

View individual component logs:

```bash
# View all logs
flowctl logs

# View specific component logs
flowctl logs --component stellar-arrow-source

# Follow logs in real-time
flowctl logs --follow
```

## Performance Tuning

### Resource Allocation

Adjust resource limits based on workload:

```yaml
# In template customization
spec:
  sources:
    - name: stellar-rpc-source
      resources:
        requests:
          cpu: 500m      # Increase for higher throughput
          memory: 1Gi    # Increase for larger batches
        limits:
          cpu: 2000m
          memory: 4Gi
```

### Batch Size Optimization

| Use Case | Recommended Batch Size | Memory Impact |
|----------|----------------------|---------------|
| Development | 500-1000 | Low |
| Real-time Processing | 1000-2000 | Medium |
| Batch Analytics | 5000-10000 | High |
| Historical Backfill | 10000+ | Very High |

### Thread Configuration

| Component | CPU Bound | IO Bound | Recommended Threads |
|-----------|-----------|----------|-------------------|
| Source | Medium | High | 2-4 |
| Processor | High | Low | CPU cores * 1.5 |
| Sink | Low | High | 4-8 |

## Next Steps

1. **Development**: Start with RPC pipeline in development mode
2. **Testing**: Use small ledger ranges with data lake pipeline
3. **Production**: Deploy analytics pipeline with monitoring
4. **Scaling**: Optimize resource allocation based on metrics
5. **Customization**: Create custom templates for specific use cases