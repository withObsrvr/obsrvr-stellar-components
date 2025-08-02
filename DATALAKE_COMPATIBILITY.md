# Datalake Compatibility Guide

## TTP-Processor-Demo Compatible Configuration

The stellar-arrow-source component now supports both the original flat file structure and the ttp-processor-demo partitioned datastore format.

## Configuration Options

### Option 1: TTP-Processor-Demo Compatible (Recommended)

For data organized using the ttp-processor-demo format with partitioned files:

```yaml
source_type: datalake
storage_backend: gcs  # or s3
bucket_name: "your-stellar-datastore"
storage_path: "landing/ledgers/testnet"  # or "landing/ledgers/mainnet"

# Datastore schema (ttp-processor-demo compatible)
ledgers_per_file: 64      # Multiple ledgers per file
files_per_partition: 10   # Files organized in partitions

# Processing range
start_ledger: 1000000
end_ledger: 1001000
batch_size: 1000
```

Environment variables:
```bash
STELLAR_ARROW_SOURCE_SOURCE_TYPE=datalake
STELLAR_ARROW_SOURCE_STORAGE_BACKEND=gcs
STELLAR_ARROW_SOURCE_BUCKET_NAME=your-stellar-datastore
STELLAR_ARROW_SOURCE_STORAGE_PATH=landing/ledgers/testnet
STELLAR_ARROW_SOURCE_LEDGERS_PER_FILE=64
STELLAR_ARROW_SOURCE_FILES_PER_PARTITION=10
STELLAR_ARROW_SOURCE_START_LEDGER=1000000
STELLAR_ARROW_SOURCE_END_LEDGER=1001000
```

### Option 2: Flat File Structure (Legacy)

For data organized as individual XDR files:

```yaml
source_type: datalake
storage_backend: filesystem  # or gcs/s3
bucket_name: "stellar-ledger-archive"  # or storage_path for filesystem
storage_path: "./data/ledgers"

# Single ledger per file (triggers flat file reader)
ledgers_per_file: 1

# Processing range
start_ledger: 1000000
end_ledger: 1001000
batch_size: 1000
```

## Data Organization Patterns

### TTP-Processor-Demo Format
```
bucket/
├── landing/
│   └── ledgers/
│       ├── testnet/
│       │   ├── partition_00001/
│       │   │   ├── batch_0001.xdr  (ledgers 1-64)
│       │   │   ├── batch_0002.xdr  (ledgers 65-128)
│       │   │   └── ...
│       │   ├── partition_00002/
│       │   │   └── ...
│       │   └── ...
│       └── mainnet/
│           └── ...
```

### Flat File Format
```
bucket/
└── ledgers/
    ├── 00001000.xdr
    ├── 00001001.xdr
    ├── 00001002.xdr
    └── ...
```

## Automatic Detection

The component automatically detects which format to use based on configuration:

- **TTP-Processor-Demo Format**: When `ledgers_per_file > 1` and `storage_backend` is `gcs` or `s3`
- **Flat File Format**: When `ledgers_per_file = 1` or `storage_backend` is `filesystem`

## Migration from TTP-Processor-Demo

If you have data from ttp-processor-demo, use these settings:

```bash
# Copy your ttp-processor-demo environment variables
export STELLAR_ARROW_SOURCE_SOURCE_TYPE=datalake
export STELLAR_ARROW_SOURCE_STORAGE_BACKEND=gcs  # or s3
export STELLAR_ARROW_SOURCE_BUCKET_NAME=$BUCKET_NAME
export STELLAR_ARROW_SOURCE_STORAGE_PATH=landing/ledgers/testnet
export STELLAR_ARROW_SOURCE_LEDGERS_PER_FILE=64
export STELLAR_ARROW_SOURCE_FILES_PER_PARTITION=10

# Set your processing range
export STELLAR_ARROW_SOURCE_START_LEDGER=1000000
export STELLAR_ARROW_SOURCE_END_LEDGER=1001000
export STELLAR_ARROW_SOURCE_BATCH_SIZE=1000
```

## Testing the Configuration

1. **Check Data Accessibility**:
   ```bash
   # List bucket contents to verify path structure
   gsutil ls gs://your-bucket/landing/ledgers/testnet/
   # or
   aws s3 ls s3://your-bucket/landing/ledgers/testnet/
   ```

2. **Test Small Range**:
   ```bash
   # Start with a small range to test
   export STELLAR_ARROW_SOURCE_START_LEDGER=1000000
   export STELLAR_ARROW_SOURCE_END_LEDGER=1000010
   ./stellar-arrow-source
   ```

3. **Monitor Logs**:
   ```bash
   # Enable debug logging
   export STELLAR_ARROW_SOURCE_LOG_LEVEL=debug
   ./stellar-arrow-source
   ```

## Troubleshooting

### Common Issues

1. **"No ledgers found"**:
   - Check `storage_path` matches your data organization
   - Verify `ledgers_per_file` and `files_per_partition` match your data schema
   - Ensure bucket/path permissions are correct

2. **"Failed to read ledger XDR"**:
   - Data might be in different format than expected
   - Try switching between ttp-processor-demo and flat file configurations

3. **"Access denied"**:
   - Check GCS/S3 credentials and permissions
   - Verify bucket name and path are correct

### Debug Commands

```bash
# Test GCS access
gsutil ls gs://your-bucket/landing/ledgers/testnet/partition_00001/

# Test S3 access
aws s3 ls s3://your-bucket/landing/ledgers/testnet/partition_00001/

# Check file contents
gsutil cat gs://your-bucket/landing/ledgers/testnet/partition_00001/batch_0001.xdr | xxd | head
```

## Performance Tuning

For ttp-processor-demo compatible format:

```yaml
# Optimize for partitioned data
concurrent_readers: 8      # Higher concurrency for partitioned reads
buffer_size: 20000        # Larger buffer for batched data
batch_size: 2000          # Larger batches for better throughput

# Arrow optimization
arrow_memory_pool: "jemalloc"  # Better memory management
compression: "lz4"             # Fast compression for large batches
```

This configuration should make the datalake source work with data that was successfully processed by ttp-processor-demo.