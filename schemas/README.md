# Arrow Schemas for Stellar Data

This package contains Apache Arrow schema definitions and utilities for processing Stellar blockchain data in the obsrvr-stellar-components ecosystem.

## Overview

The schemas are designed to optimize for:
- **Analytical Performance**: Columnar format enables fast aggregations and filtering
- **Zero-Copy Operations**: Data can be passed between components without serialization overhead
- **Type Safety**: Strong typing with schema validation
- **Evolution Support**: Versioned schemas with migration capabilities

## Schemas

### StellarLedgerSchema
Defines the structure for Stellar ledger data including:
- Ledger metadata (sequence, hash, timestamp)
- Transaction statistics
- Fee information
- Raw XDR data for detailed processing
- Source metadata

**Key Features:**
- Optimized for time-series queries with timestamp indexing
- Supports both RPC and data lake sources
- Includes network information for multi-network deployments

### TTPEventSchema
Defines the structure for Token Transfer Protocol events:
- Event identification and timing
- Asset information (code, issuer, type)
- Transfer participants (from/to accounts)
- Amount data (both precise integers and strings)
- Path payment support
- Transaction metadata (memo, fees, success status)

**Key Features:**
- Designed for real-time analytics
- Supports complex path payment scenarios
- High-precision amount handling

### ContractDataSchema
Defines the structure for Stellar smart contract data (future use):
- Contract identification
- Data key/value pairs
- Operation types (create, update, delete)
- Durability settings

## Usage

### Basic Schema Access

```go
import "github.com/withobsrvr/obsrvr-stellar-components/schemas"

// Get schema registry
registry := schemas.NewSchemaRegistry()

// Get a specific schema
ledgerSchema, exists := registry.GetSchema("stellar_ledger")
if !exists {
    log.Fatal("Schema not found")
}

// Create a record builder
builder, err := registry.CreateRecordBuilder("stellar_ledger")
if err != nil {
    log.Fatal(err)
}
defer builder.Release()
```

### Building Ledger Records

```go
pool := memory.DefaultAllocator
builder := schemas.NewStellarLedgerBuilder(pool)
defer builder.Release()

// Add ledger from parsed XDR
err := builder.AddLedgerFromXDR(xdrData, "rpc", "https://horizon.stellar.org")
if err != nil {
    log.Fatal(err)
}

// Create Arrow record
record := builder.NewRecord()
defer record.Release()
```

### Building TTP Event Records

```go
pool := memory.DefaultAllocator
builder := schemas.NewTTPEventBuilder(pool)
defer builder.Release()

// Create event data
event := schemas.TTPEventData{
    EventID:         "12345:abcd:0",
    LedgerSequence:  12345,
    TransactionHash: txHash,
    OperationIndex:  0,
    Timestamp:       time.Now(),
    EventType:       "payment",
    AssetType:       "native",
    FromAccount:     "GABC...",
    ToAccount:       "GDEF...",
    AmountRaw:       10000000, // 1 XLM in stroops
    AmountStr:       "1.0000000",
    Successful:      true,
    FeeCharged:      100,
    ProcessorVersion: "1.0.0",
}

// Add to builder
err := builder.AddEvent(event)
if err != nil {
    log.Fatal(err)
}

// Create Arrow record
record := builder.NewRecord()
defer record.Release()
```

### Schema Evolution

```go
registry := schemas.NewSchemaRegistry()
evolution := schemas.NewSchemaEvolution(registry)

// Check if record can be upgraded
if evolution.CanUpgrade(oldRecord, "stellar_ledger") {
    newRecord, err := evolution.MigrateRecord(oldRecord, "stellar_ledger")
    if err != nil {
        log.Fatal(err)
    }
    // Use newRecord...
}
```

### Utility Functions

```go
// Format amounts
amountStr := schemas.FormatAmount(10000000) // "1.0000000"

// Parse amounts
stroops, err := schemas.ParseAmount("1.5000000") // 15000000

// Generate event IDs
eventID := schemas.GenerateEventID(12345, txHash, 0) // "12345:abcd...:0"

// Handle XDR encoding
encoded := schemas.EncodeXDRBase64(xdrData)
decoded, err := schemas.DecodeXDRBase64(encoded)
```

## Schema Metadata

Each schema includes rich metadata for:
- Field descriptions and types
- Indexing hints for query optimization
- Enumeration values for categorical fields
- Units and encoding information
- Partitioning and sorting recommendations

### Example Metadata Access

```go
schema := schemas.StellarLedgerSchema
metadata := schema.Metadata()

// Get schema version
version := metadata.FindKey("version")
if version >= 0 {
    fmt.Printf("Schema version: %s\n", metadata.Values()[version])
}

// Get field metadata
field := schema.Field(0) // sequence field
fieldMetadata := field.Metadata
if desc := fieldMetadata.FindKey("description"); desc >= 0 {
    fmt.Printf("Field description: %s\n", fieldMetadata.Values()[desc])
}
```

## Performance Considerations

### Memory Management
- Always release builders and records when done
- Use memory pools for high-throughput scenarios
- Consider batch processing for better throughput

### Query Optimization
- Use indexed fields (marked in metadata) for filtering
- Leverage partition keys for time-based queries
- Consider projection (selecting only needed columns)

### Schema Evolution
- Version your schemas appropriately
- Test migrations with production data volumes
- Consider backward compatibility requirements

## Integration with Components

These schemas are used throughout the obsrvr-stellar-components:

- **stellar-arrow-source**: Produces records using StellarLedgerSchema
- **ttp-arrow-processor**: Consumes StellarLedgerSchema, produces TTPEventSchema  
- **arrow-analytics-sink**: Consumes TTPEventSchema for various outputs

## Testing

```go
// Example test
func TestSchemaCompatibility(t *testing.T) {
    registry := schemas.NewSchemaRegistry()
    
    // Test schema registration
    schema, exists := registry.GetSchema("stellar_ledger")
    assert.True(t, exists)
    assert.NotNil(t, schema)
    
    // Test record building
    builder := schemas.NewStellarLedgerBuilder(memory.DefaultAllocator)
    defer builder.Release()
    
    // Add test data and validate
    // ...
}
```

## Future Enhancements

- Additional schemas for contract events and invocations
- Advanced schema evolution with field mapping
- Compression optimization for different field types
- Integration with Arrow Datasets for partitioned storage