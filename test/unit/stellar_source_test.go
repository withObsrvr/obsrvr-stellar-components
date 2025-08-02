package test

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/withobsrvr/obsrvr-stellar-components/schemas"
)

func TestStellarLedgerBuilder(t *testing.T) {
	pool := memory.NewGoAllocator()
	defer pool.Destroy()

	builder := schemas.NewStellarLedgerBuilder(pool)
	defer builder.Release()

	// Test adding a ledger
	err := builder.AddLedger(
		12345,                                     // sequence
		make([]byte, 32),                         // hash
		make([]byte, 32),                         // previousHash  
		time.Now(),                               // closeTime
		19,                                       // protocolVersion
		make([]byte, 32),                         // networkID
		10, 9, 1, 25,                            // tx counts and op count
		5000000, 1000000000000,                   // fees
		100, 5000000, 1000,                       // base fee, reserve, max tx set
		[]byte("test-xdr-data"),                  // ledgerXDR
		"rpc",                                    // sourceType
		"https://horizon.stellar.org",            // sourceURL
	)

	require.NoError(t, err)

	// Create record and validate
	record := builder.NewRecord()
	defer record.Release()

	assert.Equal(t, int64(1), record.NumRows())
	assert.Equal(t, len(schemas.StellarLedgerSchema.Fields()), record.NumCols())

	// Verify sequence field
	sequenceCol := record.Column(0)
	assert.Equal(t, uint32(12345), sequenceCol.(*arrow.Uint32Array).Value(0))
}

func TestTTPEventBuilder(t *testing.T) {
	pool := memory.NewGoAllocator()
	defer pool.Destroy()

	builder := schemas.NewTTPEventBuilder(pool)
	defer builder.Release()

	// Test adding a TTP event
	event := schemas.TTPEventData{
		EventID:         "12345:abcdef:0",
		LedgerSequence:  12345,
		TransactionHash: make([]byte, 32),
		OperationIndex:  0,
		Timestamp:       time.Now(),
		EventType:       "payment",
		AssetType:       "native",
		FromAccount:     "GABC123...",
		ToAccount:       "GDEF456...",
		AmountRaw:       10000000,
		AmountStr:       "1.0000000",
		Successful:      true,
		FeeCharged:      100,
		ProcessorVersion: "v1.0.0",
	}

	err := builder.AddEvent(event)
	require.NoError(t, err)

	// Create record and validate
	record := builder.NewRecord()
	defer record.Release()

	assert.Equal(t, int64(1), record.NumRows())
	assert.Equal(t, len(schemas.TTPEventSchema.Fields()), record.NumCols())

	// Verify event ID field
	eventIDCol := record.Column(0)
	assert.Equal(t, "12345:abcdef:0", eventIDCol.(*arrow.StringBuilder).Value(0))
}

func TestSchemaRegistry(t *testing.T) {
	registry := schemas.NewSchemaRegistry()

	// Test schema retrieval
	schema, exists := registry.GetSchema("stellar_ledger")
	assert.True(t, exists)
	assert.NotNil(t, schema)
	assert.Equal(t, "stellar_ledger", schema.Metadata().FindKey("primary_key"))

	schema, exists = registry.GetSchema("ttp_event")
	assert.True(t, exists)
	assert.NotNil(t, schema)

	// Test non-existent schema
	_, exists = registry.GetSchema("nonexistent")
	assert.False(t, exists)

	// Test schema listing
	schemas := registry.ListSchemas()
	assert.Contains(t, schemas, "stellar_ledger")
	assert.Contains(t, schemas, "ttp_event")
	assert.Contains(t, schemas, "contract_data")
}

func TestSchemaVersioning(t *testing.T) {
	// Test schema version extraction
	version := schemas.GetSchemaVersion(schemas.StellarLedgerSchema)
	assert.Equal(t, schemas.StellarLedgerSchemaVersion, version)

	version = schemas.GetSchemaVersion(schemas.TTPEventSchema)
	assert.Equal(t, schemas.TTPEventSchemaVersion, version)
}