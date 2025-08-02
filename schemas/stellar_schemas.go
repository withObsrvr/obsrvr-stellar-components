package schemas

import (
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// Schema version for evolution tracking
const (
	StellarLedgerSchemaVersion = "1.0.0"
	TTPEventSchemaVersion      = "1.0.0"
	ContractDataSchemaVersion  = "1.0.0"
)

// StellarLedgerSchema defines the Arrow schema for Stellar ledger data
// This schema optimizes for both analytical queries and detailed processing
var StellarLedgerSchema = arrow.NewSchema(
	[]arrow.Field{
		// Core ledger identification
		{Name: "sequence", Type: arrow.PrimitiveTypes.Uint32, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Ledger sequence number (primary key)",
				"index":       "true",
			})},
		{Name: "hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Ledger hash (32 bytes)",
				"encoding":    "hex",
			})},
		{Name: "previous_hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Previous ledger hash (32 bytes)",
				"encoding":    "hex",
			})},

		// Timing information
		{Name: "close_time", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Ledger close timestamp",
				"timezone":    "UTC",
				"index":       "true",
			})},
		{Name: "ingestion_time", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "When this ledger was ingested",
				"timezone":    "UTC",
			})},

		// Network information
		{Name: "protocol_version", Type: arrow.PrimitiveTypes.Uint32, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Stellar protocol version",
			})},
		{Name: "network_id", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Network identifier hash",
				"encoding":    "hex",
			})},

		// Transaction statistics
		{Name: "transaction_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Total number of transactions in ledger",
			})},
		{Name: "successful_transaction_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Number of successful transactions",
			})},
		{Name: "failed_transaction_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Number of failed transactions",
			})},
		{Name: "operation_count", Type: arrow.PrimitiveTypes.Uint32, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Total number of operations in ledger",
			})},

		// Fee information (in stroops)
		{Name: "total_fees", Type: arrow.PrimitiveTypes.Int64, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Total fees collected (stroops)",
				"unit":        "stroops",
			})},
		{Name: "fee_pool", Type: arrow.PrimitiveTypes.Int64, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Fee pool balance (stroops)",
				"unit":        "stroops",
			})},
		{Name: "base_fee", Type: arrow.PrimitiveTypes.Uint32, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Base fee for this ledger (stroops)",
				"unit":        "stroops",
			})},
		{Name: "base_reserve", Type: arrow.PrimitiveTypes.Uint32, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Base reserve for this ledger (stroops)",
				"unit":        "stroops",
			})},

		// Ledger capacity metrics
		{Name: "max_tx_set_size", Type: arrow.PrimitiveTypes.Uint32, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Maximum transaction set size",
			})},

		// Raw data for detailed processing
		{Name: "ledger_xdr", Type: arrow.BinaryTypes.Binary, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Raw XDR ledger data",
				"encoding":    "base64",
				"compress":    "true",
			})},

		// Source metadata
		{Name: "source_type", Type: arrow.BinaryTypes.String, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Data source type",
				"enum":        "rpc,datalake,archive",
			})},
		{Name: "source_url", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Source URL or file path",
			})},

		// Processing metadata
		{Name: "schema_version", Type: arrow.BinaryTypes.String, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Schema version for this record",
			})},
	},
	func() *arrow.Metadata {
		m := arrow.NewMetadata(
			[]string{"version", "description", "primary_key", "partition_key"},
			[]string{StellarLedgerSchemaVersion, "Stellar ledger data optimized for analytics", "sequence", "close_time"},
		)
		return &m
	}(),
)

// TTPEventSchema defines the Arrow schema for Token Transfer Protocol events
// Optimized for analytical queries and real-time processing
var TTPEventSchema = arrow.NewSchema(
	[]arrow.Field{
		// Event identification
		{Name: "event_id", Type: arrow.BinaryTypes.String, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Unique event identifier",
				"format":      "ledger:tx:op",
			})},
		{Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Uint32, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Source ledger sequence",
				"index":       "true",
			})},
		{Name: "transaction_hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Transaction hash (32 bytes)",
				"encoding":    "hex",
			})},
		{Name: "operation_index", Type: arrow.PrimitiveTypes.Uint32, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Operation index within transaction",
			})},

		// Timing
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Event timestamp (ledger close time)",
				"timezone":    "UTC",
				"index":       "true",
			})},
		{Name: "processed_at", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "When this event was processed",
				"timezone":    "UTC",
			})},

		// Event classification
		{Name: "event_type", Type: arrow.BinaryTypes.String, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Type of transfer event",
				"enum":        "payment,path_payment_strict_receive,path_payment_strict_send",
				"index":       "true",
			})},

		// Asset information
		{Name: "asset_type", Type: arrow.BinaryTypes.String, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Asset type",
				"enum":        "native,credit_alphanum4,credit_alphanum12",
			})},
		{Name: "asset_code", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Asset code (null for native XLM)",
				"index":       "true",
			})},
		{Name: "asset_issuer", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Asset issuer account (null for native XLM)",
				"index":       "true",
			})},

		// Transfer participants
		{Name: "from_account", Type: arrow.BinaryTypes.String, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Source account public key",
				"index":       "true",
			})},
		{Name: "to_account", Type: arrow.BinaryTypes.String, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Destination account public key",
				"index":       "true",
			})},

		// Amount information (using both raw and string for precision)
		{Name: "amount_raw", Type: arrow.PrimitiveTypes.Int64, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Amount in stroops (for XLM) or smallest unit",
				"unit":        "stroops",
			})},
		{Name: "amount_str", Type: arrow.BinaryTypes.String, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Amount as string with full precision",
			})},

		// Path payment specific fields
		{Name: "source_asset_type", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Source asset type for path payments",
				"enum":        "native,credit_alphanum4,credit_alphanum12",
			})},
		{Name: "source_asset_code", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Source asset code for path payments",
			})},
		{Name: "source_asset_issuer", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Source asset issuer for path payments",
			})},
		{Name: "source_amount_raw", Type: arrow.PrimitiveTypes.Int64, Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Source amount for path payments (stroops)",
				"unit":        "stroops",
			})},
		{Name: "source_amount_str", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Source amount string for path payments",
			})},

		// Transaction status
		{Name: "successful", Type: arrow.FixedWidthTypes.Boolean, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Whether the transaction was successful",
				"index":       "true",
			})},

		// Transaction memo
		{Name: "memo_type", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Transaction memo type",
				"enum":        "none,text,id,hash,return",
			})},
		{Name: "memo_value", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Transaction memo value",
			})},

		// Fee information
		{Name: "fee_charged", Type: arrow.PrimitiveTypes.Int64, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Fee charged for this transaction (stroops)",
				"unit":        "stroops",
			})},

		// Processing metadata
		{Name: "processor_version", Type: arrow.BinaryTypes.String, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Version of processor that created this event",
			})},
		{Name: "schema_version", Type: arrow.BinaryTypes.String, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Schema version for this record",
			})},
	},
	func() *arrow.Metadata {
		m := arrow.NewMetadata(
			[]string{"version", "description", "primary_key", "partition_key", "sort_key"},
			[]string{TTPEventSchemaVersion, "Token Transfer Protocol events optimized for analytics", "event_id", "timestamp", "ledger_sequence,operation_index"},
		)
		return &m
	}(),
)

// ContractDataSchema defines the Arrow schema for Stellar smart contract data
// This will be used in future phases for contract processing components
var ContractDataSchema = arrow.NewSchema(
	[]arrow.Field{
		// Event identification
		{Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Uint32, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Source ledger sequence",
				"index":       "true",
			})},
		{Name: "transaction_hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Transaction hash (32 bytes)",
				"encoding":    "hex",
			})},

		// Contract information
		{Name: "contract_id", Type: arrow.BinaryTypes.String, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Contract address",
				"index":       "true",
			})},
		{Name: "contract_type", Type: arrow.BinaryTypes.String, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Type of contract data",
				"enum":        "persistent,temporary,instance",
			})},

		// Data key information
		{Name: "key_hash", Type: &arrow.FixedSizeBinaryType{ByteWidth: 32}, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Hash of the data key",
				"encoding":    "hex",
			})},
		{Name: "key_type", Type: arrow.BinaryTypes.String, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "XDR type of the key",
			})},
		{Name: "key_decoded", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Human-readable key representation",
			})},

		// Data value information
		{Name: "value_type", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "XDR type of the value",
			})},
		{Name: "value_decoded", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Human-readable value representation",
			})},

		// Operation type
		{Name: "operation", Type: arrow.BinaryTypes.String, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Type of data operation",
				"enum":        "create,update,delete",
				"index":       "true",
			})},

		// Timing
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Event timestamp",
				"timezone":    "UTC",
				"index":       "true",
			})},

		// Processing metadata
		{Name: "schema_version", Type: arrow.BinaryTypes.String, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{
				"description": "Schema version for this record",
			})},
	},
	func() *arrow.Metadata {
		m := arrow.NewMetadata(
			[]string{"version", "description", "primary_key", "partition_key"},
			[]string{ContractDataSchemaVersion, "Stellar smart contract data changes", "ledger_sequence,transaction_hash,key_hash", "timestamp"},
		)
		return &m
	}(),
)

// SchemaRegistry provides utilities for schema management and evolution
type SchemaRegistry struct {
	schemas map[string]*arrow.Schema
	pool    memory.Allocator
}

// NewSchemaRegistry creates a new schema registry
func NewSchemaRegistry() *SchemaRegistry {
	registry := &SchemaRegistry{
		schemas: make(map[string]*arrow.Schema),
		pool:    memory.DefaultAllocator,
	}

	// Register all schemas
	registry.RegisterSchema("stellar_ledger", StellarLedgerSchema)
	registry.RegisterSchema("ttp_event", TTPEventSchema)
	registry.RegisterSchema("contract_data", ContractDataSchema)

	return registry
}

// RegisterSchema registers a schema with the registry
func (r *SchemaRegistry) RegisterSchema(name string, schema *arrow.Schema) {
	r.schemas[name] = schema
}

// GetSchema retrieves a schema by name
func (r *SchemaRegistry) GetSchema(name string) (*arrow.Schema, bool) {
	schema, exists := r.schemas[name]
	return schema, exists
}

// ListSchemas returns all registered schema names
func (r *SchemaRegistry) ListSchemas() []string {
	names := make([]string, 0, len(r.schemas))
	for name := range r.schemas {
		names = append(names, name)
	}
	return names
}

// ValidateSchema validates that a record matches a schema
func (r *SchemaRegistry) ValidateSchema(schemaName string, record arrow.Record) error {
	schema, exists := r.GetSchema(schemaName)
	if !exists {
		return arrow.ErrInvalid
	}

	if !schema.Equal(record.Schema()) {
		return arrow.ErrInvalid
	}

	return nil
}

// GetSchemaVersion returns the version of a schema
func GetSchemaVersion(schema *arrow.Schema) string {
	if metadata := schema.Metadata(); metadata.Len() > 0 {
		if version := metadata.FindKey("version"); version >= 0 {
			return metadata.Values()[version]
		}
	}
	return "unknown"
}

// CreateRecordBuilder creates a new record builder for a schema
func (r *SchemaRegistry) CreateRecordBuilder(schemaName string) (*array.RecordBuilder, error) {
	schema, exists := r.GetSchema(schemaName)
	if !exists {
		return nil, arrow.ErrInvalid
	}

	return array.NewRecordBuilder(r.pool, schema), nil
}