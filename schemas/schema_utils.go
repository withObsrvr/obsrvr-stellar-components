package schemas

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stellar/go/xdr"
)

// StellarLedgerBuilder provides a convenient way to build Stellar ledger records
type StellarLedgerBuilder struct {
	builder *array.RecordBuilder
	pool    memory.Allocator
}

// NewStellarLedgerBuilder creates a new builder for Stellar ledger records
func NewStellarLedgerBuilder(pool memory.Allocator) *StellarLedgerBuilder {
	return &StellarLedgerBuilder{
		builder: array.NewRecordBuilder(pool, StellarLedgerSchema),
		pool:    pool,
	}
}

// AddLedger adds a ledger record from XDR data
func (b *StellarLedgerBuilder) AddLedger(
	sequence uint32,
	hash []byte,
	previousHash []byte,
	closeTime time.Time,
	protocolVersion uint32,
	networkID []byte,
	txCount, successfulTxCount, failedTxCount, opCount uint32,
	totalFees, feePool int64,
	baseFee, baseReserve, maxTxSetSize uint32,
	ledgerXDR []byte,
	sourceType, sourceURL string,
) error {
	// Validate required fields
	if len(hash) != 32 {
		return fmt.Errorf("hash must be 32 bytes, got %d", len(hash))
	}
	if len(previousHash) != 32 {
		return fmt.Errorf("previousHash must be 32 bytes, got %d", len(previousHash))
	}
	if len(networkID) != 32 {
		return fmt.Errorf("networkID must be 32 bytes, got %d", len(networkID))
	}

	// Add to builders
	b.builder.Field(0).(*array.Uint32Builder).Append(sequence)
	b.builder.Field(1).(*array.FixedSizeBinaryBuilder).Append(hash)
	b.builder.Field(2).(*array.FixedSizeBinaryBuilder).Append(previousHash)
	b.builder.Field(3).(*array.TimestampBuilder).Append(arrow.Timestamp(closeTime.UnixMicro()))
	b.builder.Field(4).(*array.TimestampBuilder).Append(arrow.Timestamp(time.Now().UnixMicro()))
	b.builder.Field(5).(*array.Uint32Builder).Append(protocolVersion)
	b.builder.Field(6).(*array.FixedSizeBinaryBuilder).Append(networkID)
	b.builder.Field(7).(*array.Uint32Builder).Append(txCount)
	b.builder.Field(8).(*array.Uint32Builder).Append(successfulTxCount)
	b.builder.Field(9).(*array.Uint32Builder).Append(failedTxCount)
	b.builder.Field(10).(*array.Uint32Builder).Append(opCount)
	b.builder.Field(11).(*array.Int64Builder).Append(totalFees)
	b.builder.Field(12).(*array.Int64Builder).Append(feePool)
	b.builder.Field(13).(*array.Uint32Builder).Append(baseFee)
	b.builder.Field(14).(*array.Uint32Builder).Append(baseReserve)
	b.builder.Field(15).(*array.Uint32Builder).Append(maxTxSetSize)
	b.builder.Field(16).(*array.BinaryBuilder).Append(ledgerXDR)
	b.builder.Field(17).(*array.StringBuilder).Append(sourceType)

	if sourceURL != "" {
		b.builder.Field(18).(*array.StringBuilder).Append(sourceURL)
	} else {
		b.builder.Field(18).(*array.StringBuilder).AppendNull()
	}

	b.builder.Field(19).(*array.StringBuilder).Append(StellarLedgerSchemaVersion)

	return nil
}

// ProcessedLedgerData represents validated and processed ledger data from XDRProcessor
type ProcessedLedgerData struct {
	// Core ledger identification
	Sequence     uint32
	Hash         []byte
	PreviousHash []byte
	CloseTime    time.Time

	// Network information
	ProtocolVersion uint32
	NetworkID       []byte

	// Transaction statistics
	TransactionCount          uint32
	SuccessfulTransactionCount uint32
	FailedTransactionCount     uint32
	OperationCount            uint32

	// Fee information
	TotalFees   int64
	FeePool     int64
	BaseFee     uint32
	BaseReserve uint32

	// Ledger capacity
	MaxTxSetSize uint32

	// Source metadata
	SourceType string
	SourceURL  string
	RawXDR     []byte

	// Validation metadata
	ValidationTime time.Time
	NetworkValid   bool
	StructureValid bool
}

// AddProcessedLedger adds a ledger record from pre-processed and validated data
func (b *StellarLedgerBuilder) AddProcessedLedger(data *ProcessedLedgerData) error {
	// Validate the processed data structure
	if data == nil {
		return fmt.Errorf("processed ledger data is nil")
	}

	// Use the validated data directly
	return b.AddLedger(
		data.Sequence,
		data.Hash,
		data.PreviousHash,
		data.CloseTime,
		data.ProtocolVersion,
		data.NetworkID,
		data.TransactionCount,
		data.SuccessfulTransactionCount,
		data.FailedTransactionCount,
		data.OperationCount,
		data.TotalFees,
		data.FeePool,
		data.BaseFee,
		data.BaseReserve,
		data.MaxTxSetSize,
		data.RawXDR,
		data.SourceType,
		data.SourceURL,
	)
}

// AddLedgerFromXDR adds a ledger record by parsing XDR data
func (b *StellarLedgerBuilder) AddLedgerFromXDR(
	xdrData []byte,
	sourceType, sourceURL string,
) error {
	var meta xdr.LedgerCloseMeta
	if err := meta.UnmarshalBinary(xdrData); err != nil {
		return fmt.Errorf("failed to unmarshal XDR: %w", err)
	}

	ledgerHeader := meta.MustV0().LedgerHeader.Header

	// Extract basic information
	sequence := uint32(ledgerHeader.LedgerSeq)
	hash := ledgerHeader.Hash
	previousHash := ledgerHeader.PreviousLedgerHash
	closeTime := time.Unix(int64(ledgerHeader.ScpValue.CloseTime), 0)
	protocolVersion := uint32(ledgerHeader.LedgerVersion)
	
	// Extract network ID (from the bucket list hash for now)
	networkID := ledgerHeader.BucketListHash

	// Count transactions and operations
	txSet := meta.MustV0().TxSet
	txCount := uint32(len(txSet.Txs))
	
	var successfulTxCount, failedTxCount, opCount uint32
	txProcessing := meta.MustV0().TxProcessing
	
	for _, result := range txProcessing {
		if result.Result.Successful() {
			successfulTxCount++
		} else {
			failedTxCount++
		}
		
		// Count operations in this transaction
		if result.Result.Successful() {
			opCount += uint32(len(result.Result.OperationResults()))
		}
	}

	// Calculate fees
	var totalFees int64
	for _, result := range txProcessing {
		totalFees += int64(result.FeeCharged)
	}

	feePool := int64(ledgerHeader.FeePool)
	baseFee := uint32(ledgerHeader.BaseFee)
	baseReserve := uint32(ledgerHeader.BaseReserve)
	maxTxSetSize := uint32(ledgerHeader.MaxTxSetSize)

	return b.AddLedger(
		sequence, hash[:], previousHash[:], closeTime,
		protocolVersion, networkID[:],
		txCount, successfulTxCount, failedTxCount, opCount,
		totalFees, feePool, baseFee, baseReserve, maxTxSetSize,
		xdrData, sourceType, sourceURL,
	)
}

// NewRecord creates a new Arrow record from the builder
func (b *StellarLedgerBuilder) NewRecord() arrow.Record {
	return b.builder.NewRecord()
}

// Release releases the builder resources
func (b *StellarLedgerBuilder) Release() {
	b.builder.Release()
}

// TTPEventBuilder provides a convenient way to build TTP event records
type TTPEventBuilder struct {
	builder *array.RecordBuilder
	pool    memory.Allocator
}

// NewTTPEventBuilder creates a new builder for TTP event records
func NewTTPEventBuilder(pool memory.Allocator) *TTPEventBuilder {
	return &TTPEventBuilder{
		builder: array.NewRecordBuilder(pool, TTPEventSchema),
		pool:    pool,
	}
}

// TTPEventData represents a single TTP event
type TTPEventData struct {
	EventID         string
	LedgerSequence  uint32
	TransactionHash []byte
	OperationIndex  uint32
	Timestamp       time.Time
	EventType       string
	AssetType       string
	AssetCode       *string
	AssetIssuer     *string
	FromAccount     string
	ToAccount       string
	AmountRaw       int64
	AmountStr       string
	// Path payment fields
	SourceAssetType   *string
	SourceAssetCode   *string
	SourceAssetIssuer *string
	SourceAmountRaw   *int64
	SourceAmountStr   *string
	// Transaction info
	Successful    bool
	MemoType      *string
	MemoValue     *string
	FeeCharged    int64
	ProcessorVersion string
}

// AddEvent adds a TTP event to the builder
func (b *TTPEventBuilder) AddEvent(event TTPEventData) error {
	// Validate required fields
	if len(event.TransactionHash) != 32 {
		return fmt.Errorf("transaction hash must be 32 bytes, got %d", len(event.TransactionHash))
	}

	// Add to builders
	b.builder.Field(0).(*array.StringBuilder).Append(event.EventID)
	b.builder.Field(1).(*array.Uint32Builder).Append(event.LedgerSequence)
	b.builder.Field(2).(*array.FixedSizeBinaryBuilder).Append(event.TransactionHash)
	b.builder.Field(3).(*array.Uint32Builder).Append(event.OperationIndex)
	b.builder.Field(4).(*array.TimestampBuilder).Append(arrow.Timestamp(event.Timestamp.UnixMicro()))
	b.builder.Field(5).(*array.TimestampBuilder).Append(arrow.Timestamp(time.Now().UnixMicro()))
	b.builder.Field(6).(*array.StringBuilder).Append(event.EventType)
	b.builder.Field(7).(*array.StringBuilder).Append(event.AssetType)

	// Handle nullable asset fields
	if event.AssetCode != nil {
		b.builder.Field(8).(*array.StringBuilder).Append(*event.AssetCode)
	} else {
		b.builder.Field(8).(*array.StringBuilder).AppendNull()
	}

	if event.AssetIssuer != nil {
		b.builder.Field(9).(*array.StringBuilder).Append(*event.AssetIssuer)
	} else {
		b.builder.Field(9).(*array.StringBuilder).AppendNull()
	}

	b.builder.Field(10).(*array.StringBuilder).Append(event.FromAccount)
	b.builder.Field(11).(*array.StringBuilder).Append(event.ToAccount)
	b.builder.Field(12).(*array.Int64Builder).Append(event.AmountRaw)
	b.builder.Field(13).(*array.StringBuilder).Append(event.AmountStr)

	// Handle path payment fields
	if event.SourceAssetType != nil {
		b.builder.Field(14).(*array.StringBuilder).Append(*event.SourceAssetType)
	} else {
		b.builder.Field(14).(*array.StringBuilder).AppendNull()
	}

	if event.SourceAssetCode != nil {
		b.builder.Field(15).(*array.StringBuilder).Append(*event.SourceAssetCode)
	} else {
		b.builder.Field(15).(*array.StringBuilder).AppendNull()
	}

	if event.SourceAssetIssuer != nil {
		b.builder.Field(16).(*array.StringBuilder).Append(*event.SourceAssetIssuer)
	} else {
		b.builder.Field(16).(*array.StringBuilder).AppendNull()
	}

	if event.SourceAmountRaw != nil {
		b.builder.Field(17).(*array.Int64Builder).Append(*event.SourceAmountRaw)
	} else {
		b.builder.Field(17).(*array.Int64Builder).AppendNull()
	}

	if event.SourceAmountStr != nil {
		b.builder.Field(18).(*array.StringBuilder).Append(*event.SourceAmountStr)
	} else {
		b.builder.Field(18).(*array.StringBuilder).AppendNull()
	}

	b.builder.Field(19).(*array.BooleanBuilder).Append(event.Successful)

	// Handle memo fields
	if event.MemoType != nil {
		b.builder.Field(20).(*array.StringBuilder).Append(*event.MemoType)
	} else {
		b.builder.Field(20).(*array.StringBuilder).AppendNull()
	}

	if event.MemoValue != nil {
		b.builder.Field(21).(*array.StringBuilder).Append(*event.MemoValue)
	} else {
		b.builder.Field(21).(*array.StringBuilder).AppendNull()
	}

	b.builder.Field(22).(*array.Int64Builder).Append(event.FeeCharged)
	b.builder.Field(23).(*array.StringBuilder).Append(event.ProcessorVersion)
	b.builder.Field(24).(*array.StringBuilder).Append(TTPEventSchemaVersion)

	return nil
}

// NewRecord creates a new Arrow record from the builder
func (b *TTPEventBuilder) NewRecord() arrow.Record {
	return b.builder.NewRecord()
}

// Release releases the builder resources
func (b *TTPEventBuilder) Release() {
	b.builder.Release()
}

// SchemaEvolution provides utilities for handling schema changes
type SchemaEvolution struct {
	registry *SchemaRegistry
}

// NewSchemaEvolution creates a new schema evolution manager
func NewSchemaEvolution(registry *SchemaRegistry) *SchemaEvolution {
	return &SchemaEvolution{registry: registry}
}

// CanUpgrade checks if a record can be upgraded to a newer schema version
func (se *SchemaEvolution) CanUpgrade(record arrow.Record, targetSchemaName string) bool {
	targetSchema, exists := se.registry.GetSchema(targetSchemaName)
	if !exists {
		return false
	}

	currentVersion := GetSchemaVersion(record.Schema())
	targetVersion := GetSchemaVersion(targetSchema)

	// For now, we support upgrading within the same major version
	return currentVersion <= targetVersion
}

// MigrateRecord migrates a record to a newer schema version
func (se *SchemaEvolution) MigrateRecord(record arrow.Record, targetSchemaName string) (arrow.Record, error) {
	if !se.CanUpgrade(record, targetSchemaName) {
		return nil, fmt.Errorf("cannot upgrade record to target schema")
	}

	targetSchema, _ := se.registry.GetSchema(targetSchemaName)
	
	// For now, return the same record if schemas are compatible
	// In the future, this would handle field additions, renames, etc.
	if arrow.SchemaEqual(record.Schema(), targetSchema) {
		return record, nil
	}

	return nil, fmt.Errorf("schema migration not implemented for this version")
}

// Utility functions for common operations

// EncodeXDRBase64 encodes XDR data as base64 string
func EncodeXDRBase64(xdrData []byte) string {
	return base64.StdEncoding.EncodeToString(xdrData)
}

// DecodeXDRBase64 decodes base64 string to XDR data
func DecodeXDRBase64(base64Data string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(base64Data)
}

// FormatAmount formats a stroops amount to a decimal string
func FormatAmount(stroops int64) string {
	// Stellar amounts are in stroops (1 XLM = 10,000,000 stroops)
	xlm := float64(stroops) / 10000000.0
	return fmt.Sprintf("%.7f", xlm)
}

// ParseAmount parses a decimal amount string to stroops
func ParseAmount(amountStr string) (int64, error) {
	var xlm float64
	if _, err := fmt.Sscanf(amountStr, "%f", &xlm); err != nil {
		return 0, err
	}
	return int64(xlm * 10000000), nil
}

// GenerateEventID creates a unique event ID
func GenerateEventID(ledgerSeq uint32, txHash []byte, opIndex uint32) string {
	return fmt.Sprintf("%d:%x:%d", ledgerSeq, txHash, opIndex)
}