package main

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/rs/zerolog/log"
	"github.com/stellar/go/xdr"
	"github.com/withobsrvr/obsrvr-stellar-components/schemas"
)

type XDRToArrowConverter struct {
	pool      memory.Allocator
	builder   *array.RecordBuilder
	schema    *arrow.Schema
	batchSize int
	records   int
}

func NewXDRToArrowConverter(pool memory.Allocator, batchSize int) *XDRToArrowConverter {
	schema := schemas.StellarLedgerSchema
	builder := array.NewRecordBuilder(pool, schema)

	return &XDRToArrowConverter{
		pool:      pool,
		builder:   builder,
		schema:    schema,
		batchSize: batchSize,
		records:   0,
	}
}

func (c *XDRToArrowConverter) ConvertLedger(meta xdr.LedgerCloseMeta, sourceType, sourceURL string) error {
	// Extract ledger header based on version
	var ledgerHeader xdr.LedgerHeader
	var txSet xdr.GeneralizedTransactionSet
	var txProcessing []xdr.TransactionResultMeta

	switch meta.V {
	case 0:
		v0 := meta.MustV0()
		ledgerHeader = v0.LedgerHeader.Header
		// For v0, TxSet is a legacy TransactionSet, we'll handle this differently
		legacyTxSet := v0.TxSet
		// Create an empty GeneralizedTransactionSet for v0 (legacy) processing
		txSet = xdr.GeneralizedTransactionSet{V: 0}
		txProcessing = v0.TxProcessing
		
		// We'll process the legacy transaction set directly where needed
		_ = legacyTxSet // Keep reference for later use
	case 1:
		v1 := meta.MustV1()
		ledgerHeader = v1.LedgerHeader.Header
		txSet = v1.TxSet
		txProcessing = v1.TxProcessing
	default:
		return fmt.Errorf("unsupported LedgerCloseMeta version: %d", meta.V)
	}

	// Extract basic ledger information
	sequence := uint32(ledgerHeader.LedgerSeq)
	
	// Compute ledger hash (simplified - in production would use proper hash)
	hash := ledgerHeader.PreviousLedgerHash // Placeholder
	previousHash := ledgerHeader.PreviousLedgerHash

	closeTime := time.Unix(int64(ledgerHeader.ScpValue.CloseTime), 0)
	protocolVersion := uint32(ledgerHeader.LedgerVersion)
	networkID := ledgerHeader.BucketListHash // Use bucket list hash as network ID

	// Process transactions to get counts and metrics
	var txCount uint32
	switch meta.V {
	case 0:
		// Legacy transaction set - extract from the original v0 data
		v0 := meta.MustV0()
		legacyTxSet := v0.TxSet
		txCount = uint32(len(legacyTxSet.Txs))
	case 1:
		// Generalized transaction set - extract from the txSet we already have
		if txSet.V == 1 {
			// For v1, we need to get the phases count from the generalized transaction set
			// The exact method depends on the Stellar Go SDK version
			// For now, we'll estimate based on transaction processing results
			txCount = uint32(len(txProcessing))
		} else {
			log.Warn().Int32("version", txSet.V).Msg("Unexpected transaction set version in v1 meta")
			txCount = 0
		}
	default:
		log.Warn().Int32("version", meta.V).Msg("Unknown ledger close meta version")
		txCount = 0
	}
	var successfulTxCount, failedTxCount, opCount uint32
	var totalFees int64

	for _, result := range txProcessing {
		if result.Result.Successful() {
			successfulTxCount++
		} else {
			failedTxCount++
		}

		// Count operations in this transaction
		if ops, ok := result.Result.OperationResults(); ok {
			opCount += uint32(len(ops))
		}

		// Get fee from the transaction result
		totalFees += int64(result.Result.Result.FeeCharged)
	}

	// Extract fee information from header
	feePool := int64(ledgerHeader.FeePool)
	baseFee := uint32(ledgerHeader.BaseFee)
	baseReserve := uint32(ledgerHeader.BaseReserve)
	maxTxSetSize := uint32(ledgerHeader.MaxTxSetSize)

	// Serialize XDR for storage
	xdrBytes, err := meta.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal XDR: %w", err)
	}

	// Add to Arrow builder
	c.addLedgerToBuilder(
		sequence, hash[:], previousHash[:], closeTime,
		protocolVersion, networkID[:],
		txCount, successfulTxCount, failedTxCount, opCount,
		totalFees, feePool, baseFee, baseReserve, maxTxSetSize,
		xdrBytes, sourceType, sourceURL,
	)

	c.records++
	log.Debug().
		Uint32("sequence", sequence).
		Int("records_in_batch", c.records).
		Msg("Converted ledger to Arrow format")

	return nil
}

func (c *XDRToArrowConverter) addLedgerToBuilder(
	sequence uint32, hash, previousHash []byte, closeTime time.Time,
	protocolVersion uint32, networkID []byte,
	txCount, successfulTxCount, failedTxCount, opCount uint32,
	totalFees, feePool int64, baseFee, baseReserve, maxTxSetSize uint32,
	xdrData []byte, sourceType, sourceURL string,
) {
	// Convert to Arrow format following the schema order
	// Field 0: sequence
	c.builder.Field(0).(*array.Uint32Builder).Append(sequence)

	// Field 1: hash (32 bytes)
	c.builder.Field(1).(*array.FixedSizeBinaryBuilder).Append(hash)

	// Field 2: previous_hash (32 bytes)
	c.builder.Field(2).(*array.FixedSizeBinaryBuilder).Append(previousHash)

	// Field 3: close_time (timestamp)
	closeTimeArrow := arrow.Timestamp(closeTime.UnixMicro())
	c.builder.Field(3).(*array.TimestampBuilder).Append(closeTimeArrow)

	// Field 4: ingestion_time (timestamp)
	ingestionTimeArrow := arrow.Timestamp(time.Now().UnixMicro())
	c.builder.Field(4).(*array.TimestampBuilder).Append(ingestionTimeArrow)

	// Field 5: protocol_version
	c.builder.Field(5).(*array.Uint32Builder).Append(protocolVersion)

	// Field 6: network_id (32 bytes)
	c.builder.Field(6).(*array.FixedSizeBinaryBuilder).Append(networkID)

	// Field 7: transaction_count
	c.builder.Field(7).(*array.Uint32Builder).Append(txCount)

	// Field 8: successful_transaction_count
	c.builder.Field(8).(*array.Uint32Builder).Append(successfulTxCount)

	// Field 9: failed_transaction_count
	c.builder.Field(9).(*array.Uint32Builder).Append(failedTxCount)

	// Field 10: operation_count
	c.builder.Field(10).(*array.Uint32Builder).Append(opCount)

	// Field 11: total_fees
	c.builder.Field(11).(*array.Int64Builder).Append(totalFees)

	// Field 12: fee_pool
	c.builder.Field(12).(*array.Int64Builder).Append(feePool)

	// Field 13: base_fee
	c.builder.Field(13).(*array.Uint32Builder).Append(baseFee)

	// Field 14: base_reserve
	c.builder.Field(14).(*array.Uint32Builder).Append(baseReserve)

	// Field 15: max_tx_set_size
	c.builder.Field(15).(*array.Uint32Builder).Append(maxTxSetSize)

	// Field 16: ledger_xdr (binary)
	c.builder.Field(16).(*array.BinaryBuilder).Append(xdrData)

	// Field 17: source_type (string)
	c.builder.Field(17).(*array.StringBuilder).Append(sourceType)

	// Field 18: source_url (string)
	c.builder.Field(18).(*array.StringBuilder).Append(sourceURL)

	// Field 19: schema_version (string)
	c.builder.Field(19).(*array.StringBuilder).Append(schemas.StellarLedgerSchemaVersion)
}

func (c *XDRToArrowConverter) BuildRecord() (arrow.Record, error) {
	if c.records == 0 {
		return nil, fmt.Errorf("no records to build")
	}

	record := c.builder.NewRecord()
	log.Info().
		Int("records_built", c.records).
		Int64("num_rows", record.NumRows()).
		Msg("Built Arrow record from XDR data")

	return record, nil
}

func (c *XDRToArrowConverter) ShouldFlush() bool {
	return c.records >= c.batchSize
}

func (c *XDRToArrowConverter) Reset() {
	// Reset builder for next batch
	for i := 0; i < c.schema.NumFields(); i++ {
		c.builder.Field(i).Release()
	}
	c.builder = array.NewRecordBuilder(c.pool, c.schema)
	c.records = 0
	log.Debug().Msg("Reset XDR to Arrow converter for next batch")
}

func (c *XDRToArrowConverter) GetRecordCount() int {
	return c.records
}

func (c *XDRToArrowConverter) Release() {
	if c.builder != nil {
		for i := 0; i < c.schema.NumFields(); i++ {
			c.builder.Field(i).Release()
		}
	}
	log.Debug().Msg("Released XDR to Arrow converter resources")
}

// ConvertLedgerBatch converts multiple ledgers in a single batch
func (c *XDRToArrowConverter) ConvertLedgerBatch(ledgers []LedgerResult, sourceType, sourceURL string) (arrow.Record, error) {
	for _, ledger := range ledgers {
		if ledger.Error != nil {
			log.Error().Err(ledger.Error).Uint32("sequence", ledger.Sequence).Msg("Skipping ledger with error")
			continue
		}

		if err := c.ConvertLedger(ledger.LedgerMeta, sourceType, sourceURL); err != nil {
			log.Error().Err(err).Uint32("sequence", ledger.Sequence).Msg("Failed to convert ledger")
			continue
		}
	}

	if c.records == 0 {
		return nil, fmt.Errorf("no valid ledgers to convert")
	}

	record, err := c.BuildRecord()
	if err != nil {
		return nil, fmt.Errorf("failed to build Arrow record: %w", err)
	}

	c.Reset()
	return record, nil
}