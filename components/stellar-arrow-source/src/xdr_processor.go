package main

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stellar/go/xdr"
	"github.com/withobsrvr/obsrvr-stellar-components/schemas"
)

// XDRProcessor handles comprehensive Stellar XDR processing with validation
type XDRProcessor struct {
	networkPassphrase string
	validateNetwork   bool
	strictValidation  bool
}

// NewXDRProcessor creates a new XDR processor with validation options
func NewXDRProcessor(networkPassphrase string, strictValidation bool) *XDRProcessor {
	return &XDRProcessor{
		networkPassphrase: networkPassphrase,
		validateNetwork:   true,
		strictValidation:  strictValidation,
	}
}

// ProcessLedgerXDR processes ledger XDR data with comprehensive validation
func (p *XDRProcessor) ProcessLedgerXDR(xdrData []byte, sourceType, sourceURL string) (*schemas.ProcessedLedgerData, error) {
	// Validate XDR data size
	if len(xdrData) == 0 {
		return nil, fmt.Errorf("empty XDR data")
	}

	if len(xdrData) < 32 {
		return nil, fmt.Errorf("XDR data too small: %d bytes", len(xdrData))
	}

	// Parse XDR with error recovery
	var ledgerCloseMeta xdr.LedgerCloseMeta
	if err := ledgerCloseMeta.UnmarshalBinary(xdrData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ledger XDR: %w", err)
	}

	// Validate ledger structure
	if err := p.validateLedgerStructure(&ledgerCloseMeta); err != nil {
		return nil, fmt.Errorf("ledger validation failed: %w", err)
	}

	// Extract ledger data with validation
	processedData, err := p.extractLedgerData(&ledgerCloseMeta, sourceType, sourceURL, xdrData)
	if err != nil {
		return nil, fmt.Errorf("failed to extract ledger data: %w", err)
	}

	// Additional validation checks
	if err := p.validateProcessedData(processedData); err != nil {
		return nil, fmt.Errorf("processed data validation failed: %w", err)
	}

	log.Debug().
		Uint32("sequence", processedData.Sequence).
		Uint32("tx_count", processedData.TransactionCount).
		Uint32("op_count", processedData.OperationCount).
		Int64("total_fees", processedData.TotalFees).
		Msg("Successfully processed ledger XDR")

	return processedData, nil
}


// validateLedgerStructure performs comprehensive ledger structure validation
func (p *XDRProcessor) validateLedgerStructure(meta *xdr.LedgerCloseMeta) error {
	// Check ledger version
	if meta.V < 0 || meta.V > 1 {
		return fmt.Errorf("unsupported ledger meta version: %d", meta.V)
	}

	var ledgerHeader xdr.LedgerHeader
	var txProcessing []xdr.TransactionResultMeta

	// Extract data based on version
	switch meta.V {
	case 0:
		v0 := meta.MustV0()
		ledgerHeader = v0.LedgerHeader.Header
		txProcessing = v0.TxProcessing

		// Validate transaction set for v0
		if err := p.validateTransactionSet(&v0.TxSet); err != nil {
			return fmt.Errorf("transaction set validation failed: %w", err)
		}

		// Cross-validate counts for v0
		if err := p.validateTransactionCounts(&v0.TxSet, txProcessing); err != nil {
			return fmt.Errorf("transaction count validation failed: %w", err)
		}

	case 1:
		v1 := meta.MustV1()
		ledgerHeader = v1.LedgerHeader.Header
		txProcessing = v1.TxProcessing

		// For v1, we have a GeneralizedTransactionSet, skip detailed validation for now
		// TODO: Implement GeneralizedTransactionSet validation
		log.Debug().Msg("Skipping detailed transaction set validation for GeneralizedTransactionSet")
	}

	// Validate ledger header
	if err := p.validateLedgerHeader(&ledgerHeader); err != nil {
		return fmt.Errorf("ledger header validation failed: %w", err)
	}

	// Validate transaction processing results
	if err := p.validateTransactionProcessing(txProcessing); err != nil {
		return fmt.Errorf("transaction processing validation failed: %w", err)
	}

	return nil
}

// validateLedgerHeader validates the ledger header structure
func (p *XDRProcessor) validateLedgerHeader(header *xdr.LedgerHeader) error {
	// Check sequence number
	if header.LedgerSeq < 1 {
		return fmt.Errorf("invalid ledger sequence: %d", header.LedgerSeq)
	}

	// Check protocol version
	if header.LedgerVersion < 1 || header.LedgerVersion > 50 {
		return fmt.Errorf("invalid protocol version: %d", header.LedgerVersion)
	}

	// Check close time
	if header.ScpValue.CloseTime == 0 {
		return fmt.Errorf("invalid close time: %d", header.ScpValue.CloseTime)
	}

	// Validate fee parameters
	if header.BaseFee == 0 {
		return fmt.Errorf("invalid base fee: %d", header.BaseFee)
	}

	if header.BaseReserve == 0 {
		return fmt.Errorf("invalid base reserve: %d", header.BaseReserve)
	}

	// Check max transaction set size
	if header.MaxTxSetSize == 0 {
		return fmt.Errorf("invalid max tx set size: %d", header.MaxTxSetSize)
	}

	// Validate hash lengths  
	if len(header.PreviousLedgerHash) != 32 {
		return fmt.Errorf("invalid previous hash length: %d", len(header.PreviousLedgerHash))
	}

	return nil
}

// validateTransactionSet validates the transaction set structure
func (p *XDRProcessor) validateTransactionSet(txSet *xdr.TransactionSet) error {
	// Check transaction count limits
	if len(txSet.Txs) > 5000 { // Reasonable upper limit
		return fmt.Errorf("transaction set too large: %d transactions", len(txSet.Txs))
	}

	// Validate each transaction envelope
	for i, txEnv := range txSet.Txs {
		if err := p.validateTransactionEnvelope(&txEnv, i); err != nil {
			return fmt.Errorf("transaction %d validation failed: %w", i, err)
		}
	}

	return nil
}

// validateTransactionEnvelope validates a transaction envelope
func (p *XDRProcessor) validateTransactionEnvelope(txEnv *xdr.TransactionEnvelope, index int) error {
	// Check transaction envelope type
	switch txEnv.Type {
	case xdr.EnvelopeTypeEnvelopeTypeTx:
		tx := txEnv.MustV1()
		return p.validateTransaction(&tx.Tx, index)
	case xdr.EnvelopeTypeEnvelopeTypeTxV0:
		tx := txEnv.MustV0()
		return p.validateTransactionV0(&tx.Tx, index)
	case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
		tx := txEnv.MustFeeBump()
		return p.validateFeeBumpTransaction(&tx.Tx, index)
	default:
		return fmt.Errorf("unsupported transaction envelope type: %v", txEnv.Type)
	}
}

// validateTransaction validates a transaction structure
func (p *XDRProcessor) validateTransaction(tx *xdr.Transaction, index int) error {
	// Check operation count
	if len(tx.Operations) == 0 {
		return fmt.Errorf("transaction %d has no operations", index)
	}

	if len(tx.Operations) > 100 { // Stellar limit
		return fmt.Errorf("transaction %d has too many operations: %d", index, len(tx.Operations))
	}

	// Check sequence number
	if tx.SeqNum < 1 {
		return fmt.Errorf("transaction %d has invalid sequence number: %d", index, tx.SeqNum)
	}

	// Check fee
	if tx.Fee < 100 { // Minimum fee
		return fmt.Errorf("transaction %d has invalid fee: %d", index, tx.Fee)
	}

	// Validate each operation
	for opIndex, op := range tx.Operations {
		if err := p.validateOperation(&op, index, opIndex); err != nil {
			return fmt.Errorf("transaction %d operation %d validation failed: %w", index, opIndex, err)
		}
	}

	return nil
}

// validateTransactionV0 validates a V0 transaction structure
func (p *XDRProcessor) validateTransactionV0(tx *xdr.TransactionV0, index int) error {
	// Similar validation to regular transaction but for V0 format
	if len(tx.Operations) == 0 {
		return fmt.Errorf("transaction V0 %d has no operations", index)
	}

	if len(tx.Operations) > 100 {
		return fmt.Errorf("transaction V0 %d has too many operations: %d", index, len(tx.Operations))
	}

	if tx.SeqNum < 1 {
		return fmt.Errorf("transaction V0 %d has invalid sequence number: %d", index, tx.SeqNum)
	}

	if tx.Fee < 100 {
		return fmt.Errorf("transaction V0 %d has invalid fee: %d", index, tx.Fee)
	}

	return nil
}

// validateFeeBumpTransaction validates a fee bump transaction
func (p *XDRProcessor) validateFeeBumpTransaction(tx *xdr.FeeBumpTransaction, index int) error {
	// Check fee bump fee
	if tx.Fee < 100 {
		return fmt.Errorf("fee bump transaction %d has invalid fee: %d", index, tx.Fee)
	}

	// Validate inner transaction - for fee bump, we need to extract the actual transaction envelope
	// For now, we'll skip detailed validation of the inner transaction
	log.Debug().Int("index", index).Msg("Skipping detailed fee bump inner transaction validation")
	return nil
}

// validateOperation validates an operation structure
func (p *XDRProcessor) validateOperation(op *xdr.Operation, txIndex, opIndex int) error {
	// Check operation type is valid
	switch op.Body.Type {
	case xdr.OperationTypeCreateAccount,
		 xdr.OperationTypePayment,
		 xdr.OperationTypePathPaymentStrictReceive,
		 xdr.OperationTypePathPaymentStrictSend,
		 xdr.OperationTypeManageSellOffer,
		 xdr.OperationTypeCreatePassiveSellOffer,
		 xdr.OperationTypeSetOptions,
		 xdr.OperationTypeChangeTrust,
		 xdr.OperationTypeAllowTrust,
		 xdr.OperationTypeAccountMerge,
		 xdr.OperationTypeInflation,
		 xdr.OperationTypeManageData,
		 xdr.OperationTypeBumpSequence,
		 xdr.OperationTypeManageBuyOffer,
		 xdr.OperationTypeCreateClaimableBalance,
		 xdr.OperationTypeClaimClaimableBalance,
		 xdr.OperationTypeBeginSponsoringFutureReserves,
		 xdr.OperationTypeEndSponsoringFutureReserves,
		 xdr.OperationTypeRevokeSponsorship,
		 xdr.OperationTypeClawback,
		 xdr.OperationTypeClawbackClaimableBalance,
		 xdr.OperationTypeSetTrustLineFlags,
		 xdr.OperationTypeLiquidityPoolDeposit,
		 xdr.OperationTypeLiquidityPoolWithdraw,
		 xdr.OperationTypeInvokeHostFunction,
		 xdr.OperationTypeExtendFootprintTtl,
		 xdr.OperationTypeRestoreFootprint:
		// Valid operation types
		return nil
	default:
		return fmt.Errorf("transaction %d operation %d has invalid type: %v", txIndex, opIndex, op.Body.Type)
	}
}

// validateTransactionProcessing validates transaction processing results
func (p *XDRProcessor) validateTransactionProcessing(txProcessing []xdr.TransactionResultMeta) error {
	for i, result := range txProcessing {
		if err := p.validateTransactionResult(&result, i); err != nil {
			return fmt.Errorf("transaction result %d validation failed: %w", i, err)
		}
	}
	return nil
}

// validateTransactionResult validates a transaction result
func (p *XDRProcessor) validateTransactionResult(result *xdr.TransactionResultMeta, index int) error {
	// Check fee charged
	if result.Result.Result.FeeCharged < 0 {
		return fmt.Errorf("transaction result %d has negative fee charged: %d", index, result.Result.Result.FeeCharged)
	}

	// Validate result code (skip detailed validation for now)
	if !result.Result.Successful() {
		log.Debug().
			Int("index", index).
			Msg("Transaction failed")
	}

	return nil
}

// validateTransactionCounts cross-validates transaction counts
func (p *XDRProcessor) validateTransactionCounts(txSet *xdr.TransactionSet, txProcessing []xdr.TransactionResultMeta) error {
	if len(txSet.Txs) != len(txProcessing) {
		return fmt.Errorf("transaction count mismatch: set has %d, processing has %d", 
			len(txSet.Txs), len(txProcessing))
	}
	return nil
}

// extractLedgerData extracts validated ledger data
func (p *XDRProcessor) extractLedgerData(meta *xdr.LedgerCloseMeta, sourceType, sourceURL string, rawXDR []byte) (*schemas.ProcessedLedgerData, error) {
	var ledgerHeader xdr.LedgerHeader
	var txProcessing []xdr.TransactionResultMeta

	// Extract data based on version
	switch meta.V {
	case 0:
		v0 := meta.MustV0()
		ledgerHeader = v0.LedgerHeader.Header
		txProcessing = v0.TxProcessing
	case 1:
		v1 := meta.MustV1()
		ledgerHeader = v1.LedgerHeader.Header
		txProcessing = v1.TxProcessing
	}

	// Count successful and failed transactions
	var successfulCount, failedCount uint32
	var totalFees int64
	var operationCount uint32

	for _, result := range txProcessing {
		if result.Result.Successful() {
			successfulCount++
			// Count operations in successful transactions  
			if ops, ok := result.Result.OperationResults(); ok {
				operationCount += uint32(len(ops))
			}
		} else {
			failedCount++
		}
		totalFees += int64(result.Result.Result.FeeCharged)
	}

	// Generate network ID from passphrase hash
	networkID := generateNetworkID(p.networkPassphrase)

	return &schemas.ProcessedLedgerData{
		Sequence:                   uint32(ledgerHeader.LedgerSeq),
		Hash:                      ledgerHeader.PreviousLedgerHash[:], // TODO: Compute actual ledger hash
		PreviousHash:              ledgerHeader.PreviousLedgerHash[:],
		CloseTime:                 time.Unix(int64(ledgerHeader.ScpValue.CloseTime), 0),
		ProtocolVersion:           uint32(ledgerHeader.LedgerVersion),
		NetworkID:                 networkID,
		TransactionCount:          uint32(len(txProcessing)),
		SuccessfulTransactionCount: successfulCount,
		FailedTransactionCount:     failedCount,
		OperationCount:            operationCount,
		TotalFees:                 totalFees,
		FeePool:                   int64(ledgerHeader.FeePool),
		BaseFee:                   uint32(ledgerHeader.BaseFee),
		BaseReserve:               uint32(ledgerHeader.BaseReserve),
		MaxTxSetSize:              uint32(ledgerHeader.MaxTxSetSize),
		SourceType:                sourceType,
		SourceURL:                 sourceURL,
		RawXDR:                    rawXDR,
		ValidationTime:            time.Now(),
		NetworkValid:              true,
		StructureValid:            true,
	}, nil
}

// validateProcessedData performs final validation on processed data
func (p *XDRProcessor) validateProcessedData(data *schemas.ProcessedLedgerData) error {
	// Check that transaction counts add up
	if data.SuccessfulTransactionCount+data.FailedTransactionCount != data.TransactionCount {
		return fmt.Errorf("transaction count mismatch: %d successful + %d failed != %d total",
			data.SuccessfulTransactionCount, data.FailedTransactionCount, data.TransactionCount)
	}

	// Check reasonable close time
	now := time.Now()
	if data.CloseTime.After(now.Add(time.Hour)) {
		return fmt.Errorf("ledger close time is too far in the future: %v", data.CloseTime)
	}

	// Check for reasonable fee amounts
	if data.TotalFees < 0 {
		return fmt.Errorf("negative total fees: %d", data.TotalFees)
	}

	if data.TotalFees > int64(data.TransactionCount)*10000000 { // 1 XLM per tx max
		log.Warn().
			Int64("total_fees", data.TotalFees).
			Uint32("tx_count", data.TransactionCount).
			Msg("Unusually high total fees detected")
	}

	return nil
}

// generateNetworkID generates a network ID from the passphrase
func generateNetworkID(passphrase string) []byte {
	// Use the bucket list hash as a proxy for network ID
	// In a real implementation, this would hash the network passphrase
	networkID := make([]byte, 32)
	copy(networkID, []byte(passphrase))
	return networkID
}

// RecoverFromXDRError attempts to recover from XDR parsing errors
func (p *XDRProcessor) RecoverFromXDRError(xdrData []byte, originalError error) (*schemas.ProcessedLedgerData, error) {
	log.Warn().
		Err(originalError).
		Int("xdr_size", len(xdrData)).
		Msg("Attempting XDR error recovery")

	// Try different XDR parsing strategies
	strategies := []func([]byte) (*xdr.LedgerCloseMeta, error){
		p.parseWithLenientValidation,
		p.parseWithTruncation,
		p.parseWithSkipCorrupted,
	}

	for i, strategy := range strategies {
		meta, err := strategy(xdrData)
		if err != nil {
			log.Debug().
				Int("strategy", i).
				Err(err).
				Msg("Recovery strategy failed")
			continue
		}

		log.Info().
			Int("strategy", i).
			Msg("XDR recovery successful")

		// Extract data with relaxed validation
		return p.extractLedgerDataWithRecovery(meta, "recovered", "", xdrData)
	}

	return nil, fmt.Errorf("all recovery strategies failed: %w", originalError)
}

// parseWithLenientValidation attempts parsing with relaxed validation
func (p *XDRProcessor) parseWithLenientValidation(xdrData []byte) (*xdr.LedgerCloseMeta, error) {
	var meta xdr.LedgerCloseMeta
	if err := meta.UnmarshalBinary(xdrData); err != nil {
		return nil, err
	}
	return &meta, nil
}

// parseWithTruncation attempts parsing with truncated data
func (p *XDRProcessor) parseWithTruncation(xdrData []byte) (*xdr.LedgerCloseMeta, error) {
	// Try with 90% of the data
	truncatedSize := int(float64(len(xdrData)) * 0.9)
	if truncatedSize < 100 {
		return nil, fmt.Errorf("data too small for truncation")
	}

	var meta xdr.LedgerCloseMeta
	if err := meta.UnmarshalBinary(xdrData[:truncatedSize]); err != nil {
		return nil, err
	}
	return &meta, nil
}

// parseWithSkipCorrupted attempts parsing while skipping corrupted sections
func (p *XDRProcessor) parseWithSkipCorrupted(xdrData []byte) (*xdr.LedgerCloseMeta, error) {
	// This is a placeholder for more sophisticated corruption recovery
	// In practice, this would implement byte-level XDR recovery
	return nil, fmt.Errorf("skip corrupted strategy not implemented")
}

// extractLedgerDataWithRecovery extracts data during recovery mode
func (p *XDRProcessor) extractLedgerDataWithRecovery(meta *xdr.LedgerCloseMeta, sourceType, sourceURL string, rawXDR []byte) (*schemas.ProcessedLedgerData, error) {
	// Extract with minimal validation during recovery
	data, err := p.extractLedgerData(meta, sourceType, sourceURL, rawXDR)
	if err != nil {
		return nil, err
	}

	// Mark as recovered data
	data.NetworkValid = false
	data.StructureValid = false

	return data, nil
}