package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/rs/zerolog/log"
	"github.com/stellar/go/amount"
	"github.com/stellar/go/xdr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/withobsrvr/obsrvr-stellar-components/schemas"
)

// TTPProcessorService processes Stellar ledger data to extract TTP events
type TTPProcessorService struct {
	config   *Config
	pool     memory.Allocator
	registry *schemas.SchemaRegistry

	// Source connection
	sourceClient flight.FlightServiceClient
	sourceConn   *grpc.ClientConn

	// Processing state
	mu             sync.RWMutex
	isRunning      bool
	currentLedger  uint32
	recordsChannel chan arrow.Record
	errorChannel   chan error

	// Event deduplication
	seenEvents map[string]bool
	eventsMu   sync.RWMutex

	// Processing statistics
	stats     ProcessingStats
	statsMu   sync.RWMutex

	// Shutdown
	shutdownOnce sync.Once
}

// ProcessingStats tracks processing statistics
type ProcessingStats struct {
	LedgersProcessed   uint64
	TransactionsProcessed uint64
	OperationsProcessed uint64
	EventsExtracted    uint64
	EventsFiltered     uint64
	BatchesGenerated   uint64
	StartTime          time.Time
	LastProcessedLedger uint32
}

// NewTTPProcessorService creates a new TTP processor service
func NewTTPProcessorService(config *Config, pool memory.Allocator, registry *schemas.SchemaRegistry) (*TTPProcessorService, error) {
	service := &TTPProcessorService{
		config:         config,
		pool:           pool,
		registry:       registry,
		recordsChannel: make(chan arrow.Record, config.BufferSize),
		errorChannel:   make(chan error, 10),
		seenEvents:     make(map[string]bool),
		stats:          ProcessingStats{StartTime: time.Now()},
	}

	// Connect to source
	if err := service.connectToSource(); err != nil {
		return nil, fmt.Errorf("failed to connect to source: %w", err)
	}

	return service, nil
}

// connectToSource establishes connection to the stellar-arrow-source component
func (s *TTPProcessorService) connectToSource() error {
	log.Info().
		Str("endpoint", s.config.SourceEndpoint).
		Msg("Connecting to stellar-arrow-source")

	// Create gRPC connection
	conn, err := grpc.Dial(s.config.SourceEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}

	s.sourceConn = conn
	s.sourceClient = flight.NewFlightServiceClient(conn)

	log.Info().Msg("Connected to stellar-arrow-source")
	return nil
}

// Start begins processing ledgers from the source
func (s *TTPProcessorService) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return fmt.Errorf("service is already running")
	}
	s.isRunning = true
	s.mu.Unlock()

	log.Info().
		Str("source_endpoint", s.config.SourceEndpoint).
		Strs("event_types", s.config.EventTypes).
		Int("processor_threads", s.config.ProcessorThreads).
		Msg("Starting TTP event processing")

	// Start worker pool
	for i := 0; i < s.config.ProcessorThreads; i++ {
		go s.processingWorker(ctx, i)
	}

	// Start consuming from source
	return s.consumeFromSource(ctx)
}

// Stop gracefully stops the service
func (s *TTPProcessorService) Stop(ctx context.Context) {
	s.shutdownOnce.Do(func() {
		s.mu.Lock()
		s.isRunning = false
		s.mu.Unlock()

		log.Info().Msg("Stopping TTP processor service")

		// Close channels
		close(s.recordsChannel)
		close(s.errorChannel)

		// Close source connection
		if s.sourceConn != nil {
			s.sourceConn.Close()
		}

		// Log final stats
		s.logFinalStats()

		log.Info().Msg("TTP processor service stopped")
	})
}

// consumeFromSource consumes ledger data from the stellar-arrow-source
func (s *TTPProcessorService) consumeFromSource(ctx context.Context) error {
	// Get flight info for stellar_ledgers stream
	descriptor := &flight.FlightDescriptor{
		Type: flight.FlightDescriptor_PATH,
		Path: []string{"stellar_ledgers"},
	}

	flightInfo, err := s.sourceClient.GetFlightInfo(ctx, descriptor)
	if err != nil {
		return fmt.Errorf("failed to get flight info: %w", err)
	}

	if len(flightInfo.Endpoint) == 0 {
		return fmt.Errorf("no endpoints available")
	}

	// Use the first endpoint's ticket
	ticket := flightInfo.Endpoint[0].Ticket

	log.Info().
		Str("ticket", string(ticket.Ticket)).
		Msg("Starting to consume ledger data from source")

	// Start streaming
	stream, err := s.sourceClient.DoGet(ctx, ticket)
	if err != nil {
		return fmt.Errorf("failed to start stream: %w", err)
	}

	recordCount := 0

	// Process incoming records
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Receive record
		record, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				log.Info().Msg("Source stream ended")
				return nil
			}
			return fmt.Errorf("failed to receive record: %w", err)
		}

		recordCount++
		
		// Convert to Arrow record
		arrowRecord, err := flight.DeserializeRecord(record, s.pool)
		if err != nil {
			log.Error().Err(err).Msg("Failed to deserialize record")
			continue
		}

		// Send to processing workers
		select {
		case s.recordsChannel <- arrowRecord:
			queueDepth.WithLabelValues("input").Set(float64(len(s.recordsChannel)))
		case <-ctx.Done():
			arrowRecord.Release()
			return ctx.Err()
		default:
			// Channel full, log warning
			log.Warn().Msg("Processing channel full, dropping record")
			arrowRecord.Release()
		}

		if recordCount%100 == 0 {
			log.Debug().
				Int("records_received", recordCount).
				Msg("Source stream progress")
		}
	}
}

// processingWorker processes ledger records to extract TTP events
func (s *TTPProcessorService) processingWorker(ctx context.Context, workerID int) {
	log.Debug().Int("worker_id", workerID).Msg("Starting processing worker")

	// Create TTP event builder
	eventBuilder := schemas.NewTTPEventBuilder(s.pool)
	defer eventBuilder.Release()

	for {
		select {
		case <-ctx.Done():
			log.Debug().Int("worker_id", workerID).Msg("Processing worker stopped")
			return

		case record, ok := <-s.recordsChannel:
			if !ok {
				log.Debug().Int("worker_id", workerID).Msg("Processing channel closed")
				return
			}

			timer := prometheus.NewTimer(processingDuration.WithLabelValues("ledger"))

			// Process the ledger record
			eventsExtracted := s.processLedgerRecord(record, eventBuilder)

			record.Release()
			timer.ObserveDuration()

			// Update statistics
			s.statsMu.Lock()
			s.stats.LedgersProcessed++
			s.stats.EventsExtracted += uint64(eventsExtracted)
			s.statsMu.Unlock()

			ledgersProcessed.WithLabelValues("success").Inc()

			// Send batch if we have enough events
			if eventsExtracted > 0 {
				eventRecord := eventBuilder.NewRecord()
				
				select {
				case s.recordsChannel <- eventRecord:
					batchesGenerated.WithLabelValues("events").Inc()
					s.statsMu.Lock()
					s.stats.BatchesGenerated++
					s.statsMu.Unlock()
				case <-ctx.Done():
					eventRecord.Release()
					return
				default:
					log.Warn().Msg("Output channel full, dropping events batch")
					eventRecord.Release()
				}

				// Create new builder for next batch
				eventBuilder.Release()
				eventBuilder = schemas.NewTTPEventBuilder(s.pool)
			}
		}
	}
}

// processLedgerRecord processes a single ledger record to extract TTP events
func (s *TTPProcessorService) processLedgerRecord(record arrow.Record, eventBuilder *schemas.TTPEventBuilder) int {
	eventsExtracted := 0

	// Get the ledger XDR column
	xdrColumn := record.Column(16).(*array.Binary) // ledger_xdr field
	sequenceColumn := record.Column(0).(*array.Uint32) // sequence field
	closeTimeColumn := record.Column(3).(*array.Timestamp) // close_time field

	// Process each ledger in the batch
	for i := 0; i < int(record.NumRows()); i++ {
		if xdrColumn.IsNull(i) {
			continue
		}

		sequence := sequenceColumn.Value(i)
		closeTime := time.Unix(0, int64(closeTimeColumn.Value(i))*1000) // Convert from microseconds

		// Update current ledger
		s.updateCurrentLedger(sequence)

		// Parse XDR data
		xdrData := xdrColumn.Value(i)
		var ledgerCloseMeta xdr.LedgerCloseMeta
		if err := ledgerCloseMeta.UnmarshalBinary(xdrData); err != nil {
			log.Error().
				Err(err).
				Uint32("sequence", sequence).
				Msg("Failed to unmarshal ledger XDR")
			continue
		}

		// Extract events from this ledger
		ledgerEvents := s.extractEventsFromLedger(sequence, closeTime, &ledgerCloseMeta)
		
		// Add events to builder
		for _, event := range ledgerEvents {
			if s.shouldIncludeEvent(event) {
				if err := eventBuilder.AddEvent(event); err != nil {
					log.Error().
						Err(err).
						Str("event_id", event.EventID).
						Msg("Failed to add event to builder")
					continue
				}
				eventsExtracted++
				eventsExtracted.WithLabelValues(event.EventType, getAssetCode(event.AssetCode)).Inc()
			} else {
				s.statsMu.Lock()
				s.stats.EventsFiltered++
				s.statsMu.Unlock()
			}
		}
	}

	return eventsExtracted
}

// extractEventsFromLedger extracts TTP events from a ledger
func (s *TTPProcessorService) extractEventsFromLedger(sequence uint32, closeTime time.Time, ledgerCloseMeta *xdr.LedgerCloseMeta) []schemas.TTPEventData {
	var events []schemas.TTPEventData

	// Process each transaction
	txProcessing := ledgerCloseMeta.MustV0().TxProcessing
	txSet := ledgerCloseMeta.MustV0().TxSet

	for txIndex, txResult := range txProcessing {
		if txIndex >= len(txSet.Txs) {
			continue
		}

		tx := txSet.Txs[txIndex]
		
		// Only process successful transactions
		if !txResult.Result.Successful() {
			continue
		}

		s.statsMu.Lock()
		s.stats.TransactionsProcessed++
		s.statsMu.Unlock()

		// Extract transaction hash
		txHash := tx.Hash()

		// Process each operation in the transaction
		for opIndex, op := range tx.Operations() {
			s.statsMu.Lock()
			s.stats.OperationsProcessed++
			s.statsMu.Unlock()

			// Extract events from this operation
			opEvents := s.extractEventsFromOperation(
				sequence, closeTime, txHash[:], uint32(opIndex),
				&op, &tx, txResult.FeeCharged,
			)
			events = append(events, opEvents...)
		}
	}

	return events
}

// extractEventsFromOperation extracts TTP events from a single operation
func (s *TTPProcessorService) extractEventsFromOperation(
	sequence uint32, closeTime time.Time, txHash []byte, opIndex uint32,
	op *xdr.Operation, tx *xdr.Transaction, feeCharged xdr.Uint32,
) []schemas.TTPEventData {
	var events []schemas.TTPEventData

	// Check if this operation type should be processed
	opType := op.Body.Type.String()
	if !s.shouldProcessOperationType(opType) {
		return events
	}

	// Generate event ID
	eventID := schemas.GenerateEventID(sequence, txHash, opIndex)

	// Extract memo information
	var memoType, memoValue *string
	if tx.Memo != nil {
		mType := tx.Memo.Type.String()
		memoType = &mType
		
		switch tx.Memo.Type {
		case xdr.MemoTypeMemoText:
			value := string(tx.Memo.MustText())
			memoValue = &value
		case xdr.MemoTypeMemoId:
			value := fmt.Sprintf("%d", tx.Memo.MustId())
			memoValue = &value
		case xdr.MemoTypeMemoHash:
			value := fmt.Sprintf("%x", tx.Memo.MustHash())
			memoValue = &value
		case xdr.MemoTypeMemoReturn:
			value := fmt.Sprintf("%x", tx.Memo.MustRetHash())
			memoValue = &value
		}
	}

	// Process different operation types
	switch op.Body.Type {
	case xdr.OperationTypePayment:
		if event := s.extractPaymentEvent(eventID, sequence, closeTime, txHash, opIndex, 
			op.Body.MustPaymentOp(), memoType, memoValue, int64(feeCharged)); event != nil {
			events = append(events, *event)
		}

	case xdr.OperationTypePathPaymentStrictReceive:
		if event := s.extractPathPaymentReceiveEvent(eventID, sequence, closeTime, txHash, opIndex,
			op.Body.MustPathPaymentStrictReceiveOp(), memoType, memoValue, int64(feeCharged)); event != nil {
			events = append(events, *event)
		}

	case xdr.OperationTypePathPaymentStrictSend:
		if event := s.extractPathPaymentSendEvent(eventID, sequence, closeTime, txHash, opIndex,
			op.Body.MustPathPaymentStrictSendOp(), memoType, memoValue, int64(feeCharged)); event != nil {
			events = append(events, *event)
		}

	case xdr.OperationTypeCreateAccount:
		if event := s.extractCreateAccountEvent(eventID, sequence, closeTime, txHash, opIndex,
			op.Body.MustCreateAccountOp(), memoType, memoValue, int64(feeCharged)); event != nil {
			events = append(events, *event)
		}

	case xdr.OperationTypeAccountMerge:
		if event := s.extractAccountMergeEvent(eventID, sequence, closeTime, txHash, opIndex,
			op.Body.MustDestination(), memoType, memoValue, int64(feeCharged)); event != nil {
			events = append(events, *event)
		}
	}

	return events
}

// extractPaymentEvent extracts a TTP event from a payment operation
func (s *TTPProcessorService) extractPaymentEvent(
	eventID string, sequence uint32, closeTime time.Time, txHash []byte, opIndex uint32,
	payment xdr.PaymentOp, memoType, memoValue *string, feeCharged int64,
) *schemas.TTPEventData {
	// Extract asset information
	assetType, assetCode, assetIssuer := s.extractAssetInfo(payment.Asset)

	// Extract amount
	amountRaw := int64(payment.Amount)
	amountStr := amount.String(payment.Amount)

	// Create event
	event := &schemas.TTPEventData{
		EventID:          eventID,
		LedgerSequence:   sequence,
		TransactionHash:  txHash,
		OperationIndex:   opIndex,
		Timestamp:        closeTime,
		EventType:        "payment",
		AssetType:        assetType,
		AssetCode:        assetCode,
		AssetIssuer:      assetIssuer,
		FromAccount:      payment.Source.Address(),
		ToAccount:        payment.Destination.Address(),
		AmountRaw:        amountRaw,
		AmountStr:        amountStr,
		Successful:       true, // We only process successful transactions
		MemoType:         memoType,
		MemoValue:        memoValue,
		FeeCharged:       feeCharged,
		ProcessorVersion: ComponentVersion,
	}

	return event
}

// extractPathPaymentReceiveEvent extracts a TTP event from a path payment strict receive operation
func (s *TTPProcessorService) extractPathPaymentReceiveEvent(
	eventID string, sequence uint32, closeTime time.Time, txHash []byte, opIndex uint32,
	pathPayment xdr.PathPaymentStrictReceiveOp, memoType, memoValue *string, feeCharged int64,
) *schemas.TTPEventData {
	// Extract destination asset (what was received)
	assetType, assetCode, assetIssuer := s.extractAssetInfo(pathPayment.DestAsset)

	// Extract source asset (what was sent)
	sourceAssetType, sourceAssetCode, sourceAssetIssuer := s.extractAssetInfo(pathPayment.SendAsset)

	// Amounts
	amountRaw := int64(pathPayment.DestAmount)
	amountStr := amount.String(pathPayment.DestAmount)
	
	// We don't know the exact source amount sent, so we use the max
	sourceAmountRaw := int64(pathPayment.SendMax)
	sourceAmountStr := amount.String(pathPayment.SendMax)

	event := &schemas.TTPEventData{
		EventID:           eventID,
		LedgerSequence:    sequence,
		TransactionHash:   txHash,
		OperationIndex:    opIndex,
		Timestamp:         closeTime,
		EventType:         "path_payment_strict_receive",
		AssetType:         assetType,
		AssetCode:         assetCode,
		AssetIssuer:       assetIssuer,
		FromAccount:       pathPayment.Source.Address(),
		ToAccount:         pathPayment.Destination.Address(),
		AmountRaw:         amountRaw,
		AmountStr:         amountStr,
		SourceAssetType:   &sourceAssetType,
		SourceAssetCode:   sourceAssetCode,
		SourceAssetIssuer: sourceAssetIssuer,
		SourceAmountRaw:   &sourceAmountRaw,
		SourceAmountStr:   &sourceAmountStr,
		Successful:        true,
		MemoType:          memoType,
		MemoValue:         memoValue,
		FeeCharged:        feeCharged,
		ProcessorVersion:  ComponentVersion,
	}

	return event
}

// extractPathPaymentSendEvent extracts a TTP event from a path payment strict send operation
func (s *TTPProcessorService) extractPathPaymentSendEvent(
	eventID string, sequence uint32, closeTime time.Time, txHash []byte, opIndex uint32,
	pathPayment xdr.PathPaymentStrictSendOp, memoType, memoValue *string, feeCharged int64,
) *schemas.TTPEventData {
	// Extract destination asset (what was received)
	assetType, assetCode, assetIssuer := s.extractAssetInfo(pathPayment.DestAsset)

	// Extract source asset (what was sent)
	sourceAssetType, sourceAssetCode, sourceAssetIssuer := s.extractAssetInfo(pathPayment.SendAsset)

	// Amounts - for strict send, we know the send amount but not exact receive amount
	amountRaw := int64(pathPayment.DestMin) // Minimum that was expected to be received
	amountStr := amount.String(pathPayment.DestMin)
	
	sourceAmountRaw := int64(pathPayment.SendAmount)
	sourceAmountStr := amount.String(pathPayment.SendAmount)

	event := &schemas.TTPEventData{
		EventID:           eventID,
		LedgerSequence:    sequence,
		TransactionHash:   txHash,
		OperationIndex:    opIndex,
		Timestamp:         closeTime,
		EventType:         "path_payment_strict_send",
		AssetType:         assetType,
		AssetCode:         assetCode,
		AssetIssuer:       assetIssuer,
		FromAccount:       pathPayment.Source.Address(),
		ToAccount:         pathPayment.Destination.Address(),
		AmountRaw:         amountRaw,
		AmountStr:         amountStr,
		SourceAssetType:   &sourceAssetType,
		SourceAssetCode:   sourceAssetCode,
		SourceAssetIssuer: sourceAssetIssuer,
		SourceAmountRaw:   &sourceAmountRaw,
		SourceAmountStr:   &sourceAmountStr,
		Successful:        true,
		MemoType:          memoType,
		MemoValue:         memoValue,
		FeeCharged:        feeCharged,
		ProcessorVersion:  ComponentVersion,
	}

	return event
}

// extractCreateAccountEvent extracts a TTP event from a create account operation
func (s *TTPProcessorService) extractCreateAccountEvent(
	eventID string, sequence uint32, closeTime time.Time, txHash []byte, opIndex uint32,
	createAccount xdr.CreateAccountOp, memoType, memoValue *string, feeCharged int64,
) *schemas.TTPEventData {
	// Create account is treated as a native XLM payment
	amountRaw := int64(createAccount.StartingBalance)
	amountStr := amount.String(createAccount.StartingBalance)

	event := &schemas.TTPEventData{
		EventID:          eventID,
		LedgerSequence:   sequence,
		TransactionHash:  txHash,
		OperationIndex:   opIndex,
		Timestamp:        closeTime,
		EventType:        "create_account",
		AssetType:        "native",
		AssetCode:        nil, // Native XLM
		AssetIssuer:      nil, // Native XLM
		FromAccount:      createAccount.Source.Address(),
		ToAccount:        createAccount.Destination.Address(),
		AmountRaw:        amountRaw,
		AmountStr:        amountStr,
		Successful:       true,
		MemoType:         memoType,
		MemoValue:        memoValue,
		FeeCharged:       feeCharged,
		ProcessorVersion: ComponentVersion,
	}

	return event
}

// extractAccountMergeEvent extracts a TTP event from an account merge operation
func (s *TTPProcessorService) extractAccountMergeEvent(
	eventID string, sequence uint32, closeTime time.Time, txHash []byte, opIndex uint32,
	destination xdr.MuxedAccount, memoType, memoValue *string, feeCharged int64,
) *schemas.TTPEventData {
	// Account merge transfers all XLM from source to destination
	// We don't know the exact amount without checking account balance
	// For now, we'll use 0 and mark it specially

	event := &schemas.TTPEventData{
		EventID:          eventID,
		LedgerSequence:   sequence,
		TransactionHash:  txHash,
		OperationIndex:   opIndex,
		Timestamp:        closeTime,
		EventType:        "account_merge",
		AssetType:        "native",
		AssetCode:        nil, // Native XLM
		AssetIssuer:      nil, // Native XLM
		FromAccount:      "", // Will be filled by source account from operation source
		ToAccount:        destination.Address(),
		AmountRaw:        0, // Unknown without additional account lookup
		AmountStr:        "0.0000000",
		Successful:       true,
		MemoType:         memoType,
		MemoValue:        memoValue,
		FeeCharged:       feeCharged,
		ProcessorVersion: ComponentVersion,
	}

	return event
}

// extractAssetInfo extracts asset type, code, and issuer from an XDR asset
func (s *TTPProcessorService) extractAssetInfo(asset xdr.Asset) (string, *string, *string) {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "native", nil, nil
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		code := strings.TrimRight(string(asset.MustAlphaNum4().AssetCode[:]), "\x00")
		issuer := asset.MustAlphaNum4().Issuer.Address()
		return "credit_alphanum4", &code, &issuer
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		code := strings.TrimRight(string(asset.MustAlphaNum12().AssetCode[:]), "\x00")
		issuer := asset.MustAlphaNum12().Issuer.Address()
		return "credit_alphanum12", &code, &issuer
	default:
		return "unknown", nil, nil
	}
}

// shouldProcessOperationType checks if an operation type should be processed
func (s *TTPProcessorService) shouldProcessOperationType(opType string) bool {
	// Convert operation type to our event type format
	eventType := strings.ToLower(opType)
	eventType = strings.Replace(eventType, "operation_type_", "", 1)

	for _, configType := range s.config.EventTypes {
		if eventType == configType {
			return true
		}
	}
	return false
}

// shouldIncludeEvent checks if an event should be included based on filters
func (s *TTPProcessorService) shouldIncludeEvent(event schemas.TTPEventData) bool {
	// Check for duplicates if deduplication is enabled
	if s.config.DeduplicateEvents {
		s.eventsMu.RLock()
		if s.seenEvents[event.EventID] {
			s.eventsMu.RUnlock()
			return false
		}
		s.eventsMu.RUnlock()

		s.eventsMu.Lock()
		s.seenEvents[event.EventID] = true
		s.eventsMu.Unlock()
	}

	// Apply asset filters
	if len(s.config.AssetFilters) > 0 {
		matched := false
		for _, filter := range s.config.AssetFilters {
			if s.matchesAssetFilter(event, filter) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Apply amount filters
	if s.config.AmountFilters.MinAmount != "" || s.config.AmountFilters.MaxAmount != "" {
		if !s.matchesAmountFilter(event) {
			return false
		}
	}

	return true
}

// matchesAssetFilter checks if an event matches an asset filter
func (s *TTPProcessorService) matchesAssetFilter(event schemas.TTPEventData, filter AssetFilter) bool {
	// Handle native XLM
	if filter.AssetCode == "" && event.AssetCode == nil {
		return true
	}

	// Handle non-native assets
	if event.AssetCode != nil && *event.AssetCode == filter.AssetCode {
		if filter.AssetIssuer == "" || (event.AssetIssuer != nil && *event.AssetIssuer == filter.AssetIssuer) {
			return true
		}
	}

	return false
}

// matchesAmountFilter checks if an event matches amount filters
func (s *TTPProcessorService) matchesAmountFilter(event schemas.TTPEventData) bool {
	// Parse event amount
	eventAmount, err := strconv.ParseFloat(event.AmountStr, 64)
	if err != nil {
		return false
	}

	// Check minimum amount
	if s.config.AmountFilters.MinAmount != "" {
		minAmount, err := strconv.ParseFloat(s.config.AmountFilters.MinAmount, 64)
		if err != nil || eventAmount < minAmount {
			return false
		}
	}

	// Check maximum amount
	if s.config.AmountFilters.MaxAmount != "" {
		maxAmount, err := strconv.ParseFloat(s.config.AmountFilters.MaxAmount, 64)
		if err != nil || eventAmount > maxAmount {
			return false
		}
	}

	return true
}

// updateCurrentLedger safely updates the current ledger being processed
func (s *TTPProcessorService) updateCurrentLedger(ledger uint32) {
	s.mu.Lock()
	s.currentLedger = ledger
	s.mu.Unlock()
	
	currentLedger.Set(float64(ledger))
	
	s.statsMu.Lock()
	s.stats.LastProcessedLedger = ledger
	s.statsMu.Unlock()
}

// GetRecordsChannel returns the channel for consuming TTP event records
func (s *TTPProcessorService) GetRecordsChannel() <-chan arrow.Record {
	return s.recordsChannel
}

// GetErrorChannel returns the channel for consuming errors
func (s *TTPProcessorService) GetErrorChannel() <-chan error {
	return s.errorChannel
}

// GetCurrentLedger returns the current ledger being processed
func (s *TTPProcessorService) GetCurrentLedger() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentLedger
}

// IsRunning returns whether the service is currently running
func (s *TTPProcessorService) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isRunning
}

// GetStats returns current processing statistics
func (s *TTPProcessorService) GetStats() ProcessingStats {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	return s.stats
}

// logFinalStats logs final processing statistics
func (s *TTPProcessorService) logFinalStats() {
	stats := s.GetStats()
	duration := time.Since(stats.StartTime)

	log.Info().
		Uint64("ledgers_processed", stats.LedgersProcessed).
		Uint64("transactions_processed", stats.TransactionsProcessed).
		Uint64("operations_processed", stats.OperationsProcessed).
		Uint64("events_extracted", stats.EventsExtracted).
		Uint64("events_filtered", stats.EventsFiltered).
		Uint64("batches_generated", stats.BatchesGenerated).
		Uint32("last_ledger", stats.LastProcessedLedger).
		Dur("total_duration", duration).
		Msg("Final processing statistics")
}

// getAssetCode helper function for metrics
func getAssetCode(assetCode *string) string {
	if assetCode == nil {
		return "XLM"
	}
	return *assetCode
}