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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"github.com/stellar/go/amount"
	"github.com/stellar/go/xdr"
	"google.golang.org/grpc"

	"github.com/withobsrvr/obsrvr-stellar-components/internal/flowctl"
	flowctlpb "github.com/withobsrvr/obsrvr-stellar-components/proto/gen"
	"github.com/withobsrvr/obsrvr-stellar-components/schemas"
)

// TTPProcessorService processes Stellar ledger data to extract TTP events
type TTPProcessorService struct {
	config   *Config
	pool     memory.Allocator
	registry *schemas.SchemaRegistry

	// flowctl integration
	flowctlController *flowctl.Controller
	connectionPool    *flowctl.ConnectionPool

	// Source connection
	sourceClient   flight.FlightServiceClient
	sourceConn     *grpc.ClientConn
	sourceEndpoint string
	reconnectChan  chan struct{}

	// Flow control
	currentInflight   int32
	maxInflight       int32
	throttleRate      float64
	flowControlMu     sync.RWMutex

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
	LastProcessed      time.Time
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
		reconnectChan:  make(chan struct{}, 1),
		sourceEndpoint: config.SourceEndpoint, // Fallback to configured endpoint
		maxInflight:    int32(config.BufferSize),
		throttleRate:   1.0, // Start at full rate
		connectionPool: flowctl.NewConnectionPool(flowctl.DefaultConnectionPoolConfig()),
	}

	// Note: Source connection will be established later after flowctl controller is set
	// This allows for dynamic discovery of upstream services

	return service, nil
}

// SetFlowCtlController sets the flowctl controller for service discovery
func (s *TTPProcessorService) SetFlowCtlController(controller *flowctl.Controller) {
	s.flowctlController = controller
}

// connectToSource establishes connection to the stellar-arrow-source component
func (s *TTPProcessorService) connectToSource() error {
	endpoint := s.discoverSourceEndpoint()
	
	log.Info().
		Str("endpoint", endpoint).
		Bool("discovered", endpoint != s.config.SourceEndpoint).
		Msg("Connecting to stellar-arrow-source")

	// Use connection pool to get a client
	client, err := s.connectionPool.GetConnection(endpoint)
	if err != nil {
		return fmt.Errorf("failed to get pooled connection to source at %s: %w", endpoint, err)
	}

	// Update service connection info
	s.sourceClient = client
	s.sourceEndpoint = endpoint

	log.Info().
		Str("endpoint", endpoint).
		Msg("Connected to stellar-arrow-source via connection pool")
	return nil
}

// discoverSourceEndpoint discovers source service endpoint through flowctl
func (s *TTPProcessorService) discoverSourceEndpoint() string {
	// If no flowctl controller, use configured endpoint
	if s.flowctlController == nil {
		log.Debug().Msg("No flowctl controller, using configured source endpoint")
		return s.config.SourceEndpoint
	}

	// Discover upstream services that produce stellar ledger events
	upstreams, err := s.flowctlController.DiscoverUpstreamServices(flowctl.StellarLedgerEventType)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to discover upstream services, using configured endpoint")
		return s.config.SourceEndpoint
	}

	if len(upstreams) == 0 {
		log.Warn().Msg("No upstream source services found, using configured endpoint")
		return s.config.SourceEndpoint
	}

	// Use the first discovered source service
	sourceService := upstreams[0]
	connectionInfo := s.flowctlController.ExtractConnectionInfo(sourceService)
	
	log.Info().
		Str("service_id", sourceService.ServiceId).
		Str("discovered_endpoint", connectionInfo.Endpoint).
		Msg("Discovered stellar-arrow-source via flowctl")

	return connectionInfo.Endpoint
}

// startServiceDiscoveryWatcher watches for service changes and reconnects as needed
func (s *TTPProcessorService) startServiceDiscoveryWatcher(ctx context.Context) {
	if s.flowctlController == nil {
		log.Debug().Msg("No flowctl controller, skipping service discovery watcher")
		return
	}

	go func() {
		log.Info().Msg("Starting service discovery watcher")
		
		err := s.flowctlController.WatchServices(ctx, func(services []*flowctlpb.ServiceStatus) {
			// Check if upstream source has changed
			newEndpoint := s.discoverSourceEndpoint()
			if newEndpoint != s.sourceEndpoint {
				log.Info().
					Str("old_endpoint", s.sourceEndpoint).
					Str("new_endpoint", newEndpoint).
					Msg("Source service endpoint changed, triggering reconnect")
				
				select {
				case s.reconnectChan <- struct{}{}:
				default:
					// Channel full, reconnection already pending
				}
			}
		})
		
		if err != nil && err != context.Canceled {
			log.Error().Err(err).Msg("Service discovery watcher failed")
		}
	}()
}

// handleReconnections processes reconnection requests
func (s *TTPProcessorService) handleReconnections(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.reconnectChan:
				log.Info().Msg("Processing reconnection request")
				
				// Attempt to reconnect to source
				if err := s.connectToSource(); err != nil {
					log.Error().Err(err).Msg("Failed to reconnect to source")
					
					// Retry after delay
					time.Sleep(10 * time.Second)
					select {
					case s.reconnectChan <- struct{}{}:
					default:
					}
				} else {
					log.Info().Msg("Successfully reconnected to source")
				}
			}
		}
	}()
}

// ========================================================================
// Flow Control and Backpressure Management
// ========================================================================

// startBackpressureMonitoring starts monitoring backpressure signals from flowctl
func (s *TTPProcessorService) startBackpressureMonitoring(ctx context.Context) {
	if s.flowctlController == nil {
		return
	}

	go func() {
		log.Info().Msg("Starting backpressure monitoring")
		
		backpressureSignals := s.flowctlController.GetBackpressureSignals()
		
		for {
			select {
			case <-ctx.Done():
				return
			case signal := <-backpressureSignals:
				s.handleBackpressureSignal(signal)
			}
		}
	}()
}

// handleBackpressureSignal processes backpressure signals and adjusts processing rate
func (s *TTPProcessorService) handleBackpressureSignal(signal flowctl.BackpressureSignal) {
	s.flowControlMu.Lock()
	defer s.flowControlMu.Unlock()
	
	oldRate := s.throttleRate
	s.throttleRate = signal.RecommendedRate
	
	log.Info().
		Str("service_id", signal.ServiceID).
		Float64("old_rate", oldRate).
		Float64("new_rate", s.throttleRate).
		Float64("current_load", signal.CurrentLoad).
		Str("reason", signal.Reason).
		Msg("Adjusted processing rate due to backpressure")
}

// updateInflightCount updates the current inflight count and reports to flowctl
func (s *TTPProcessorService) updateInflightCount(delta int32) {
	s.flowControlMu.Lock()
	s.currentInflight += delta
	if s.currentInflight < 0 {
		s.currentInflight = 0
	}
	currentCount := s.currentInflight
	s.flowControlMu.Unlock()
	
	// Report to flowctl for global flow control
	if s.flowctlController != nil {
		serviceID := s.flowctlController.GetServiceID()
		if serviceID != "" {
			s.flowctlController.UpdateServiceInflight(serviceID, currentCount)
		}
	}
}

// shouldThrottle returns whether processing should be throttled based on current rate
func (s *TTPProcessorService) shouldThrottle() bool {
	s.flowControlMu.RLock()
	rate := s.throttleRate
	s.flowControlMu.RUnlock()
	
	if rate >= 1.0 {
		return false // No throttling needed
	}
	
	// Simple probabilistic throttling
	// More sophisticated approaches could use token bucket or sliding window
	return (time.Now().UnixNano() % 100) >= int64(rate*100)
}

// getCurrentThrottleRate returns the current throttle rate for metrics
func (s *TTPProcessorService) getCurrentThrottleRate() float64 {
	s.flowControlMu.RLock()
	defer s.flowControlMu.RUnlock()
	return s.throttleRate
}

// getInflightMetrics returns current inflight metrics
func (s *TTPProcessorService) getInflightMetrics() (int32, int32) {
	s.flowControlMu.RLock()
	defer s.flowControlMu.RUnlock()
	return s.currentInflight, s.maxInflight
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
		Bool("flowctl_enabled", s.flowctlController != nil).
		Msg("Starting TTP event processing")

	// Start connection pool
	if err := s.connectionPool.Start(ctx); err != nil {
		log.Warn().Err(err).Msg("Failed to start connection pool, continuing without it")
	}

	// Start flowctl-based dynamic connection management if available
	if s.flowctlController != nil {
		// Register max inflight capacity with flowctl
		serviceID := s.flowctlController.GetServiceID()
		if serviceID != "" {
			s.flowctlController.SetServiceMaxInflight(serviceID, s.maxInflight)
		}
		
		// Start service discovery watcher
		s.startServiceDiscoveryWatcher(ctx)
		
		// Start reconnection handler
		s.handleReconnections(ctx)
		
		// Start backpressure monitoring
		s.startBackpressureMonitoring(ctx)
		
		log.Info().
			Int32("max_inflight", s.maxInflight).
			Msg("Started dynamic connection management and flow control via flowctl")
	}

	// Establish initial connection to source (uses discovery if flowctl available)
	if err := s.connectToSource(); err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}

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

		// Stop connection pool
		if s.connectionPool != nil {
			s.connectionPool.Stop()
		}

		// Close source connection (if using direct connection)
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
		Type: flight.DescriptorPATH,
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
		_, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				log.Info().Msg("Source stream ended")
				return nil
			}
			return fmt.Errorf("failed to receive record: %w", err)
		}

		recordCount++
		
		// For now, skip FlightData to arrow.Record conversion
		// TODO: Implement proper FlightData deserialization
		log.Debug().Msg("Received FlightData - conversion to arrow.Record needed")
		
		// Skip processing for now to allow compilation
		continue

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

			// Check if we should throttle processing
			if s.shouldThrottle() {
				// Put the record back and wait a bit
				select {
				case s.recordsChannel <- record:
					time.Sleep(time.Duration(100+workerID*10) * time.Millisecond) // Staggered backoff
					continue
				default:
					// Channel full, process anyway but log throttling
					log.Debug().Int("worker_id", workerID).Msg("Throttling active but channel full")
				}
			}

			// Track inflight processing
			s.updateInflightCount(1)
			defer s.updateInflightCount(-1)

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
				
				// TODO: Send processed events to Flight server clients
				log.Debug().Int64("records", eventRecord.NumRows()).Msg("Generated TTP events batch")
				
				batchesGenerated.WithLabelValues("events").Inc()
				s.statsMu.Lock()
				s.stats.BatchesGenerated++
				s.statsMu.Unlock()
				
				eventRecord.Release()

				// Create new builder for next batch
				eventBuilder.Release()
				eventBuilder = schemas.NewTTPEventBuilder(s.pool)
			}
		}
	}
}

// processLedgerRecord processes a single ledger record to extract TTP events
func (s *TTPProcessorService) processLedgerRecord(record arrow.Record, eventBuilder *schemas.TTPEventBuilder) int {
	eventsCount := 0

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
				eventsCount++
				// Note: Global metrics would be updated here if available
			} else {
				s.statsMu.Lock()
				s.stats.EventsFiltered++
				s.statsMu.Unlock()
			}
		}
	}

	return eventsCount
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

		// Extract transaction hash (simplified - using a placeholder for now)
		var txHash [32]byte // TODO: Compute actual transaction hash
		
		// Get transaction operations and actual transaction based on envelope type
		var operations []xdr.Operation
		var actualTx *xdr.Transaction
		switch tx.Type {
		case xdr.EnvelopeTypeEnvelopeTypeTx:
			v1 := tx.MustV1()
			operations = v1.Tx.Operations
			actualTx = &v1.Tx
		case xdr.EnvelopeTypeEnvelopeTypeTxV0:
			v0 := tx.MustV0()
			operations = v0.Tx.Operations
			// For v0, we need to convert to v1 transaction format
			// For now, use a placeholder
			actualTx = nil // TODO: Convert v0 to v1 transaction
		case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
			// Skip fee bump transactions for now
			continue
		default:
			continue
		}

		// Skip if we couldn't get the actual transaction
		if actualTx == nil {
			continue
		}

		// Process each operation in the transaction
		for opIndex, op := range operations {
			s.statsMu.Lock()
			s.stats.OperationsProcessed++
			s.statsMu.Unlock()

			// Extract events from this operation
			opEvents := s.extractEventsFromOperation(
				sequence, closeTime, txHash[:], uint32(opIndex),
				&op, actualTx, xdr.Uint32(txResult.Result.Result.FeeCharged),
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
	if tx.Memo.Type != xdr.MemoTypeMemoNone {
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
			op.Body.MustPaymentOp(), tx, memoType, memoValue, int64(feeCharged)); event != nil {
			events = append(events, *event)
		}

	case xdr.OperationTypePathPaymentStrictReceive:
		if event := s.extractPathPaymentReceiveEvent(eventID, sequence, closeTime, txHash, opIndex,
			op.Body.MustPathPaymentStrictReceiveOp(), tx, memoType, memoValue, int64(feeCharged)); event != nil {
			events = append(events, *event)
		}

	case xdr.OperationTypePathPaymentStrictSend:
		if event := s.extractPathPaymentSendEvent(eventID, sequence, closeTime, txHash, opIndex,
			op.Body.MustPathPaymentStrictSendOp(), tx, memoType, memoValue, int64(feeCharged)); event != nil {
			events = append(events, *event)
		}

	case xdr.OperationTypeCreateAccount:
		if event := s.extractCreateAccountEvent(eventID, sequence, closeTime, txHash, opIndex,
			op.Body.MustCreateAccountOp(), tx, memoType, memoValue, int64(feeCharged)); event != nil {
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
	payment xdr.PaymentOp, tx *xdr.Transaction, memoType, memoValue *string, feeCharged int64,
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
		FromAccount:      tx.SourceAccount.Address(),
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
	pathPayment xdr.PathPaymentStrictReceiveOp, tx *xdr.Transaction, memoType, memoValue *string, feeCharged int64,
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
		FromAccount:       tx.SourceAccount.Address(),
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
	pathPayment xdr.PathPaymentStrictSendOp, tx *xdr.Transaction, memoType, memoValue *string, feeCharged int64,
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
		FromAccount:       tx.SourceAccount.Address(),
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
	createAccount xdr.CreateAccountOp, tx *xdr.Transaction, memoType, memoValue *string, feeCharged int64,
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
		FromAccount:      tx.SourceAccount.Address(),
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
		alphanum4 := asset.MustAlphaNum4()
		assetCode := alphanum4.AssetCode
		code := strings.TrimRight(string(assetCode[:]), "\x00")
		issuer := alphanum4.Issuer.Address()
		return "credit_alphanum4", &code, &issuer
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		alphanum12 := asset.MustAlphaNum12()
		assetCode := alphanum12.AssetCode
		code := strings.TrimRight(string(assetCode[:]), "\x00")
		issuer := alphanum12.Issuer.Address()
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

// GetMetrics implements flowctl.MetricsProvider interface
func (s *TTPProcessorService) GetMetrics() map[string]float64 {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()

	metrics := make(map[string]float64)
	
	// Basic processing statistics
	metrics["ledgers_processed_total"] = float64(s.stats.LedgersProcessed)
	metrics["transactions_processed_total"] = float64(s.stats.TransactionsProcessed)
	metrics["operations_processed_total"] = float64(s.stats.OperationsProcessed)
	metrics["events_extracted_total"] = float64(s.stats.EventsExtracted)
	metrics["events_filtered_total"] = float64(s.stats.EventsFiltered)
	metrics["batches_generated_total"] = float64(s.stats.BatchesGenerated)
	
	// Current processing state
	s.mu.RLock()
	metrics["current_ledger"] = float64(s.currentLedger)
	isRunning := s.isRunning
	s.mu.RUnlock()
	
	// Service status
	if isRunning {
		metrics["service_status"] = 1.0
	} else {
		metrics["service_status"] = 0.0
	}
	
	// Last processed ledger
	metrics["last_processed_ledger"] = float64(s.stats.LastProcessedLedger)
	
	// Processing rates
	if !s.stats.StartTime.IsZero() {
		uptime := time.Since(s.stats.StartTime).Seconds()
		metrics["uptime_seconds"] = uptime
		
		if uptime > 0 {
			metrics["ledgers_per_second"] = float64(s.stats.LedgersProcessed) / uptime
			metrics["events_per_second"] = float64(s.stats.EventsExtracted) / uptime
			metrics["operations_per_second"] = float64(s.stats.OperationsProcessed) / uptime
		}
	}
	
	// Time since last processed
	if !s.stats.LastProcessed.IsZero() {
		metrics["seconds_since_last_processed"] = time.Since(s.stats.LastProcessed).Seconds()
	}
	
	// Queue depths (if channels exist)
	if s.recordsChannel != nil {
		metrics["records_queue_depth"] = float64(len(s.recordsChannel))
	}
	if s.errorChannel != nil {
		metrics["error_queue_depth"] = float64(len(s.errorChannel))
	}
	
	// Event processing efficiency
	if s.stats.OperationsProcessed > 0 {
		metrics["event_extraction_rate"] = float64(s.stats.EventsExtracted) / float64(s.stats.OperationsProcessed)
		metrics["event_filter_rate"] = float64(s.stats.EventsFiltered) / float64(s.stats.OperationsProcessed)
	}
	
	// Flow control metrics
	currentInflight, maxInflight := s.getInflightMetrics()
	metrics["current_inflight"] = float64(currentInflight)
	metrics["max_inflight"] = float64(maxInflight)
	metrics["throttle_rate"] = s.getCurrentThrottleRate()
	
	// Inflight utilization
	if maxInflight > 0 {
		metrics["inflight_utilization"] = float64(currentInflight) / float64(maxInflight)
	}
	
	// Backpressure status
	if s.flowctlController != nil && s.flowctlController.IsBackpressureActive() {
		metrics["backpressure_active"] = 1.0
	} else {
		metrics["backpressure_active"] = 0.0
	}
	
	// Connection pool metrics
	if s.connectionPool != nil {
		poolStats := s.connectionPool.GetPoolStats()
		for endpoint, stats := range poolStats {
			// Use endpoint as a label in the metric name for simplicity
			prefix := fmt.Sprintf("pool_%s_", s.sanitizeEndpointForMetrics(endpoint))
			metrics[prefix+"total_connections"] = float64(stats.TotalConnections)
			metrics[prefix+"healthy_connections"] = float64(stats.HealthyConnections)
			metrics[prefix+"idle_connections"] = float64(stats.IdleConnections)
			metrics[prefix+"average_use_count"] = stats.AverageUseCount
			metrics[prefix+"oldest_connection_seconds"] = stats.OldestConnection.Seconds()
		}
	}
	
	return metrics
}

// sanitizeEndpointForMetrics converts endpoint to metric-safe string
func (s *TTPProcessorService) sanitizeEndpointForMetrics(endpoint string) string {
	// Replace common characters that aren't metric-safe
	safe := strings.ReplaceAll(endpoint, ":", "_")
	safe = strings.ReplaceAll(safe, ".", "_")
	safe = strings.ReplaceAll(safe, "-", "_")
	return safe
}