package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/withobsrvr/obsrvr-stellar-components/schemas"
)

// AnalyticsSinkService processes TTP events and writes them to multiple output formats
type AnalyticsSinkService struct {
	config   *Config
	pool     memory.Allocator
	registry *schemas.SchemaRegistry

	// Processor connection
	processorClient flight.FlightServiceClient
	processorConn   *grpc.ClientConn

	// Output writers
	parquetWriter   *ParquetWriter
	jsonWriter      *JSONWriter
	csvWriter       *CSVWriter
	
	// Real-time components
	wsHub           *WebSocketHub
	apiCache        *APICache

	// Processing state
	mu              sync.RWMutex
	isRunning       bool
	eventsChannel   chan *TTPEvent
	errorChannel    chan error

	// Statistics
	stats           AnalyticsStats
	statsMu         sync.RWMutex

	// Shutdown
	shutdownOnce    sync.Once
}

// AnalyticsStats tracks processing statistics
type AnalyticsStats struct {
	EventsReceived        uint64
	EventsWritten         uint64
	FilesWritten          uint64
	WebSocketConnections  int
	StartTime             time.Time
	LastEventTime         time.Time
}

// TTPEvent represents a processed TTP event
type TTPEvent struct {
	EventID         string    `json:"event_id"`
	LedgerSequence  uint32    `json:"ledger_sequence"`
	TransactionHash string    `json:"transaction_hash"`
	OperationIndex  uint32    `json:"operation_index"`
	Timestamp       time.Time `json:"timestamp"`
	EventType       string    `json:"event_type"`
	AssetType       string    `json:"asset_type"`
	AssetCode       *string   `json:"asset_code"`
	AssetIssuer     *string   `json:"asset_issuer"`
	FromAccount     string    `json:"from_account"`
	ToAccount       string    `json:"to_account"`
	AmountRaw       int64     `json:"amount_raw"`
	AmountStr       string    `json:"amount_str"`
	Successful      bool      `json:"successful"`
	MemoType        *string   `json:"memo_type"`
	MemoValue       *string   `json:"memo_value"`
	FeeCharged      int64     `json:"fee_charged"`
}

// NewAnalyticsSinkService creates a new analytics sink service
func NewAnalyticsSinkService(config *Config, pool memory.Allocator, registry *schemas.SchemaRegistry) (*AnalyticsSinkService, error) {
	service := &AnalyticsSinkService{
		config:        config,
		pool:          pool,
		registry:      registry,
		eventsChannel: make(chan *TTPEvent, config.BufferSize),
		errorChannel:  make(chan error, 10),
		stats:         AnalyticsStats{StartTime: time.Now()},
	}

	// Connect to processor
	if err := service.connectToProcessor(); err != nil {
		return nil, fmt.Errorf("failed to connect to processor: %w", err)
	}

	// Initialize output writers
	if err := service.initializeWriters(); err != nil {
		return nil, fmt.Errorf("failed to initialize writers: %w", err)
	}

	// Initialize real-time components
	if contains(config.OutputFormats, "websocket") {
		service.wsHub = NewWebSocketHub(config.MaxWebSocketConnections)
	}

	if contains(config.OutputFormats, "api") {
		service.apiCache = NewAPICache(config.APICacheSize)
	}

	return service, nil
}

// connectToProcessor establishes connection to the TTP processor component
func (s *AnalyticsSinkService) connectToProcessor() error {
	log.Info().
		Str("endpoint", s.config.ProcessorEndpoint).
		Msg("Connecting to ttp-arrow-processor")

	// Create gRPC connection
	conn, err := grpc.Dial(s.config.ProcessorEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to processor: %w", err)
	}

	s.processorConn = conn
	s.processorClient = flight.NewFlightServiceClient(conn)

	log.Info().Msg("Connected to ttp-arrow-processor")
	return nil
}

// initializeWriters creates and initializes all output writers
func (s *AnalyticsSinkService) initializeWriters() error {
	for _, format := range s.config.OutputFormats {
		switch format {
		case "parquet":
			writer, err := NewParquetWriter(s.config, s.pool)
			if err != nil {
				return fmt.Errorf("failed to create parquet writer: %w", err)
			}
			s.parquetWriter = writer

		case "json":
			writer, err := NewJSONWriter(s.config)
			if err != nil {
				return fmt.Errorf("failed to create JSON writer: %w", err)
			}
			s.jsonWriter = writer

		case "csv":
			writer, err := NewCSVWriter(s.config)
			if err != nil {
				return fmt.Errorf("failed to create CSV writer: %w", err)
			}
			s.csvWriter = writer
		}
	}

	return nil
}

// Start begins processing TTP events from the processor
func (s *AnalyticsSinkService) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return fmt.Errorf("service is already running")
	}
	s.isRunning = true
	s.mu.Unlock()

	log.Info().
		Str("processor_endpoint", s.config.ProcessorEndpoint).
		Strs("output_formats", s.config.OutputFormats).
		Int("writer_threads", s.config.WriterThreads).
		Msg("Starting analytics sink processing")

	// Start writer workers
	for i := 0; i < s.config.WriterThreads; i++ {
		go s.writerWorker(ctx, i)
	}

	// Start consuming from processor
	return s.consumeFromProcessor(ctx)
}

// Stop gracefully stops the service
func (s *AnalyticsSinkService) Stop(ctx context.Context) {
	s.shutdownOnce.Do(func() {
		s.mu.Lock()
		s.isRunning = false
		s.mu.Unlock()

		log.Info().Msg("Stopping analytics sink service")

		// Close channels
		close(s.eventsChannel)
		close(s.errorChannel)

		// Close processor connection
		if s.processorConn != nil {
			s.processorConn.Close()
		}

		// Close writers
		if s.parquetWriter != nil {
			s.parquetWriter.Close()
		}
		if s.jsonWriter != nil {
			s.jsonWriter.Close()
		}
		if s.csvWriter != nil {
			s.csvWriter.Close()
		}

		// Close WebSocket hub
		if s.wsHub != nil {
			s.wsHub.Close()
		}

		// Log final stats
		s.logFinalStats()

		log.Info().Msg("Analytics sink service stopped")
	})
}

// consumeFromProcessor consumes TTP events from the processor
func (s *AnalyticsSinkService) consumeFromProcessor(ctx context.Context) error {
	// Get flight info for ttp_events stream
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"ttp_events"},
	}

	flightInfo, err := s.processorClient.GetFlightInfo(ctx, descriptor)
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
		Msg("Starting to consume TTP events from processor")

	// Start streaming
	stream, err := s.processorClient.DoGet(ctx, ticket)
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
				log.Info().Msg("Processor stream ended")
				return nil
			}
			return fmt.Errorf("failed to receive record: %w", err)
		}

		recordCount++

		// Use the record directly (FlightData to Arrow Record conversion needed)
		// TODO: Implement proper FlightData deserialization
		log.Debug().Int("count", recordCount).Msg("Received FlightData - processing placeholder")

		if recordCount%50 == 0 {
			log.Debug().
				Int("records_received", recordCount).
				Msg("Processor stream progress")
		}
	}
}

// convertRecordToEvents converts an Arrow record to TTP events
func (s *AnalyticsSinkService) convertRecordToEvents(record arrow.Record) []*TTPEvent {
	var events []*TTPEvent

	// Extract columns from the record
	eventIDCol := record.Column(0).(*array.String)
	ledgerSeqCol := record.Column(1).(*array.Uint32)
	txHashCol := record.Column(2).(*array.FixedSizeBinary)
	opIndexCol := record.Column(3).(*array.Uint32)
	timestampCol := record.Column(4).(*array.Timestamp)
	eventTypeCol := record.Column(6).(*array.String)
	assetTypeCol := record.Column(7).(*array.String)
	assetCodeCol := record.Column(8).(*array.String)
	assetIssuerCol := record.Column(9).(*array.String)
	fromAccountCol := record.Column(10).(*array.String)
	toAccountCol := record.Column(11).(*array.String)
	amountRawCol := record.Column(12).(*array.Int64)
	amountStrCol := record.Column(13).(*array.String)
	successfulCol := record.Column(19).(*array.Boolean)
	memoTypeCol := record.Column(20).(*array.String)
	memoValueCol := record.Column(21).(*array.String)
	feeChargedCol := record.Column(22).(*array.Int64)

	// Process each row
	for i := 0; i < int(record.NumRows()); i++ {
		event := &TTPEvent{
			EventID:         eventIDCol.Value(i),
			LedgerSequence:  ledgerSeqCol.Value(i),
			TransactionHash: fmt.Sprintf("%x", txHashCol.Value(i)),
			OperationIndex:  opIndexCol.Value(i),
			Timestamp:       time.Unix(0, int64(timestampCol.Value(i))*1000),
			EventType:       eventTypeCol.Value(i),
			AssetType:       assetTypeCol.Value(i),
			FromAccount:     fromAccountCol.Value(i),
			ToAccount:       toAccountCol.Value(i),
			AmountRaw:       amountRawCol.Value(i),
			AmountStr:       amountStrCol.Value(i),
			Successful:      successfulCol.Value(i),
			FeeCharged:      feeChargedCol.Value(i),
		}

		// Handle nullable fields
		if !assetCodeCol.IsNull(i) {
			assetCode := assetCodeCol.Value(i)
			event.AssetCode = &assetCode
		}
		if !assetIssuerCol.IsNull(i) {
			assetIssuer := assetIssuerCol.Value(i)
			event.AssetIssuer = &assetIssuer
		}
		if !memoTypeCol.IsNull(i) {
			memoType := memoTypeCol.Value(i)
			event.MemoType = &memoType
		}
		if !memoValueCol.IsNull(i) {
			memoValue := memoValueCol.Value(i)
			event.MemoValue = &memoValue
		}

		events = append(events, event)
	}

	return events
}

// writerWorker processes events and writes them to outputs
func (s *AnalyticsSinkService) writerWorker(ctx context.Context, workerID int) {
	log.Debug().Int("worker_id", workerID).Msg("Starting writer worker")

	for {
		select {
		case <-ctx.Done():
			log.Debug().Int("worker_id", workerID).Msg("Writer worker stopped")
			return

		case event, ok := <-s.eventsChannel:
			if !ok {
				log.Debug().Int("worker_id", workerID).Msg("Events channel closed")
				return
			}

			// Write to all configured outputs
			s.writeToOutputs(event)

			// Send to real-time components
			if s.wsHub != nil {
				s.wsHub.Broadcast(event)
			}
			if s.apiCache != nil {
				s.apiCache.Add(event)
			}

			// Update statistics
			s.statsMu.Lock()
			s.stats.EventsWritten++
			s.statsMu.Unlock()
		}
	}
}

// writeToOutputs writes an event to all configured output formats
func (s *AnalyticsSinkService) writeToOutputs(event *TTPEvent) {
	for _, format := range s.config.OutputFormats {
		timer := prometheus.NewTimer(writeDuration.WithLabelValues(format))

		var err error
		switch format {
		case "parquet":
			if s.parquetWriter != nil {
				err = s.parquetWriter.WriteEvent(event)
			}
		case "json":
			if s.jsonWriter != nil {
				err = s.jsonWriter.WriteEvent(event)
			}
		case "csv":
			if s.csvWriter != nil {
				err = s.csvWriter.WriteEvent(event)
			}
		}

		timer.ObserveDuration()

		if err != nil {
			log.Error().
				Err(err).
				Str("format", format).
				Str("event_id", event.EventID).
				Msg("Failed to write event")
			eventsWritten.WithLabelValues(format, "error").Inc()
		} else {
			eventsWritten.WithLabelValues(format, "success").Inc()
		}
	}
}

// StartWebSocketServer starts the WebSocket and API server
func (s *AnalyticsSinkService) StartWebSocketServer(ctx context.Context) error {
	if s.wsHub == nil && s.apiCache == nil {
		return nil // Neither WebSocket nor API enabled
	}

	router := mux.NewRouter()

	// WebSocket endpoint
	if s.wsHub != nil {
		upgrader := websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins in development
			},
		}

		router.HandleFunc(s.config.WebSocketPath, func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				log.Error().Err(err).Msg("WebSocket upgrade failed")
				return
			}
			s.wsHub.AddConnection(conn)
		}).Methods("GET")

		// Start the hub
		go s.wsHub.Run(ctx)
	}

	// API endpoints
	if s.apiCache != nil {
		router.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
			events := s.apiCache.GetRecent(100) // Get last 100 events
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(events)
		}).Methods("GET")

		router.HandleFunc("/api/events/{event_id}", func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			eventID := vars["event_id"]
			
			event := s.apiCache.GetByID(eventID)
			if event == nil {
				http.NotFound(w, r)
				return
			}
			
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(event)
		}).Methods("GET")
	}

	port := s.config.WebSocketPort
	if contains(s.config.OutputFormats, "api") {
		port = s.config.APIPort
	}

	log.Info().
		Int("port", port).
		Bool("websocket_enabled", s.wsHub != nil).
		Bool("api_enabled", s.apiCache != nil).
		Msg("Starting WebSocket/API server")

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: router,
	}

	return server.ListenAndServe()
}

// GetStats returns current processing statistics
func (s *AnalyticsSinkService) GetStats() AnalyticsStats {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	
	stats := s.stats
	if s.wsHub != nil {
		stats.WebSocketConnections = s.wsHub.GetConnectionCount()
	}
	
	return stats
}

// IsRunning returns whether the service is currently running
func (s *AnalyticsSinkService) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isRunning
}

// logFinalStats logs final processing statistics
func (s *AnalyticsSinkService) logFinalStats() {
	stats := s.GetStats()
	duration := time.Since(stats.StartTime)

	log.Info().
		Uint64("events_received", stats.EventsReceived).
		Uint64("events_written", stats.EventsWritten).
		Uint64("files_written", stats.FilesWritten).
		Int("websocket_connections", stats.WebSocketConnections).
		Dur("total_duration", duration).
		Msg("Final analytics processing statistics")
}

// Helper function for metrics
func getAssetCodeForMetrics(assetCode *string) string {
	if assetCode == nil {
		return "XLM"
	}
	return *assetCode
}

// createDirectories ensures output directories exist
func createDirectories(paths ...string) error {
	for _, path := range paths {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", filepath.Dir(path), err)
		}
	}
	return nil
}