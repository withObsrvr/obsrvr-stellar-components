package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"

	"github.com/withobsrvr/obsrvr-stellar-components/schemas"
)

// StellarSourceService manages the data ingestion from various Stellar sources
type StellarSourceService struct {
	config   *Config
	pool     memory.Allocator
	registry *schemas.SchemaRegistry

	// Data sources
	backendClient  *StellarBackendClient
	datalakeReader DataLakeReader
	xdrProcessor   *XDRProcessor

	// Processing state
	mu             sync.RWMutex
	isRunning      bool
	currentLedger  uint32
	recordsChannel chan arrow.Record
	errorChannel   chan error

	// Processing statistics
	stats ProcessingStats
	statsMu sync.RWMutex

	// Shutdown
	shutdownOnce sync.Once
}

// ProcessingStats tracks XDR processing statistics
type ProcessingStats struct {
	LedgersProcessed   uint64
	LedgersRecovered   uint64
	ValidationErrors   uint64
	ProcessingErrors   uint64
	StartTime          time.Time
	LastProcessed      time.Time
}

// NewStellarSourceService creates a new source service
func NewStellarSourceService(config *Config, pool memory.Allocator, registry *schemas.SchemaRegistry) (*StellarSourceService, error) {
	service := &StellarSourceService{
		config:         config,
		pool:           pool,
		registry:       registry,
		recordsChannel: make(chan arrow.Record, config.BufferSize),
		errorChannel:   make(chan error, 10),
		stats:          ProcessingStats{StartTime: time.Now()},
		xdrProcessor:   NewXDRProcessor(config.NetworkPassphrase, true), // Enable strict validation
	}

	// Initialize the appropriate data source
	switch config.SourceType {
	case "rpc", "archive", "captive", "storage":
		backendClient, err := NewStellarBackendClient(config)
		if err != nil {
			return nil, fmt.Errorf("failed to create backend client: %w", err)
		}
		service.backendClient = backendClient

	case "datalake":
		// Try ttp-processor-demo compatible datastore first
		if config.LedgersPerFile > 1 && (config.StorageBackend == "gcs" || config.StorageBackend == "s3") {
			log.Info().Msg("Using ttp-processor-demo compatible datastore reader")
			datastoreReader, err := NewStellarDatastoreReader(config)
			if err != nil {
				log.Warn().Err(err).Msg("Failed to create datastore reader, falling back to basic datalake reader")
				// Fall back to basic datalake reader
				datalakeReader, err := NewDataLakeReader(config)
				if err != nil {
					return nil, fmt.Errorf("failed to create data lake reader: %w", err)
				}
				service.datalakeReader = datalakeReader
			} else {
				// Wrap datastore reader to implement DataLakeReader interface
				service.datalakeReader = &DatastoreReaderWrapper{datastoreReader}
			}
		} else {
			// Use basic datalake reader for filesystem or single-ledger-per-file setups
			log.Info().Msg("Using basic datalake reader")
			datalakeReader, err := NewDataLakeReader(config)
			if err != nil {
				return nil, fmt.Errorf("failed to create data lake reader: %w", err)
			}
			service.datalakeReader = datalakeReader
		}

	default:
		return nil, fmt.Errorf("unsupported source type: %s", config.SourceType)
	}

	return service, nil
}

// Start begins processing ledgers from the configured source
func (s *StellarSourceService) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return fmt.Errorf("service is already running")
	}
	s.isRunning = true
	s.mu.Unlock()

	log.Info().
		Str("source_type", s.config.SourceType).
		Uint32("start_ledger", s.config.StartLedger).
		Uint32("end_ledger", s.config.EndLedger).
		Int("batch_size", s.config.BatchSize).
		Msg("Starting ledger processing")

	// Start processing based on source type
	switch s.config.SourceType {
	case "rpc", "archive", "captive", "storage":
		return s.processBackendStream(ctx)
	case "datalake":
		return s.processDataLake(ctx)
	default:
		return fmt.Errorf("unsupported source type: %s", s.config.SourceType)
	}
}

// Stop gracefully stops the service
func (s *StellarSourceService) Stop(ctx context.Context) {
	s.shutdownOnce.Do(func() {
		s.mu.Lock()
		s.isRunning = false
		s.mu.Unlock()

		log.Info().Msg("Stopping stellar source service")

		// Close channels
		close(s.recordsChannel)
		close(s.errorChannel)

		// Clean up data sources
		if s.backendClient != nil {
			s.backendClient.Close()
		}
		if s.datalakeReader != nil {
			s.datalakeReader.Close()
		}

		log.Info().Msg("Stellar source service stopped")
	})
}

// GetRecordsChannel returns the channel for consuming Arrow records
func (s *StellarSourceService) GetRecordsChannel() <-chan arrow.Record {
	return s.recordsChannel
}

// GetErrorChannel returns the channel for consuming errors
func (s *StellarSourceService) GetErrorChannel() <-chan error {
	return s.errorChannel
}

// GetCurrentLedger returns the current ledger being processed
func (s *StellarSourceService) GetCurrentLedger() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentLedger
}

// IsRunning returns whether the service is currently running
func (s *StellarSourceService) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isRunning
}

// processBackendStream processes ledgers using the new backend client
func (s *StellarSourceService) processBackendStream(ctx context.Context) error {
	if s.backendClient == nil {
		return fmt.Errorf("backend client not initialized")
	}

	log.Info().
		Str("backend_type", s.config.BackendType).
		Msg("Starting backend stream processing")

	// Validate configuration
	if err := s.backendClient.ValidateConfiguration(); err != nil {
		return fmt.Errorf("invalid backend configuration: %w", err)
	}

	// Perform health check
	if err := s.backendClient.HealthCheck(ctx); err != nil {
		log.Warn().Err(err).Msg("Backend health check failed, continuing anyway")
	}

	var startSeq uint32 = s.config.StartLedger
	var endSeq uint32 = s.config.EndLedger

	// Get latest ledger if starting from 0
	if startSeq == 0 {
		latest, err := s.backendClient.GetLatestLedgerSequence(ctx)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to get latest ledger, starting from 1")
			startSeq = 1
		} else {
			startSeq = latest
			log.Info().Uint32("ledger", startSeq).Msg("Starting from latest ledger")
		}
	}

	// For streaming mode, process indefinitely
	if endSeq == 0 {
		return s.processIndefiniteStream(ctx, startSeq)
	}

	// For bounded mode, process the specified range
	return s.processBoundedRange(ctx, startSeq, endSeq)
}

// processIndefiniteStream processes ledgers indefinitely (real-time mode)
func (s *StellarSourceService) processIndefiniteStream(ctx context.Context, startSeq uint32) error {
	log.Info().Uint32("start_seq", startSeq).Msg("Starting indefinite stream processing")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	currentSeq := startSeq

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Process individual ledgers in real-time mode
			record, err := s.backendClient.GetLedgerRecord(ctx, currentSeq)
			if err != nil {
				log.Error().
					Err(err).
					Uint32("sequence", currentSeq).
					Msg("Failed to get ledger record")
				ledgersProcessed.WithLabelValues(s.config.SourceType, "error").Inc()
				// Skip to next ledger
				currentSeq++
				continue
			}

			// Send record
			select {
			case s.recordsChannel <- record:
				batchesGenerated.WithLabelValues(s.config.SourceType).Inc()
				ledgersProcessed.WithLabelValues(s.config.SourceType, "success").Inc()
				s.updateCurrentLedger(currentSeq)
				s.updateProcessingStats(true, false, false)

				log.Debug().
					Uint32("sequence", currentSeq).
					Int64("num_rows", record.NumRows()).
					Msg("Sent ledger record")

				currentSeq++
			case <-ctx.Done():
				record.Release()
				return ctx.Err()
			default:
				log.Warn().Msg("Records channel full, dropping record")
				record.Release()
			}
		}
	}
}

// processBoundedRange processes a specific range of ledgers
func (s *StellarSourceService) processBoundedRange(ctx context.Context, startSeq, endSeq uint32) error {
	log.Info().
		Uint32("start_seq", startSeq).
		Uint32("end_seq", endSeq).
		Msg("Starting bounded range processing")

	// Use streaming for better performance with large ranges
	recordChan, err := s.backendClient.StreamLedgerRecords(ctx, startSeq, endSeq)
	if err != nil {
		return fmt.Errorf("failed to start ledger record stream: %w", err)
	}

	batchCount := 0

	for recordResult := range recordChan {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if recordResult.Error != nil {
			log.Error().
				Err(recordResult.Error).
				Uint32("start_seq", recordResult.StartSeq).
				Uint32("end_seq", recordResult.EndSeq).
				Msg("Error in ledger record stream")
			ledgersProcessed.WithLabelValues(s.config.SourceType, "error").Inc()
			s.updateProcessingStats(false, true, false)
			continue
		}

		// Send record
		select {
		case s.recordsChannel <- recordResult.Record:
			batchesGenerated.WithLabelValues(s.config.SourceType).Inc()
			ledgersProcessed.WithLabelValues(s.config.SourceType, "success").Inc()
			s.updateCurrentLedger(recordResult.EndSeq)
			s.updateProcessingStats(true, false, false)

			batchCount++
			log.Debug().
				Int("batch_count", batchCount).
				Uint32("start_seq", recordResult.StartSeq).
				Uint32("end_seq", recordResult.EndSeq).
				Int64("num_rows", recordResult.Record.NumRows()).
				Msg("Sent ledger record batch")

		case <-ctx.Done():
			recordResult.Record.Release()
			return ctx.Err()
		default:
			log.Warn().Msg("Records channel full, dropping record batch")
			recordResult.Record.Release()
		}
	}

	log.Info().
		Int("total_batches", batchCount).
		Uint32("start_seq", startSeq).
		Uint32("end_seq", endSeq).
		Msg("Bounded range processing completed")

	return nil
}

// processDataLake processes ledgers from data lake storage
func (s *StellarSourceService) processDataLake(ctx context.Context) error {
	if s.datalakeReader == nil {
		return fmt.Errorf("data lake reader not initialized")
	}

	log.Info().Msg("Starting data lake processing")

	// Get list of ledgers to process
	ledgers, err := s.datalakeReader.ListLedgers(ctx, s.config.StartLedger, s.config.EndLedger)
	if err != nil {
		return fmt.Errorf("failed to list ledgers: %w", err)
	}

	log.Info().
		Int("total_ledgers", len(ledgers)).
		Uint32("start_ledger", s.config.StartLedger).
		Uint32("end_ledger", s.config.EndLedger).
		Msg("Processing data lake ledgers")

	// Create worker pool for concurrent processing
	ledgerChan := make(chan uint32, len(ledgers))
	for _, ledger := range ledgers {
		ledgerChan <- ledger
	}
	close(ledgerChan)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < s.config.ConcurrentReaders; i++ {
		wg.Add(1)
		go s.dataLakeWorker(ctx, &wg, ledgerChan)
	}

	// Wait for all workers to complete
	wg.Wait()

	log.Info().Msg("Data lake processing completed")
	return nil
}

// dataLakeWorker processes ledgers from the data lake concurrently
func (s *StellarSourceService) dataLakeWorker(ctx context.Context, wg *sync.WaitGroup, ledgerChan <-chan uint32) {
	defer wg.Done()

	builder := schemas.NewStellarLedgerBuilder(s.pool)
	defer builder.Release()

	batchCount := 0

	for ledgerSeq := range ledgerChan {
		select {
		case <-ctx.Done():
			return
		default:
		}

		timer := prometheus.NewTimer(processingDuration.WithLabelValues(s.config.SourceType, "ledger"))

		// Read ledger from data lake
		ledgerData, err := s.datalakeReader.GetLedger(ctx, ledgerSeq)
		if err != nil {
			log.Error().
				Err(err).
				Uint32("ledger", ledgerSeq).
				Msg("Failed to read ledger from data lake")
			ledgersProcessed.WithLabelValues(s.config.SourceType, "error").Inc()
			timer.ObserveDuration()
			continue
		}

		// Process ledger XDR with comprehensive validation
		sourceURL := s.datalakeReader.GetSourceURL(ledgerSeq)
		processedData, err := s.xdrProcessor.ProcessLedgerXDR(ledgerData, "datalake", sourceURL)
		if err != nil {
			// Attempt recovery if strict validation fails
			log.Warn().
				Err(err).
				Uint32("ledger", ledgerSeq).
				Msg("Primary XDR processing failed, attempting recovery")
			
			processedData, err = s.xdrProcessor.RecoverFromXDRError(ledgerData, err)
			if err != nil {
				log.Error().
					Err(err).
					Uint32("ledger", ledgerSeq).
					Msg("Failed to process ledger XDR even with recovery")
				ledgersProcessed.WithLabelValues(s.config.SourceType, "error").Inc()
				s.updateProcessingStats(false, true, false)
				timer.ObserveDuration()
				continue
			} else {
				s.updateProcessingStats(true, false, true) // Recovered
				log.Info().
					Uint32("ledger", ledgerSeq).
					Msg("Successfully recovered from XDR processing error")
			}
		} else {
			s.updateProcessingStats(true, false, false) // Clean processing
		}

		// Add processed data to builder
		if err := builder.AddProcessedLedger(processedData); err != nil {
			log.Error().
				Err(err).
				Uint32("ledger", ledgerSeq).
				Msg("Failed to add processed ledger to builder")
			ledgersProcessed.WithLabelValues(s.config.SourceType, "error").Inc()
			s.updateProcessingStats(false, true, false)
			timer.ObserveDuration()
			continue
		}

		s.updateCurrentLedger(ledgerSeq)
		ledgersProcessed.WithLabelValues(s.config.SourceType, "success").Inc()
		timer.ObserveDuration()

		batchCount++

		// Send batch when full
		if batchCount >= s.config.BatchSize {
			record := builder.NewRecord()
			
			select {
			case s.recordsChannel <- record:
				batchesGenerated.WithLabelValues(s.config.SourceType).Inc()
				log.Debug().
					Int("ledgers_in_batch", batchCount).
					Uint32("latest_ledger", ledgerSeq).
					Msg("Generated Arrow batch from data lake")
			case <-ctx.Done():
				record.Release()
				return
			default:
				log.Warn().Msg("Records channel full, dropping batch")
				record.Release()
			}

			// Reset for next batch
			builder.Release()
			builder = schemas.NewStellarLedgerBuilder(s.pool)
			batchCount = 0
		}
	}

	// Send final partial batch if any
	if batchCount > 0 {
		record := builder.NewRecord()
		
		select {
		case s.recordsChannel <- record:
			batchesGenerated.WithLabelValues(s.config.SourceType).Inc()
			log.Debug().
				Int("ledgers_in_batch", batchCount).
				Msg("Generated final Arrow batch from data lake")
		case <-ctx.Done():
			record.Release()
			return
		default:
			log.Warn().Msg("Records channel full, dropping final batch")
			record.Release()
		}
	}
}

// updateCurrentLedger safely updates the current ledger being processed
func (s *StellarSourceService) updateCurrentLedger(ledger uint32) {
	s.mu.Lock()
	s.currentLedger = ledger
	s.mu.Unlock()
	
	currentLedger.WithLabelValues(s.config.SourceType).Set(float64(ledger))
}

// updateProcessingStats updates XDR processing statistics
func (s *StellarSourceService) updateProcessingStats(processed, hasError, recovered bool) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	
	if processed {
		s.stats.LedgersProcessed++
		s.stats.LastProcessed = time.Now()
	}
	
	if hasError {
		s.stats.ProcessingErrors++
	}
	
	if recovered {
		s.stats.LedgersRecovered++
	}
}

// GetProcessingStats returns current processing statistics
func (s *StellarSourceService) GetProcessingStats() ProcessingStats {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	return s.stats
}

// GetMetrics implements flowctl.MetricsProvider interface
func (s *StellarSourceService) GetMetrics() map[string]float64 {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()

	metrics := make(map[string]float64)
	
	// Basic processing statistics
	metrics["ledgers_processed_total"] = float64(s.stats.LedgersProcessed)
	metrics["ledgers_recovered_total"] = float64(s.stats.LedgersRecovered)
	metrics["validation_errors_total"] = float64(s.stats.ValidationErrors)
	metrics["processing_errors_total"] = float64(s.stats.ProcessingErrors)
	
	// Current ledger
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
	
	// Processing duration metrics
	if !s.stats.StartTime.IsZero() {
		uptime := time.Since(s.stats.StartTime).Seconds()
		metrics["uptime_seconds"] = uptime
		
		if s.stats.LedgersProcessed > 0 && uptime > 0 {
			metrics["ledgers_per_second"] = float64(s.stats.LedgersProcessed) / uptime
		}
	}
	
	// Time since last processed
	if !s.stats.LastProcessed.IsZero() {
		metrics["seconds_since_last_processed"] = time.Since(s.stats.LastProcessed).Seconds()
	}
	
	return metrics
}