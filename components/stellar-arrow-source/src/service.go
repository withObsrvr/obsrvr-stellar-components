package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/rs/zerolog/log"

	"github.com/withobsrvr/obsrvr-stellar-components/schemas"
)

// StellarSourceService manages the data ingestion from various Stellar sources
type StellarSourceService struct {
	config   *Config
	pool     memory.Allocator
	registry *schemas.SchemaRegistry

	// Data sources
	rpcClient    RPCClient
	datalakeReader DataLakeReader

	// Processing state
	mu             sync.RWMutex
	isRunning      bool
	currentLedger  uint32
	recordsChannel chan arrow.Record
	errorChannel   chan error

	// Shutdown
	shutdownOnce sync.Once
}

// NewStellarSourceService creates a new source service
func NewStellarSourceService(config *Config, pool memory.Allocator, registry *schemas.SchemaRegistry) (*StellarSourceService, error) {
	service := &StellarSourceService{
		config:         config,
		pool:           pool,
		registry:       registry,
		recordsChannel: make(chan arrow.Record, config.BufferSize),
		errorChannel:   make(chan error, 10),
	}

	// Initialize the appropriate data source
	switch config.SourceType {
	case "rpc":
		rpcClient, err := NewRPCClient(config)
		if err != nil {
			return nil, fmt.Errorf("failed to create RPC client: %w", err)
		}
		service.rpcClient = rpcClient

	case "datalake":
		datalakeReader, err := NewDataLakeReader(config)
		if err != nil {
			return nil, fmt.Errorf("failed to create data lake reader: %w", err)
		}
		service.datalakeReader = datalakeReader

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
	case "rpc":
		return s.processRPCStream(ctx)
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
		if s.rpcClient != nil {
			s.rpcClient.Close()
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

// processRPCStream processes ledgers from RPC endpoints
func (s *StellarSourceService) processRPCStream(ctx context.Context) error {
	if s.rpcClient == nil {
		return fmt.Errorf("RPC client not initialized")
	}

	log.Info().Msg("Starting RPC stream processing")

	// Create ledger builder
	builder := schemas.NewStellarLedgerBuilder(s.pool)
	defer builder.Release()

	var ledgerSeq uint32 = s.config.StartLedger
	if ledgerSeq == 0 {
		// Get latest ledger if starting from 0
		latest, err := s.rpcClient.GetLatestLedger(ctx)
		if err != nil {
			return fmt.Errorf("failed to get latest ledger: %w", err)
		}
		ledgerSeq = latest
		log.Info().Uint32("ledger", ledgerSeq).Msg("Starting from latest ledger")
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	batchCount := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			timer := prometheus.NewTimer(processingDuration.WithLabelValues(s.config.SourceType, "batch"))
			
			// Process a batch of ledgers
			batchProcessed := 0
			for i := 0; i < s.config.BatchSize && (s.config.EndLedger == 0 || ledgerSeq <= s.config.EndLedger); i++ {
				ledgerData, err := s.rpcClient.GetLedger(ctx, ledgerSeq)
				if err != nil {
					log.Error().
						Err(err).
						Uint32("ledger", ledgerSeq).
						Msg("Failed to get ledger from RPC")
					ledgersProcessed.WithLabelValues(s.config.SourceType, "error").Inc()
					continue
				}

				// Add ledger to builder
				if err := builder.AddLedgerFromXDR(ledgerData, "rpc", s.config.RPCEndpoint); err != nil {
					log.Error().
						Err(err).
						Uint32("ledger", ledgerSeq).
						Msg("Failed to process ledger XDR")
					ledgersProcessed.WithLabelValues(s.config.SourceType, "error").Inc()
					continue
				}

				s.updateCurrentLedger(ledgerSeq)
				ledgersProcessed.WithLabelValues(s.config.SourceType, "success").Inc()
				batchProcessed++
				ledgerSeq++
			}

			timer.ObserveDuration()

			// Create and send record if we processed any ledgers
			if batchProcessed > 0 {
				record := builder.NewRecord()
				
				select {
				case s.recordsChannel <- record:
					batchesGenerated.WithLabelValues(s.config.SourceType).Inc()
					batchCount++
					log.Debug().
						Int("batch_count", batchCount).
						Int("ledgers_in_batch", batchProcessed).
						Uint32("current_ledger", ledgerSeq-1).
						Msg("Generated Arrow batch")
				case <-ctx.Done():
					record.Release()
					return ctx.Err()
				default:
					// Channel full, log warning
					log.Warn().Msg("Records channel full, dropping batch")
					record.Release()
				}

				// Create new builder for next batch
				builder.Release()
				builder = schemas.NewStellarLedgerBuilder(s.pool)
			}

			// Check if we've reached the end ledger
			if s.config.EndLedger != 0 && ledgerSeq > s.config.EndLedger {
				log.Info().
					Uint32("end_ledger", s.config.EndLedger).
					Msg("Reached end ledger, stopping")
				return nil
			}
		}
	}
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

		// Add ledger to builder
		sourceURL := s.datalakeReader.GetSourceURL(ledgerSeq)
		if err := builder.AddLedgerFromXDR(ledgerData, "datalake", sourceURL); err != nil {
			log.Error().
				Err(err).
				Uint32("ledger", ledgerSeq).
				Msg("Failed to process ledger XDR")
			ledgersProcessed.WithLabelValues(s.config.SourceType, "error").Inc()
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