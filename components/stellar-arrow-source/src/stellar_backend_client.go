package main

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/rs/zerolog/log"
)

// StellarBackendClient provides high-level interface for Stellar data operations
type StellarBackendClient struct {
	backendManager *StellarBackendManager
	converter      *XDRToArrowConverter
	pool           memory.Allocator
	config         *Config
	metrics        *ClientMetrics
}

type ClientMetrics struct {
	TotalLedgersProcessed int64
	TotalRecordsGenerated int64
	TotalBytesProcessed   int64
	AverageProcessingTime time.Duration
	ErrorCount            int64
}

func NewClientMetrics() *ClientMetrics {
	return &ClientMetrics{}
}

func (m *ClientMetrics) IncrementLedgersProcessed() {
	m.TotalLedgersProcessed++
}

func (m *ClientMetrics) IncrementRecordsGenerated(count int64) {
	m.TotalRecordsGenerated += count
}

func (m *ClientMetrics) IncrementBytesProcessed(bytes int64) {
	m.TotalBytesProcessed += bytes
}

func (m *ClientMetrics) ObserveProcessingTime(duration time.Duration) {
	// Simple average calculation - in production, use proper metrics aggregation
	if m.TotalLedgersProcessed > 0 {
		m.AverageProcessingTime = time.Duration(
			(int64(m.AverageProcessingTime)*m.TotalLedgersProcessed + int64(duration)) / (m.TotalLedgersProcessed + 1),
		)
	} else {
		m.AverageProcessingTime = duration
	}
}

func (m *ClientMetrics) IncrementErrors() {
	m.ErrorCount++
}

// NewStellarBackendClient creates a new Stellar backend client
func NewStellarBackendClient(config *Config) (*StellarBackendClient, error) {
	log.Info().Msg("Creating Stellar backend client")

	// Create memory allocator for Arrow operations
	pool := memory.NewGoAllocator()

	// Create backend configuration from main config
	backendConfig := &BackendConfig{
		Type:              config.BackendType,
		NetworkPassphrase: GetNetworkPassphrase(config.NetworkPassphrase),
		RPCEndpoint:       config.RPCEndpoint,
		ArchiveURL:        config.ArchiveURL,
		StorageType:       config.StorageBackend,
		BucketName:        config.BucketName,
		Region:            config.AWSRegion,
		CoreBinaryPath:    config.CoreBinaryPath,
		BufferSize:        config.BufferSize,
		ConcurrentReads:   config.ConcurrentReaders,
	}

	// Initialize backend manager
	backendManager, err := NewStellarBackendManager(backendConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create backend manager: %w", err)
	}

	// Create XDR to Arrow converter
	converter := NewXDRToArrowConverter(pool, config.BatchSize)

	client := &StellarBackendClient{
		backendManager: backendManager,
		converter:      converter,
		pool:           pool,
		config:         config,
		metrics:        NewClientMetrics(),
	}

	log.Info().
		Str("backend_type", config.BackendType).
		Str("network", config.NetworkPassphrase).
		Int("batch_size", config.BatchSize).
		Msg("Stellar backend client created successfully")

	return client, nil
}

// GetLedgerRecord retrieves a single ledger and converts it to Arrow format
func (c *StellarBackendClient) GetLedgerRecord(ctx context.Context, sequence uint32) (arrow.Record, error) {
	start := time.Now()
	defer func() {
		c.metrics.ObserveProcessingTime(time.Since(start))
	}()

	log.Debug().Uint32("sequence", sequence).Msg("Getting ledger record")

	// Get ledger from backend
	ledgerMeta, err := c.backendManager.GetLedger(ctx, sequence)
	if err != nil {
		c.metrics.IncrementErrors()
		return nil, fmt.Errorf("failed to get ledger %d: %w", sequence, err)
	}

	// Convert to Arrow format
	sourceType := c.config.BackendType
	sourceURL := c.getSourceURL(sequence)

	if err := c.converter.ConvertLedger(ledgerMeta, sourceType, sourceURL); err != nil {
		c.metrics.IncrementErrors()
		return nil, fmt.Errorf("failed to convert ledger %d to Arrow: %w", sequence, err)
	}

	// Build record
	record, err := c.converter.BuildRecord()
	if err != nil {
		c.metrics.IncrementErrors()
		return nil, fmt.Errorf("failed to build Arrow record for ledger %d: %w", sequence, err)
	}

	// Reset converter for next use
	c.converter.Reset()

	// Update metrics
	c.metrics.IncrementLedgersProcessed()
	c.metrics.IncrementRecordsGenerated(record.NumRows())

	// Calculate approximate bytes processed
	xdrBytes, _ := ledgerMeta.MarshalBinary()
	c.metrics.IncrementBytesProcessed(int64(len(xdrBytes)))

	log.Debug().
		Uint32("sequence", sequence).
		Int64("num_rows", record.NumRows()).
		Int("xdr_bytes", len(xdrBytes)).
		Msg("Successfully converted ledger to Arrow record")

	return record, nil
}

// StreamLedgerRecords streams multiple ledgers and converts them to Arrow format
func (c *StellarBackendClient) StreamLedgerRecords(ctx context.Context, startSeq, endSeq uint32) (<-chan LedgerRecordResult, error) {
	log.Info().
		Uint32("start_seq", startSeq).
		Uint32("end_seq", endSeq).
		Msg("Starting ledger record stream")

	resultChan := make(chan LedgerRecordResult, c.config.BufferSize)

	// Get ledger stream from backend
	ledgerChan, err := c.backendManager.StreamLedgers(ctx, startSeq, endSeq)
	if err != nil {
		close(resultChan)
		return nil, fmt.Errorf("failed to start ledger stream: %w", err)
	}

	// Process ledgers in background
	go func() {
		defer close(resultChan)

		batch := make([]LedgerResult, 0, c.config.BatchSize)
		sourceType := c.config.BackendType

		for ledgerResult := range ledgerChan {
			select {
			case <-ctx.Done():
				log.Info().Msg("Ledger record stream cancelled by context")
				return
			default:
				if ledgerResult.Error != nil {
					log.Error().Err(ledgerResult.Error).Uint32("sequence", ledgerResult.Sequence).Msg("Error in ledger stream")
					resultChan <- LedgerRecordResult{Error: ledgerResult.Error}
					continue
				}

				// Add to batch
				batch = append(batch, ledgerResult)

				// Process batch when full or at end of stream
				if len(batch) >= c.config.BatchSize {
					c.processBatch(batch, sourceType, resultChan)
					batch = batch[:0] // Reset slice
				}
			}
		}

		// Process remaining batch
		if len(batch) > 0 {
			c.processBatch(batch, sourceType, resultChan)
		}

		log.Info().
			Uint32("start_seq", startSeq).
			Uint32("end_seq", endSeq).
			Int64("total_processed", c.metrics.TotalLedgersProcessed).
			Msg("Ledger record stream completed")
	}()

	return resultChan, nil
}

// processBatch processes a batch of ledgers and sends the Arrow record result
func (c *StellarBackendClient) processBatch(batch []LedgerResult, sourceType string, resultChan chan<- LedgerRecordResult) {
	start := time.Now()

	// Get source URL from first ledger in batch
	sourceURL := c.getSourceURL(batch[0].Sequence)

	// Convert batch to Arrow record
	record, err := c.converter.ConvertLedgerBatch(batch, sourceType, sourceURL)
	if err != nil {
		log.Error().Err(err).Int("batch_size", len(batch)).Msg("Failed to convert ledger batch")
		c.metrics.IncrementErrors()
		resultChan <- LedgerRecordResult{Error: fmt.Errorf("failed to convert batch: %w", err)}
		return
	}

	// Update metrics
	c.metrics.IncrementLedgersProcessed()
	c.metrics.IncrementRecordsGenerated(record.NumRows())
	c.metrics.ObserveProcessingTime(time.Since(start))

	// Calculate total bytes processed in batch
	var totalBytes int64
	for _, ledger := range batch {
		if ledger.Error == nil {
			xdrBytes, _ := ledger.LedgerMeta.MarshalBinary()
			totalBytes += int64(len(xdrBytes))
		}
	}
	c.metrics.IncrementBytesProcessed(totalBytes)

	log.Debug().
		Int("batch_size", len(batch)).
		Int64("num_rows", record.NumRows()).
		Int64("total_bytes", totalBytes).
		Dur("processing_time", time.Since(start)).
		Msg("Successfully processed ledger batch")

	resultChan <- LedgerRecordResult{
		Record:   record,
		StartSeq: batch[0].Sequence,
		EndSeq:   batch[len(batch)-1].Sequence,
	}
}

// GetLatestLedgerSequence returns the latest available ledger sequence
func (c *StellarBackendClient) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	return c.backendManager.GetLatestLedgerSequence(ctx)
}

// getSourceURL generates a source URL based on backend type and configuration
func (c *StellarBackendClient) getSourceURL(sequence uint32) string {
	switch c.config.BackendType {
	case "rpc":
		return c.config.RPCEndpoint
	case "archive":
		return c.config.ArchiveURL
	case "storage":
		switch c.config.StorageBackend {
		case "s3":
			return fmt.Sprintf("s3://%s/ledgers/%08d.xdr", c.config.BucketName, sequence)
		case "gcs":
			return fmt.Sprintf("gs://%s/ledgers/%08d.xdr", c.config.BucketName, sequence)
		case "filesystem":
			return fmt.Sprintf("file://%s/%08d.xdr", c.config.StoragePath, sequence)
		default:
			return fmt.Sprintf("storage://%s/%08d.xdr", c.config.BucketName, sequence)
		}
	default:
		return fmt.Sprintf("ledger://%d", sequence)
	}
}

// GetMetrics returns current client metrics
func (c *StellarBackendClient) GetMetrics() *ClientMetrics {
	return c.metrics
}

// GetBackendMetrics returns backend-specific metrics
func (c *StellarBackendClient) GetBackendMetrics() *BackendMetrics {
	return c.backendManager.GetMetrics()
}

// Close cleanly shuts down the client and releases resources
func (c *StellarBackendClient) Close() error {
	log.Info().Msg("Closing Stellar backend client")

	// Close backend manager
	if err := c.backendManager.Close(); err != nil {
		log.Error().Err(err).Msg("Error closing backend manager")
	}

	// Release converter resources
	if c.converter != nil {
		c.converter.Release()
	}

	log.Info().
		Int64("total_ledgers", c.metrics.TotalLedgersProcessed).
		Int64("total_records", c.metrics.TotalRecordsGenerated).
		Int64("total_bytes", c.metrics.TotalBytesProcessed).
		Int64("errors", c.metrics.ErrorCount).
		Msg("Stellar backend client closed")

	return nil
}

// HealthCheck performs a health check on the backend
func (c *StellarBackendClient) HealthCheck(ctx context.Context) error {
	// Try to get latest ledger sequence as a health check
	_, err := c.backendManager.GetLatestLedgerSequence(ctx)
	if err != nil {
		return fmt.Errorf("backend health check failed: %w", err)
	}

	log.Debug().Msg("Backend health check passed")
	return nil
}

// LedgerRecordResult represents the result of ledger processing
type LedgerRecordResult struct {
	Record   arrow.Record
	StartSeq uint32
	EndSeq   uint32
	Error    error
}

// ValidateConfiguration checks if the client configuration is valid
func (c *StellarBackendClient) ValidateConfiguration() error {
	log.Debug().Msg("Validating backend client configuration")

	// Check required fields based on backend type
	switch c.config.BackendType {
	case "rpc":
		if c.config.RPCEndpoint == "" {
			return fmt.Errorf("RPC endpoint is required for RPC backend")
		}
	case "archive":
		if c.config.ArchiveURL == "" {
			return fmt.Errorf("archive URL is required for archive backend")
		}
	case "storage":
		if c.config.StorageBackend == "" {
			return fmt.Errorf("storage backend type is required for storage backend")
		}
		if c.config.BucketName == "" && c.config.StoragePath == "" {
			return fmt.Errorf("bucket name or storage path is required for storage backend")
		}
	case "captive":
		if c.config.CoreBinaryPath == "" {
			return fmt.Errorf("core binary path is required for captive backend")
		}
	default:
		return fmt.Errorf("unsupported backend type: %s", c.config.BackendType)
	}

	if c.config.NetworkPassphrase == "" {
		return fmt.Errorf("network passphrase is required")
	}

	if c.config.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive")
	}

	log.Debug().Msg("Backend client configuration is valid")
	return nil
}