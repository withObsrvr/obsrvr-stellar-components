package main

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/xdr"
)

type BackendConfig struct {
	Type              string `mapstructure:"backend_type"`           // rpc, archive, captive, storage
	NetworkPassphrase string `mapstructure:"network_passphrase"`

	// RPC Backend
	RPCEndpoint string `mapstructure:"rpc_endpoint"`

	// Archive Backend
	ArchiveURL string `mapstructure:"archive_url"`

	// Storage Backend
	StorageType string `mapstructure:"storage_type"` // s3, gcs, filesystem
	BucketName  string `mapstructure:"bucket_name"`
	Region      string `mapstructure:"region"`

	// Captive Core
	CoreBinaryPath string `mapstructure:"core_binary_path"`

	// Performance
	BufferSize      int `mapstructure:"buffer_size"`
	ConcurrentReads int `mapstructure:"concurrent_reads"`
}

type BackendMetrics struct {
	LedgerRequests int64
	LedgerSuccess  int64
	LedgerErrors   int64
	TotalLatency   time.Duration
}

func NewBackendMetrics() *BackendMetrics {
	return &BackendMetrics{}
}

func (m *BackendMetrics) IncLedgerRequests() {
	m.LedgerRequests++
}

func (m *BackendMetrics) IncLedgerSuccess() {
	m.LedgerSuccess++
}

func (m *BackendMetrics) IncLedgerErrors() {
	m.LedgerErrors++
}

func (m *BackendMetrics) ObserveLedgerLatency(latency time.Duration) {
	m.TotalLatency += latency
}

type StellarBackendManager struct {
	backend   ledgerbackend.LedgerBackend
	datastore datastore.DataStore
	config    *BackendConfig
	metrics   *BackendMetrics
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewStellarBackendManager(config *BackendConfig) (*StellarBackendManager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	manager := &StellarBackendManager{
		config:  config,
		ctx:     ctx,
		cancel:  cancel,
		metrics: NewBackendMetrics(),
	}

	if err := manager.initializeBackend(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize backend: %w", err)
	}

	return manager, nil
}

func (m *StellarBackendManager) initializeBackend() error {
	switch m.config.Type {
	case "rpc":
		return m.initRPCBackend()
	case "archive":
		return m.initArchiveBackend()
	case "captive":
		return m.initCaptiveBackend()
	case "storage":
		return m.initStorageBackend()
	default:
		return fmt.Errorf("unsupported backend type: %s", m.config.Type)
	}
}

func (m *StellarBackendManager) initRPCBackend() error {
	log.Info().Str("endpoint", m.config.RPCEndpoint).Msg("Initializing RPC backend")

	// Create RPC backend using the new API
	backend := ledgerbackend.NewRPCLedgerBackend(ledgerbackend.RPCLedgerBackendOptions{
		RPCServerURL: m.config.RPCEndpoint,
		BufferSize:   uint32(m.config.BufferSize),
	})

	m.backend = backend
	log.Info().Msg("RPC backend initialized successfully")
	return nil
}

func (m *StellarBackendManager) initArchiveBackend() error {
	log.Info().Str("archive_url", m.config.ArchiveURL).Msg("Initializing archive backend")

	archive, err := historyarchive.Connect(m.config.ArchiveURL, historyarchive.ArchiveOptions{})
	if err != nil {
		return fmt.Errorf("failed to connect to archive: %w", err)
	}
	_ = archive // Archive is not directly used in new API

	// Create basic datastore for archive access
	dataStore, err := datastore.NewDataStore(context.Background(), datastore.DataStoreConfig{
		Type: "memory", // Simple in-memory datastore for archive access
	})
	if err != nil {
		return fmt.Errorf("failed to create datastore: %w", err)
	}

	// Create buffered storage backend for archives
	config := ledgerbackend.BufferedStorageBackendConfig{
		RetryLimit:    3,
		RetryWait:     time.Second,
		BufferSize:    uint32(m.config.BufferSize),
		NumWorkers:    uint32(m.config.ConcurrentReads),
	}

	backend, err := ledgerbackend.NewBufferedStorageBackend(config, dataStore)
	if err != nil {
		return fmt.Errorf("failed to create buffered storage backend: %w", err)
	}

	m.backend = backend
	m.datastore = dataStore
	log.Info().Msg("Archive backend initialized successfully")
	return nil
}

func (m *StellarBackendManager) initCaptiveBackend() error {
	log.Info().Str("core_binary", m.config.CoreBinaryPath).Msg("Initializing captive core backend")

	config := ledgerbackend.CaptiveCoreConfig{
		BinaryPath:         m.config.CoreBinaryPath,
		NetworkPassphrase:  m.config.NetworkPassphrase,
		HistoryArchiveURLs: []string{}, // Can be empty for basic setup
	}

	backend, err := ledgerbackend.NewCaptive(config)
	if err != nil {
		return fmt.Errorf("failed to create captive core backend: %w", err)
	}

	m.backend = backend
	log.Info().Msg("Captive core backend initialized successfully")
	return nil
}

func (m *StellarBackendManager) initStorageBackend() error {
	log.Info().
		Str("storage_type", m.config.StorageType).
		Str("bucket", m.config.BucketName).
		Msg("Initializing storage backend")

	var store datastore.DataStore
	var err error

	ctx := context.Background()

	switch m.config.StorageType {
	case "s3":
		store, err = datastore.NewS3DataStore(ctx, datastore.DataStoreConfig{
			Type:   "s3",
			Params: map[string]string{
				"region": m.config.Region,
				"bucket": m.config.BucketName,
			},
		})
	case "gcs":
		store, err = datastore.NewGCSDataStore(ctx, datastore.DataStoreConfig{
			Type:   "gcs",
			Params: map[string]string{
				"project": m.config.Region, // Use region field for project
				"bucket":  m.config.BucketName,
			},
		})
	case "memory":
		store, err = datastore.NewDataStore(ctx, datastore.DataStoreConfig{
			Type: "memory",
		})
	default:
		return fmt.Errorf("unsupported storage type: %s", m.config.StorageType)
	}

	if err != nil {
		return fmt.Errorf("failed to create datastore: %w", err)
	}

	m.datastore = store

	// Create buffered storage backend using datastore
	config := ledgerbackend.BufferedStorageBackendConfig{
		RetryLimit:    3,
		RetryWait:     time.Second,
		BufferSize:    uint32(m.config.BufferSize),
		NumWorkers:    uint32(m.config.ConcurrentReads),
	}

	backend, err := ledgerbackend.NewBufferedStorageBackend(config, store)
	if err != nil {
		return fmt.Errorf("failed to create buffered storage backend: %w", err)
	}

	m.backend = backend
	log.Info().Msg("Storage backend initialized successfully")
	return nil
}

func (m *StellarBackendManager) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	m.metrics.IncLedgerRequests()
	start := time.Now()
	defer func() {
		m.metrics.ObserveLedgerLatency(time.Since(start))
	}()

	ledgerRange := ledgerbackend.UnboundedRange(sequence)

	if prepared, err := m.backend.IsPrepared(ctx, ledgerRange); err != nil {
		m.metrics.IncLedgerErrors()
		return xdr.LedgerCloseMeta{}, fmt.Errorf("failed to check if prepared: %w", err)
	} else if !prepared {
		log.Debug().Uint32("sequence", sequence).Msg("Preparing ledger range")
		if err := m.backend.PrepareRange(ctx, ledgerRange); err != nil {
			m.metrics.IncLedgerErrors()
			return xdr.LedgerCloseMeta{}, fmt.Errorf("failed to prepare range: %w", err)
		}
	}

	meta, err := m.backend.GetLedger(ctx, sequence)
	if err != nil {
		m.metrics.IncLedgerErrors()
		return xdr.LedgerCloseMeta{}, fmt.Errorf("failed to get ledger %d: %w", sequence, err)
	}

	m.metrics.IncLedgerSuccess()
	log.Debug().Uint32("sequence", sequence).Msg("Successfully retrieved ledger")
	return meta, nil
}

func (m *StellarBackendManager) StreamLedgers(ctx context.Context, startSeq, endSeq uint32) (<-chan LedgerResult, error) {
	resultChan := make(chan LedgerResult, m.config.BufferSize)

	go func() {
		defer close(resultChan)

		ledgerRange := ledgerbackend.BoundedRange(startSeq, endSeq)

		log.Info().
			Uint32("start_seq", startSeq).
			Uint32("end_seq", endSeq).
			Msg("Starting ledger stream")

		if prepared, err := m.backend.IsPrepared(ctx, ledgerRange); err != nil {
			resultChan <- LedgerResult{Error: fmt.Errorf("failed to check if prepared: %w", err)}
			return
		} else if !prepared {
			log.Debug().Msg("Preparing ledger range for streaming")
			if err := m.backend.PrepareRange(ctx, ledgerRange); err != nil {
				resultChan <- LedgerResult{Error: fmt.Errorf("failed to prepare range: %w", err)}
				return
			}
		}

		for seq := startSeq; seq <= endSeq; seq++ {
			select {
			case <-ctx.Done():
				log.Info().Msg("Ledger stream cancelled by context")
				return
			default:
				meta, err := m.backend.GetLedger(ctx, seq)
				if err != nil {
					log.Error().Err(err).Uint32("sequence", seq).Msg("Failed to get ledger in stream")
					resultChan <- LedgerResult{Error: fmt.Errorf("failed to get ledger %d: %w", seq, err)}
					continue
				}

				resultChan <- LedgerResult{LedgerMeta: meta, Sequence: seq}

				// Log progress every 1000 ledgers
				if seq%1000 == 0 {
					log.Info().Uint32("sequence", seq).Msg("Streaming progress")
				}
			}
		}

		log.Info().
			Uint32("start_seq", startSeq).
			Uint32("end_seq", endSeq).
			Msg("Ledger stream completed")
	}()

	return resultChan, nil
}

func (m *StellarBackendManager) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	// This implementation depends on the backend type
	// For now, return a placeholder - this would need to be implemented
	// based on the specific backend capabilities
	switch m.config.Type {
	case "rpc":
		// Would make an RPC call to get latest ledger
		return 0, fmt.Errorf("GetLatestLedgerSequence not implemented for RPC backend")
	case "archive":
		// Would check the archive for the latest available ledger
		return 0, fmt.Errorf("GetLatestLedgerSequence not implemented for archive backend")
	default:
		return 0, fmt.Errorf("GetLatestLedgerSequence not supported for backend type: %s", m.config.Type)
	}
}

func (m *StellarBackendManager) Close() error {
	log.Info().Msg("Closing Stellar backend manager")
	m.cancel()

	if m.backend != nil {
		return m.backend.Close()
	}

	return nil
}

func (m *StellarBackendManager) GetMetrics() *BackendMetrics {
	return m.metrics
}

type LedgerResult struct {
	LedgerMeta xdr.LedgerCloseMeta
	Sequence   uint32
	Error      error
}

// Helper function to get network passphrase from common networks
func GetNetworkPassphrase(networkName string) string {
	switch networkName {
	case "mainnet", "public":
		return network.PublicNetworkPassphrase
	case "testnet", "test":
		return network.TestNetworkPassphrase
	default:
		return networkName // Allow custom passphrases
	}
}