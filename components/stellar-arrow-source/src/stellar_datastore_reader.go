package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/xdr"
)

// StellarDatastoreReader implements ttp-processor-demo compatible datalake reading
type StellarDatastoreReader struct {
	dataStore datastore.DataStore
	schema    datastore.DataStoreSchema
	config    *Config
}

// NewStellarDatastoreReader creates a new datastore reader compatible with ttp-processor-demo
func NewStellarDatastoreReader(config *Config) (*StellarDatastoreReader, error) {
	log.Info().
		Str("backend", config.StorageBackend).
		Str("bucket", config.BucketName).
		Str("path", config.StoragePath).
		Uint32("ledgers_per_file", config.LedgersPerFile).
		Uint32("files_per_partition", config.FilesPerPartition).
		Msg("Creating Stellar datastore reader (ttp-processor-demo compatible)")

	ctx := context.Background()

	// Create datastore configuration matching ttp-processor-demo pattern
	var store datastore.DataStore
	var err error

	switch config.StorageBackend {
	case "gcs":
		store, err = datastore.NewGCSDataStore(ctx, datastore.DataStoreConfig{
			Type: "GCS",
			Params: map[string]string{
				"destination_bucket_path": fmt.Sprintf("%s/%s", config.BucketName, config.StoragePath),
			},
		})
	case "s3":
		store, err = datastore.NewS3DataStore(ctx, datastore.DataStoreConfig{
			Type: "S3",
			Params: map[string]string{
				"destination_bucket_path": fmt.Sprintf("%s/%s", config.BucketName, config.StoragePath),
				"region": config.AWSRegion,
			},
		})
	default:
		return nil, fmt.Errorf("unsupported storage backend for datastore reader: %s", config.StorageBackend)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create datastore: %w", err)
	}

	// Create schema matching ttp-processor-demo configuration
	schema := datastore.DataStoreSchema{
		LedgersPerFile:    config.LedgersPerFile,
		FilesPerPartition: config.FilesPerPartition,
	}

	reader := &StellarDatastoreReader{
		dataStore: store,
		schema:    schema,
		config:    config,
	}

	log.Info().
		Uint32("ledgers_per_file", schema.LedgersPerFile).
		Uint32("files_per_partition", schema.FilesPerPartition).
		Msg("Stellar datastore reader created successfully")

	return reader, nil
}

// ListLedgers lists available ledgers in the specified range using datastore
func (r *StellarDatastoreReader) ListLedgers(ctx context.Context, startSeq, endSeq uint32) ([]uint32, error) {
	log.Debug().
		Uint32("start", startSeq).
		Uint32("end", endSeq).
		Msg("Listing ledgers from datastore")

	var ledgers []uint32

	// For datastore, we need to check what files are available
	// This is a simplified approach - in production, you'd use the datastore's metadata capabilities
	for seq := startSeq; seq <= endSeq && (endSeq == 0 || seq <= endSeq); seq++ {
		// Check if this ledger exists by trying to read it
		if exists, err := r.ledgerExists(ctx, seq); err != nil {
			log.Warn().Err(err).Uint32("sequence", seq).Msg("Error checking ledger existence")
			continue
		} else if exists {
			ledgers = append(ledgers, seq)
		}

		// Break if we're looking for a large range to avoid timeout
		if len(ledgers) > 10000 {
			log.Warn().Int("found", len(ledgers)).Msg("Limiting ledger list to prevent timeout")
			break
		}
	}

	log.Info().
		Int("count", len(ledgers)).
		Uint32("start", startSeq).
		Uint32("end", endSeq).
		Msg("Listed ledgers from datastore")

	return ledgers, nil
}

// ledgerExists checks if a ledger exists in the datastore
func (r *StellarDatastoreReader) ledgerExists(ctx context.Context, sequence uint32) (bool, error) {
	// Try to get the ledger - if it exists, we can read it
	_, err := r.GetLedger(ctx, sequence)
	if err != nil {
		// If it's a "not found" type error, return false
		return false, nil
	}
	return true, nil
}

// GetLedger retrieves a specific ledger from the datastore
func (r *StellarDatastoreReader) GetLedger(ctx context.Context, sequence uint32) ([]byte, error) {
	log.Debug().
		Uint32("sequence", sequence).
		Msg("Getting ledger from datastore")

	start := time.Now()

	// Construct the file path for the ledger
	filePath := r.getFilePath(sequence)
	
	// Read the ledger file using datastore
	reader, err := r.dataStore.GetFile(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get ledger file for sequence %d: %w", sequence, err)
	}
	defer reader.Close()

	// Read the XDR data from the file
	var ledgerCloseMeta xdr.LedgerCloseMeta
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read ledger data: %w", err)
	}
	
	err = ledgerCloseMeta.UnmarshalBinary(data)
	if err != nil {
		return nil, fmt.Errorf("failed to read ledger %d from datastore: %w", sequence, err)
	}

	// Marshal to binary for processing
	xdrData, err := ledgerCloseMeta.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ledger %d XDR: %w", sequence, err)
	}

	log.Debug().
		Uint32("sequence", sequence).
		Int("size", len(xdrData)).
		Dur("duration", time.Since(start)).
		Msg("Successfully retrieved ledger from datastore")

	return xdrData, nil
}

// getFilePath constructs the file path for a ledger sequence
func (r *StellarDatastoreReader) getFilePath(sequence uint32) string {
	// Calculate which file contains this ledger based on ledgers_per_file
	fileNumber := sequence / r.config.LedgersPerFile
	
	// Calculate partition based on files_per_partition
	partition := fileNumber / r.config.FilesPerPartition
	
	// Construct the path following ttp-processor-demo pattern
	return fmt.Sprintf("ledgers/partition-%d/file-%d.xdr", partition, fileNumber)
}

// GetSourceURL returns the datastore URL for a ledger
func (r *StellarDatastoreReader) GetSourceURL(sequence uint32) string {
	return fmt.Sprintf("datastore://%s/%s/ledger_%d", r.config.BucketName, r.config.StoragePath, sequence)
}

// Close cleans up datastore resources
func (r *StellarDatastoreReader) Close() {
	log.Debug().Msg("Closing Stellar datastore reader")
	// Datastore will be garbage collected
}

// GetSchema returns the datastore schema configuration
func (r *StellarDatastoreReader) GetSchema() datastore.DataStoreSchema {
	return r.schema
}

// GetStats returns basic statistics about the datastore
func (r *StellarDatastoreReader) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"storage_backend":     r.config.StorageBackend,
		"bucket_name":         r.config.BucketName,
		"storage_path":        r.config.StoragePath,
		"ledgers_per_file":    r.schema.LedgersPerFile,
		"files_per_partition": r.schema.FilesPerPartition,
	}
}