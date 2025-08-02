package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/iterator"
)

// DataLakeReader interface defines methods for reading Stellar ledger data from storage
type DataLakeReader interface {
	ListLedgers(ctx context.Context, startSeq, endSeq uint32) ([]uint32, error)
	GetLedger(ctx context.Context, sequence uint32) ([]byte, error)
	GetSourceURL(sequence uint32) string
	Close()
}

// NewDataLakeReader creates a new data lake reader based on configuration
func NewDataLakeReader(config *Config) (DataLakeReader, error) {
	log.Info().
		Str("backend", config.StorageBackend).
		Str("bucket", config.BucketName).
		Str("path", config.StoragePath).
		Msg("Creating data lake reader")

	switch config.StorageBackend {
	case "s3":
		return NewS3DataLakeReader(config)
	case "gcs":
		return NewGCSDataLakeReader(config)
	case "filesystem":
		return NewFilesystemDataLakeReader(config)
	default:
		return nil, fmt.Errorf("unsupported storage backend: %s", config.StorageBackend)
	}
}

// S3DataLakeReader implements data lake reading from AWS S3
type S3DataLakeReader struct {
	client     *s3.Client
	bucketName string
	prefix     string
	region     string
}

// NewS3DataLakeReader creates a new S3 data lake reader
func NewS3DataLakeReader(config *Config) (*S3DataLakeReader, error) {
	log.Info().
		Str("bucket", config.BucketName).
		Str("region", config.AWSRegion).
		Msg("Creating S3 data lake reader")

	// Load AWS config
	cfg, err := awsConfig.LoadDefaultConfig(context.TODO(),
		awsConfig.WithRegion(config.AWSRegion),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	client := s3.NewFromConfig(cfg)

	return &S3DataLakeReader{
		client:     client,
		bucketName: config.BucketName,
		prefix:     "ledgers/", // Standard prefix for ledger data
		region:     config.AWSRegion,
	}, nil
}

// ListLedgers lists available ledgers in the specified range
func (r *S3DataLakeReader) ListLedgers(ctx context.Context, startSeq, endSeq uint32) ([]uint32, error) {
	log.Debug().
		Uint32("start", startSeq).
		Uint32("end", endSeq).
		Msg("Listing ledgers from S3")

	var ledgers []uint32

	// List objects with prefix
	paginator := s3.NewListObjectsV2Paginator(r.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(r.bucketName),
		Prefix: aws.String(r.prefix),
	})

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range output.Contents {
			// Extract ledger sequence from key
			// Expected format: ledgers/00012345.xdr
			key := *obj.Key
			if !strings.HasSuffix(key, ".xdr") {
				continue
			}

			// Extract sequence number
			basename := filepath.Base(key)
			seqStr := strings.TrimSuffix(basename, ".xdr")
			
			// Remove leading zeros
			seqStr = strings.TrimLeft(seqStr, "0")
			if seqStr == "" {
				seqStr = "0"
			}

			sequence, err := strconv.ParseUint(seqStr, 10, 32)
			if err != nil {
				log.Warn().
					Str("key", key).
					Str("sequence_str", seqStr).
					Msg("Failed to parse ledger sequence from S3 key")
				continue
			}

			seq := uint32(sequence)
			if (startSeq == 0 || seq >= startSeq) && (endSeq == 0 || seq <= endSeq) {
				ledgers = append(ledgers, seq)
			}
		}
	}

	// Sort ledgers
	sort.Slice(ledgers, func(i, j int) bool {
		return ledgers[i] < ledgers[j]
	})

	log.Info().
		Int("count", len(ledgers)).
		Uint32("start", startSeq).
		Uint32("end", endSeq).
		Msg("Listed ledgers from S3")

	return ledgers, nil
}

// GetLedger retrieves a specific ledger from S3
func (r *S3DataLakeReader) GetLedger(ctx context.Context, sequence uint32) ([]byte, error) {
	// Generate key: ledgers/00012345.xdr
	key := fmt.Sprintf("%s%08d.xdr", r.prefix, sequence)

	log.Debug().
		Uint32("sequence", sequence).
		Str("key", key).
		Msg("Getting ledger from S3")

	// Get object
	output, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get ledger %d from S3: %w", sequence, err)
	}
	defer output.Body.Close()

	// Read data
	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read ledger %d data from S3: %w", sequence, err)
	}

	log.Debug().
		Uint32("sequence", sequence).
		Int("size", len(data)).
		Msg("Successfully retrieved ledger from S3")

	return data, nil
}

// GetSourceURL returns the S3 URL for a ledger
func (r *S3DataLakeReader) GetSourceURL(sequence uint32) string {
	key := fmt.Sprintf("%s%08d.xdr", r.prefix, sequence)
	return fmt.Sprintf("s3://%s/%s", r.bucketName, key)
}

// Close cleans up S3 resources
func (r *S3DataLakeReader) Close() {
	log.Debug().Msg("Closing S3 data lake reader")
	// S3 client will be garbage collected
}

// GCSDataLakeReader implements data lake reading from Google Cloud Storage
type GCSDataLakeReader struct {
	client     *storage.Client
	bucketName string
	prefix     string
}

// NewGCSDataLakeReader creates a new GCS data lake reader
func NewGCSDataLakeReader(config *Config) (*GCSDataLakeReader, error) {
	log.Info().
		Str("bucket", config.BucketName).
		Str("project", config.GCPProject).
		Msg("Creating GCS data lake reader")

	// Create GCS client
	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return &GCSDataLakeReader{
		client:     client,
		bucketName: config.BucketName,
		prefix:     "ledgers/", // Standard prefix for ledger data
	}, nil
}

// ListLedgers lists available ledgers in the specified range
func (r *GCSDataLakeReader) ListLedgers(ctx context.Context, startSeq, endSeq uint32) ([]uint32, error) {
	log.Debug().
		Uint32("start", startSeq).
		Uint32("end", endSeq).
		Msg("Listing ledgers from GCS")

	var ledgers []uint32

	// List objects with prefix
	bucket := r.client.Bucket(r.bucketName)
	query := &storage.Query{Prefix: r.prefix}

	it := bucket.Objects(ctx, query)
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list GCS objects: %w", err)
		}

		// Extract ledger sequence from name
		if !strings.HasSuffix(objAttrs.Name, ".xdr") {
			continue
		}

		basename := filepath.Base(objAttrs.Name)
		seqStr := strings.TrimSuffix(basename, ".xdr")
		
		// Remove leading zeros
		seqStr = strings.TrimLeft(seqStr, "0")
		if seqStr == "" {
			seqStr = "0"
		}

		sequence, err := strconv.ParseUint(seqStr, 10, 32)
		if err != nil {
			log.Warn().
				Str("name", objAttrs.Name).
				Str("sequence_str", seqStr).
				Msg("Failed to parse ledger sequence from GCS object name")
			continue
		}

		seq := uint32(sequence)
		if (startSeq == 0 || seq >= startSeq) && (endSeq == 0 || seq <= endSeq) {
			ledgers = append(ledgers, seq)
		}
	}

	// Sort ledgers
	sort.Slice(ledgers, func(i, j int) bool {
		return ledgers[i] < ledgers[j]
	})

	log.Info().
		Int("count", len(ledgers)).
		Uint32("start", startSeq).
		Uint32("end", endSeq).
		Msg("Listed ledgers from GCS")

	return ledgers, nil
}

// GetLedger retrieves a specific ledger from GCS
func (r *GCSDataLakeReader) GetLedger(ctx context.Context, sequence uint32) ([]byte, error) {
	// Generate object name: ledgers/00012345.xdr
	objectName := fmt.Sprintf("%s%08d.xdr", r.prefix, sequence)

	log.Debug().
		Uint32("sequence", sequence).
		Str("object", objectName).
		Msg("Getting ledger from GCS")

	// Get object
	bucket := r.client.Bucket(r.bucketName)
	obj := bucket.Object(objectName)
	
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ledger %d from GCS: %w", sequence, err)
	}
	defer reader.Close()

	// Read data
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read ledger %d data from GCS: %w", sequence, err)
	}

	log.Debug().
		Uint32("sequence", sequence).
		Int("size", len(data)).
		Msg("Successfully retrieved ledger from GCS")

	return data, nil
}

// GetSourceURL returns the GCS URL for a ledger
func (r *GCSDataLakeReader) GetSourceURL(sequence uint32) string {
	objectName := fmt.Sprintf("%s%08d.xdr", r.prefix, sequence)
	return fmt.Sprintf("gs://%s/%s", r.bucketName, objectName)
}

// Close cleans up GCS resources
func (r *GCSDataLakeReader) Close() {
	log.Debug().Msg("Closing GCS data lake reader")
	if r.client != nil {
		r.client.Close()
	}
}

// FilesystemDataLakeReader implements data lake reading from local filesystem
type FilesystemDataLakeReader struct {
	basePath string
}

// NewFilesystemDataLakeReader creates a new filesystem data lake reader
func NewFilesystemDataLakeReader(config *Config) (*FilesystemDataLakeReader, error) {
	log.Info().
		Str("path", config.StoragePath).
		Msg("Creating filesystem data lake reader")

	// Check if path exists
	if _, err := os.Stat(config.StoragePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("storage path does not exist: %s", config.StoragePath)
	}

	return &FilesystemDataLakeReader{
		basePath: config.StoragePath,
	}, nil
}

// ListLedgers lists available ledgers in the specified range
func (r *FilesystemDataLakeReader) ListLedgers(ctx context.Context, startSeq, endSeq uint32) ([]uint32, error) {
	log.Debug().
		Uint32("start", startSeq).
		Uint32("end", endSeq).
		Str("path", r.basePath).
		Msg("Listing ledgers from filesystem")

	var ledgers []uint32

	// Walk directory
	err := filepath.Walk(r.basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Check if it's an XDR file
		if !strings.HasSuffix(info.Name(), ".xdr") {
			return nil
		}

		// Extract sequence number
		seqStr := strings.TrimSuffix(info.Name(), ".xdr")
		
		// Remove leading zeros
		seqStr = strings.TrimLeft(seqStr, "0")
		if seqStr == "" {
			seqStr = "0"
		}

		sequence, err := strconv.ParseUint(seqStr, 10, 32)
		if err != nil {
			log.Warn().
				Str("file", info.Name()).
				Str("sequence_str", seqStr).
				Msg("Failed to parse ledger sequence from filename")
			return nil
		}

		seq := uint32(sequence)
		if (startSeq == 0 || seq >= startSeq) && (endSeq == 0 || seq <= endSeq) {
			ledgers = append(ledgers, seq)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk filesystem: %w", err)
	}

	// Sort ledgers
	sort.Slice(ledgers, func(i, j int) bool {
		return ledgers[i] < ledgers[j]
	})

	log.Info().
		Int("count", len(ledgers)).
		Uint32("start", startSeq).
		Uint32("end", endSeq).
		Msg("Listed ledgers from filesystem")

	return ledgers, nil
}

// GetLedger retrieves a specific ledger from filesystem
func (r *FilesystemDataLakeReader) GetLedger(ctx context.Context, sequence uint32) ([]byte, error) {
	// Generate filename: 00012345.xdr
	filename := fmt.Sprintf("%08d.xdr", sequence)
	filepath := filepath.Join(r.basePath, filename)

	log.Debug().
		Uint32("sequence", sequence).
		Str("file", filepath).
		Msg("Getting ledger from filesystem")

	// Read file
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read ledger %d from filesystem: %w", sequence, err)
	}

	log.Debug().
		Uint32("sequence", sequence).
		Int("size", len(data)).
		Msg("Successfully retrieved ledger from filesystem")

	return data, nil
}

// GetSourceURL returns the filesystem path for a ledger
func (r *FilesystemDataLakeReader) GetSourceURL(sequence uint32) string {
	filename := fmt.Sprintf("%08d.xdr", sequence)
	return filepath.Join(r.basePath, filename)
}

// Close cleans up filesystem resources
func (r *FilesystemDataLakeReader) Close() {
	log.Debug().Msg("Closing filesystem data lake reader")
	// No cleanup needed for filesystem
}