package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/rs/zerolog/log"

	"github.com/withobsrvr/obsrvr-stellar-components/schemas"
)

// ParquetWriter handles writing TTP events to Parquet files using Apache Arrow
type ParquetWriter struct {
	config     *Config
	pool       memory.Allocator
	mu         sync.Mutex
	eventBatch []*TTPEvent
	batchCount int
}

// NewParquetWriter creates a new Parquet writer with Arrow integration
func NewParquetWriter(config *Config, pool memory.Allocator) (*ParquetWriter, error) {
	if err := createDirectories(config.ParquetPath); err != nil {
		return nil, err
	}

	return &ParquetWriter{
		config:     config,
		pool:       pool,
		eventBatch: make([]*TTPEvent, 0, config.ParquetBatchSize),
	}, nil
}

// WriteEvent adds an event to the batch
func (w *ParquetWriter) WriteEvent(event *TTPEvent) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.eventBatch = append(w.eventBatch, event)
	
	if len(w.eventBatch) >= w.config.ParquetBatchSize {
		return w.flushBatch()
	}
	
	return nil
}

// flushBatch writes the current batch to a Parquet file using Arrow
func (w *ParquetWriter) flushBatch() error {
	if len(w.eventBatch) == 0 {
		return nil
	}

	log.Debug().
		Int("events", len(w.eventBatch)).
		Int("batch_count", w.batchCount).
		Msg("Flushing Parquet batch")

	// Create Arrow record from TTP events
	record, err := w.createArrowRecord()
	if err != nil {
		return fmt.Errorf("failed to create Arrow record: %w", err)
	}
	defer record.Release()

	// Generate filename with timestamp and partition info
	now := time.Now()
	filename := fmt.Sprintf("ttp_events_%s_%04d.parquet", 
		now.Format("2006-01-02_15-04-05"), w.batchCount)
	
	// Create partitioned path
	partitionPath := w.config.ParquetPath
	for _, partition := range w.config.PartitionBy {
		switch partition {
		case "date":
			partitionPath = filepath.Join(partitionPath, "date="+now.Format("2006-01-02"))
		case "hour":
			partitionPath = filepath.Join(partitionPath, "hour="+now.Format("15"))
		case "asset_code":
			// Use the first event's asset code for partition
			if len(w.eventBatch) > 0 && w.eventBatch[0].AssetCode != nil {
				partitionPath = filepath.Join(partitionPath, "asset_code="+*w.eventBatch[0].AssetCode)
			} else {
				partitionPath = filepath.Join(partitionPath, "asset_code=XLM")
			}
		case "event_type":
			// Use the first event's type for partition
			if len(w.eventBatch) > 0 {
				partitionPath = filepath.Join(partitionPath, "event_type="+w.eventBatch[0].EventType)
			}
		}
	}

	// Ensure directory exists
	if err := os.MkdirAll(partitionPath, 0755); err != nil {
		return fmt.Errorf("failed to create partition directory: %w", err)
	}

	fullPath := filepath.Join(partitionPath, filename)

	// Write Arrow record to Parquet file
	if err := w.writeArrowRecordToParquet(record, fullPath); err != nil {
		return fmt.Errorf("failed to write Parquet file: %w", err)
	}

	log.Info().
		Str("file", fullPath).
		Int("events", len(w.eventBatch)).
		Int64("file_size", w.getFileSize(fullPath)).
		Str("compression", w.config.ParquetCompression).
		Msg("Successfully wrote Parquet batch")

	filesWritten.WithLabelValues("parquet").Inc()
	w.eventBatch = w.eventBatch[:0] // Clear batch
	w.batchCount++

	return nil
}

// createArrowRecord converts TTP events to Arrow record
func (w *ParquetWriter) createArrowRecord() (arrow.Record, error) {
	builder := schemas.NewTTPEventBuilder(w.pool)
	defer builder.Release()

	// Add all events to the builder
	for _, event := range w.eventBatch {
		eventData := schemas.TTPEventData{
			EventID:          event.EventID,
			LedgerSequence:   event.LedgerSequence,
			TransactionHash:  []byte(event.TransactionHash), // Convert hex string to bytes
			OperationIndex:   event.OperationIndex,
			Timestamp:        event.Timestamp,
			EventType:        event.EventType,
			AssetType:        event.AssetType,
			AssetCode:        event.AssetCode,
			AssetIssuer:      event.AssetIssuer,
			FromAccount:      event.FromAccount,
			ToAccount:        event.ToAccount,
			AmountRaw:        event.AmountRaw,
			AmountStr:        event.AmountStr,
			Successful:       event.Successful,
			MemoType:         event.MemoType,
			MemoValue:        event.MemoValue,
			FeeCharged:       event.FeeCharged,
			ProcessorVersion: "arrow-analytics-sink-v1.0.0",
		}

		if err := builder.AddEvent(eventData); err != nil {
			return nil, fmt.Errorf("failed to add event to builder: %w", err)
		}
	}

	return builder.NewRecord(), nil
}

// writeArrowRecordToParquet writes an Arrow record to a Parquet file
func (w *ParquetWriter) writeArrowRecordToParquet(record arrow.Record, filepath string) error {
	// Create Parquet file
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Configure Parquet writer properties with compression
	var compressionCodec compress.Compression
	switch w.config.ParquetCompression {
	case "snappy":
		compressionCodec = compress.Codecs.Snappy
	case "gzip":
		compressionCodec = compress.Codecs.Gzip  
	case "lz4":
		compressionCodec = compress.Codecs.Lz4
	case "zstd":
		compressionCodec = compress.Codecs.Zstd
	case "brotli":
		compressionCodec = compress.Codecs.Brotli
	default:
		compressionCodec = compress.Codecs.Uncompressed
	}
	
	props := parquet.NewWriterProperties(parquet.WithCompression(compressionCodec))

	// Create Arrow properties for the writer
	arrowProps := pqarrow.DefaultWriterProps()

	// Create table from record for writing
	table := array.NewTableFromRecords(record.Schema(), []arrow.Record{record})
	defer table.Release()

	// Create Arrow-to-Parquet writer and write table
	err = pqarrow.WriteTable(table, file, table.NumRows(), props, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to write Parquet table: %w", err)
	}

	return nil
}

// getFileSize returns the size of a file in bytes
func (w *ParquetWriter) getFileSize(filepath string) int64 {
	if info, err := os.Stat(filepath); err == nil {
		return info.Size()
	}
	return 0
}

// Close flushes any remaining events and closes the writer
func (w *ParquetWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushBatch()
}

// GetStats returns statistics about the Parquet writer
func (w *ParquetWriter) GetStats() map[string]interface{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	return map[string]interface{}{
		"batches_written":    w.batchCount,
		"events_in_buffer":   len(w.eventBatch),
		"buffer_utilization": float64(len(w.eventBatch)) / float64(w.config.ParquetBatchSize),
		"compression":        w.config.ParquetCompression,
		"partitioning":       w.config.PartitionBy,
	}
}

// ValidateParquetFile validates that a Parquet file can be read correctly
func ValidateParquetFile(filepath string, pool memory.Allocator) error {
	// Open the file for reading
	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()
	
	// Create reader properties
	readerProps := parquet.NewReaderProperties(pool)
	arrowReadProps := pqarrow.ArrowReadProperties{}
	
	// Read the entire Parquet file as a table
	table, err := pqarrow.ReadTable(context.Background(), file, readerProps, arrowReadProps, pool)
	if err != nil {
		return fmt.Errorf("failed to read Parquet file: %w", err)
	}
	defer table.Release()

	// Validate schema matches expected TTP event schema
	expectedSchema := schemas.TTPEventSchema
	if !schemasCompatible(table.Schema(), expectedSchema) {
		return fmt.Errorf("schema mismatch in Parquet file")
	}

	// Validate we have data
	if table.NumRows() == 0 {
		return fmt.Errorf("Parquet file is empty")
	}

	log.Debug().
		Str("file", filepath).
		Int64("rows", table.NumRows()).
		Int("columns", table.Schema().NumFields()).
		Msg("Parquet file validation successful")

	return nil
}

// schemasCompatible checks if two Arrow schemas are compatible
func schemasCompatible(schema1, schema2 *arrow.Schema) bool {
	if schema1.NumFields() != schema2.NumFields() {
		return false
	}

	for i := 0; i < schema1.NumFields(); i++ {
		field1 := schema1.Field(i)
		field2 := schema2.Field(i)
		
		if field1.Name != field2.Name {
			return false
		}
		
		if !arrow.TypeEqual(field1.Type, field2.Type) {
			return false
		}
	}

	return true
}