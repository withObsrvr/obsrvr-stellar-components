package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/rs/zerolog/log"
)

// ParquetWriter is now implemented in parquet_writer.go with full Arrow integration

// JSONWriter handles writing events to JSON files
type JSONWriter struct {
	config *Config
	mu     sync.Mutex
	file   *os.File
	encoder *json.Encoder
	eventCount int
}

// NewJSONWriter creates a new JSON writer
func NewJSONWriter(config *Config) (*JSONWriter, error) {
	if err := createDirectories(config.JSONPath); err != nil {
		return nil, err
	}

	// Create initial file
	filename := fmt.Sprintf("ttp_events_%s.json", time.Now().Format("2006-01-02_15-04-05"))
	fullPath := filepath.Join(config.JSONPath, filename)
	
	file, err := os.Create(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create JSON file: %w", err)
	}

	encoder := json.NewEncoder(file)
	if config.JSONFormat == "pretty" {
		encoder.SetIndent("", "  ")
	}

	return &JSONWriter{
		config:  config,
		file:    file,
		encoder: encoder,
	}, nil
}

// WriteEvent writes an event to the JSON file
func (w *JSONWriter) WriteEvent(event *TTPEvent) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.encoder.Encode(event); err != nil {
		return fmt.Errorf("failed to encode JSON event: %w", err)
	}

	w.eventCount++

	// Rotate file every 10000 events
	if w.eventCount%10000 == 0 {
		w.rotateFile()
	}

	return nil
}

// rotateFile creates a new JSON file
func (w *JSONWriter) rotateFile() error {
	if w.file != nil {
		w.file.Close()
		filesWritten.WithLabelValues("json").Inc()
	}

	filename := fmt.Sprintf("ttp_events_%s.json", time.Now().Format("2006-01-02_15-04-05"))
	fullPath := filepath.Join(w.config.JSONPath, filename)
	
	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create new JSON file: %w", err)
	}

	w.file = file
	w.encoder = json.NewEncoder(file)
	if w.config.JSONFormat == "pretty" {
		w.encoder.SetIndent("", "  ")
	}

	log.Debug().Str("file", fullPath).Msg("Rotated JSON file")
	return nil
}

// Close closes the JSON writer
func (w *JSONWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.file != nil {
		filesWritten.WithLabelValues("json").Inc()
		return w.file.Close()
	}
	return nil
}

// CSVWriter handles writing events to CSV files
type CSVWriter struct {
	config *Config
	mu     sync.Mutex
	file   *os.File
	writer *csv.Writer
	eventCount int
	headerWritten bool
}

// NewCSVWriter creates a new CSV writer
func NewCSVWriter(config *Config) (*CSVWriter, error) {
	if err := createDirectories(config.CSVPath); err != nil {
		return nil, err
	}

	// Create initial file
	filename := fmt.Sprintf("ttp_events_%s.csv", time.Now().Format("2006-01-02_15-04-05"))
	fullPath := filepath.Join(config.CSVPath, filename)
	
	file, err := os.Create(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create CSV file: %w", err)
	}

	writer := csv.NewWriter(file)
	if config.CSVDelimiter != "," {
		writer.Comma = rune(config.CSVDelimiter[0])
	}

	csvWriter := &CSVWriter{
		config: config,
		file:   file,
		writer: writer,
	}

	// Write header
	if err := csvWriter.writeHeader(); err != nil {
		return nil, err
	}

	return csvWriter, nil
}

// writeHeader writes the CSV header row
func (w *CSVWriter) writeHeader() error {
	header := []string{
		"event_id", "ledger_sequence", "transaction_hash", "operation_index",
		"timestamp", "event_type", "asset_type", "asset_code", "asset_issuer",
		"from_account", "to_account", "amount_raw", "amount_str",
		"successful", "memo_type", "memo_value", "fee_charged",
	}
	
	if err := w.writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}
	
	w.headerWritten = true
	return nil
}

// WriteEvent writes an event to the CSV file
func (w *CSVWriter) WriteEvent(event *TTPEvent) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Convert event to CSV row
	row := []string{
		event.EventID,
		fmt.Sprintf("%d", event.LedgerSequence),
		event.TransactionHash,
		fmt.Sprintf("%d", event.OperationIndex),
		event.Timestamp.Format(time.RFC3339),
		event.EventType,
		event.AssetType,
		stringOrEmpty(event.AssetCode),
		stringOrEmpty(event.AssetIssuer),
		event.FromAccount,
		event.ToAccount,
		fmt.Sprintf("%d", event.AmountRaw),
		event.AmountStr,
		fmt.Sprintf("%t", event.Successful),
		stringOrEmpty(event.MemoType),
		stringOrEmpty(event.MemoValue),
		fmt.Sprintf("%d", event.FeeCharged),
	}

	if err := w.writer.Write(row); err != nil {
		return fmt.Errorf("failed to write CSV row: %w", err)
	}

	w.writer.Flush()
	w.eventCount++

	// Rotate file every 50000 events
	if w.eventCount%50000 == 0 {
		w.rotateFile()
	}

	return nil
}

// rotateFile creates a new CSV file
func (w *CSVWriter) rotateFile() error {
	if w.file != nil {
		w.writer.Flush()
		w.file.Close()
		filesWritten.WithLabelValues("csv").Inc()
	}

	filename := fmt.Sprintf("ttp_events_%s.csv", time.Now().Format("2006-01-02_15-04-05"))
	fullPath := filepath.Join(w.config.CSVPath, filename)
	
	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create new CSV file: %w", err)
	}

	w.file = file
	w.writer = csv.NewWriter(file)
	if w.config.CSVDelimiter != "," {
		w.writer.Comma = rune(w.config.CSVDelimiter[0])
	}

	// Write header to new file
	if err := w.writeHeader(); err != nil {
		return err
	}

	log.Debug().Str("file", fullPath).Msg("Rotated CSV file")
	return nil
}

// Close closes the CSV writer
func (w *CSVWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.writer != nil {
		w.writer.Flush()
		filesWritten.WithLabelValues("csv").Inc()
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// Helper function to handle nullable strings
func stringOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}