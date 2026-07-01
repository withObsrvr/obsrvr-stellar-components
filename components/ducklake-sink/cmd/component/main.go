package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "github.com/duckdb/duckdb-go/v2"
	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	"github.com/withObsrvr/flowctl-sdk/pkg/consumer"
	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func main() {
	sink, err := NewDuckLakeSink(DuckLakeConfigFromEnv())
	if err != nil {
		panic(err)
	}
	defer sink.Close()

	consumer.Run(consumer.ConsumerConfig{
		ConsumerName: "Stellar Ledger DuckLake Sink",
		ComponentID:  getenv("COMPONENT_ID", "ducklake-sink"),
		InputTypes:   []string{contracts.LedgerBatchEventType},
		OnEvent: func(ctx context.Context, event *flowctlv1.Event) error {
			if event.Type != contracts.LedgerBatchEventType {
				return nil
			}
			var batch componentsv1.LedgerBatch
			if err := proto.Unmarshal(event.Payload, &batch); err != nil {
				return fmt.Errorf("unmarshal ledger batch: %w", err)
			}
			return sink.WriteBatch(&batch)
		},
	})
}

type DuckLakeConfig struct {
	CatalogPath string
	DataPath    string
	AttachName  string
}

func DuckLakeConfigFromEnv() DuckLakeConfig {
	return DuckLakeConfig{
		CatalogPath: getenv("DUCKLAKE_CATALOG_PATH", "ducklake/stellar.ducklake"),
		DataPath:    getenv("DUCKLAKE_DATA_PATH", "ducklake/data"),
		AttachName:  getenv("DUCKLAKE_ATTACH_NAME", "stellar_lake"),
	}
}

type DuckLakeSink struct {
	db         *sql.DB
	attachName string
	mu         sync.Mutex
}

func NewDuckLakeSink(cfg DuckLakeConfig) (*DuckLakeSink, error) {
	if cfg.CatalogPath == "" {
		return nil, fmt.Errorf("DUCKLAKE_CATALOG_PATH is required")
	}
	if cfg.DataPath == "" {
		return nil, fmt.Errorf("DUCKLAKE_DATA_PATH is required")
	}
	if cfg.AttachName == "" {
		return nil, fmt.Errorf("DUCKLAKE_ATTACH_NAME is required")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.CatalogPath), 0o755); err != nil && filepath.Dir(cfg.CatalogPath) != "." {
		return nil, fmt.Errorf("create DuckLake catalog directory: %w", err)
	}
	if err := os.MkdirAll(cfg.DataPath, 0o755); err != nil {
		return nil, fmt.Errorf("create DuckLake data directory: %w", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open embedded DuckDB: %w", err)
	}
	sink := &DuckLakeSink{db: db, attachName: sanitizeIdentifier(cfg.AttachName)}
	if err := sink.init(cfg); err != nil {
		db.Close()
		return nil, err
	}
	return sink, nil
}

func (s *DuckLakeSink) init(cfg DuckLakeConfig) error {
	stmts := []string{
		"INSTALL ducklake",
		"LOAD ducklake",
		fmt.Sprintf(
			"ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')",
			escapeSQLString(cfg.CatalogPath),
			s.attachName,
			escapeSQLString(cfg.DataPath),
		),
		"USE " + s.attachName,
		createLedgerBatchesSQL,
		createBronzeRowsSQL,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("ducklake init %q: %w", stmt, err)
		}
	}
	return nil
}

func (s *DuckLakeSink) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *DuckLakeSink) WriteBatch(batch *componentsv1.LedgerBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	payloadJSON, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(batch)
	if err != nil {
		return err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin DuckLake transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		"DELETE FROM bronze_rows WHERE network_passphrase = ? AND ledger_sequence = ?",
		batch.NetworkPassphrase,
		batch.LedgerSequence,
	); err != nil {
		return fmt.Errorf("delete existing bronze rows: %w", err)
	}
	if _, err := tx.Exec(
		"DELETE FROM ledger_batches WHERE network_passphrase = ? AND ledger_sequence = ?",
		batch.NetworkPassphrase,
		batch.LedgerSequence,
	); err != nil {
		return fmt.Errorf("delete existing ledger batch: %w", err)
	}
	if _, err := tx.Exec(
		`INSERT INTO ledger_batches (
			network_passphrase,
			ledger_sequence,
			closed_at_unix,
			schema_version,
			extraction_version,
			transaction_count,
			operation_count,
			bronze_row_count,
			payload_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		batch.NetworkPassphrase,
		batch.LedgerSequence,
		batch.ClosedAtUnix,
		batch.SchemaVersion,
		batch.ExtractionVersion,
		len(batch.Transactions),
		len(batch.Operations),
		len(batch.BronzeRows),
		string(payloadJSON),
	); err != nil {
		return fmt.Errorf("insert ledger batch: %w", err)
	}

	stmt, err := tx.Prepare(`INSERT INTO bronze_rows (
		network_passphrase,
		ledger_sequence,
		ledger_range,
		row_ordinal,
		bronze_row_id,
		table_name,
		row_json
	) VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare bronze insert: %w", err)
	}
	defer stmt.Close()

	for i, row := range batch.BronzeRows {
		if _, err := stmt.Exec(
			row.NetworkPassphrase,
			row.LedgerSequence,
			row.LedgerRange,
			i,
			row.Id,
			row.TableName,
			row.RowJson,
		); err != nil {
			return fmt.Errorf("insert bronze row %d: %w", i, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit DuckLake transaction: %w", err)
	}
	return nil
}

func sanitizeIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "stellar_lake"
	}
	var b strings.Builder
	for i, r := range value {
		valid := r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || i > 0 && r >= '0' && r <= '9'
		if valid {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	return b.String()
}

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

const createLedgerBatchesSQL = `
CREATE TABLE IF NOT EXISTS ledger_batches (
	network_passphrase VARCHAR,
	ledger_sequence UBIGINT,
	closed_at_unix BIGINT,
	schema_version VARCHAR,
	extraction_version VARCHAR,
	transaction_count INTEGER,
	operation_count INTEGER,
	bronze_row_count INTEGER,
	payload_json VARCHAR
);
`

const createBronzeRowsSQL = `
CREATE TABLE IF NOT EXISTS bronze_rows (
	network_passphrase VARCHAR,
	ledger_sequence UBIGINT,
	ledger_range UBIGINT,
	row_ordinal INTEGER,
	bronze_row_id VARCHAR,
	table_name VARCHAR,
	row_json VARCHAR
);
`
