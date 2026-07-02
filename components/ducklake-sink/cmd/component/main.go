package main

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	"github.com/withObsrvr/flowctl-sdk/pkg/consumer"
	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	extract "github.com/withObsrvr/stellar-extract"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

//go:embed bronze_schema.sql
var bronzeSchemaSQL string

var writeRetryBackoff = 500 * time.Millisecond

type duckLakeMigration struct {
	Version int
	Name    string
	SQL     string
}

var duckLakeMigrations = []duckLakeMigration{
	{Version: 1, Name: "bronze_schema", SQL: bronzeSchemaSQL},
}

func main() {
	sink, err := NewDuckLakeSink(DuckLakeConfigFromEnv())
	if err != nil {
		panic(err)
	}
	defer sink.Close()
	if err := startSinkHealthServer(getenv("HEALTH_PORT", "8089"), sink); err != nil {
		log.Fatalf("start sink health endpoint: %v", err)
	}

	writeGate := make(chan struct{}, 1)
	consumer.Run(consumer.ConsumerConfig{
		ConsumerName: "Stellar Ledger DuckLake Sink",
		ComponentID:  getenv("COMPONENT_ID", "ducklake-sink"),
		InputTypes:   []string{contracts.LedgerBatchEventType},
		OnEvent: func(ctx context.Context, event *flowctlv1.Event) error {
			writeGate <- struct{}{}
			defer func() { <-writeGate }()

			if err := handleLedgerBatchEvent(ctx, event, sink); err != nil {
				log.Printf("fatal ledger batch handling error: %v", err)
				os.Exit(1)
			}
			return nil
		},
	})
}

type ledgerBatchWriter interface {
	WriteBatch(*componentsv1.LedgerBatch) error
}

func handleLedgerBatchEvent(ctx context.Context, event *flowctlv1.Event, sink ledgerBatchWriter) error {
	if event.Type != contracts.LedgerBatchEventType {
		return nil
	}
	var batch componentsv1.LedgerBatch
	if err := proto.Unmarshal(event.Payload, &batch); err != nil {
		return fmt.Errorf("unmarshal ledger batch: %w", err)
	}
	var lastErr error
	backoff := writeRetryBackoff
	for attempt := 1; attempt <= 3; attempt++ {
		if err := sink.WriteBatch(&batch); err != nil {
			lastErr = err
			log.Printf("write ledger batch %d failed on attempt %d/3: %v", batch.LedgerSequence, attempt, err)
			if attempt == 3 {
				break
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("write ledger batch %d canceled after attempt %d: %w", batch.LedgerSequence, attempt, ctx.Err())
			case <-time.After(backoff):
				backoff *= 2
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("write ledger batch %d failed after retries: %w", batch.LedgerSequence, lastErr)
}

type DuckLakeConfig struct {
	Mode            string
	CatalogPath     string
	DataPath        string
	AttachName      string
	QuackURI        string
	QuackToken      string
	QuackRemoteDB   string
	QuackDisableSSL bool
	RemoteTimeout   time.Duration
}

func DuckLakeConfigFromEnv() DuckLakeConfig {
	return DuckLakeConfig{
		Mode:            strings.ToLower(getenv("DUCKLAKE_MODE", "embedded")),
		CatalogPath:     getenv("DUCKLAKE_CATALOG_PATH", "ducklake/stellar.ducklake"),
		DataPath:        getenv("DUCKLAKE_DATA_PATH", "ducklake/data"),
		AttachName:      getenv("DUCKLAKE_ATTACH_NAME", "stellar_lake"),
		QuackURI:        getenv("QUACK_URI", "quack:127.0.0.1:9494"),
		QuackToken:      getenv("QUACK_TOKEN", ""),
		QuackRemoteDB:   getenv("QUACK_REMOTE_DB", "remote_lake"),
		QuackDisableSSL: getenvBool("QUACK_DISABLE_SSL", false),
		RemoteTimeout:   getenvDuration("DUCKLAKE_REMOTE_TIMEOUT", 30*time.Second),
	}
}

type DuckLakeSink struct {
	db            *sql.DB
	attachName    string
	remoteDB      string
	remoteCatalog string
	remoteMode    bool
	quackURI      string
	quackToken    string
	quackNoSSL    bool
	remoteTimeout time.Duration
	mu            sync.Mutex
	healthMu      sync.RWMutex
	lastWriteAt   time.Time
	lastWriteErr  string
	lastLedger    uint32
}

func NewDuckLakeSink(cfg DuckLakeConfig) (*DuckLakeSink, error) {
	if cfg.Mode == "" {
		cfg.Mode = "embedded"
	}
	if cfg.AttachName == "" {
		return nil, fmt.Errorf("DUCKLAKE_ATTACH_NAME is required")
	}
	if cfg.Mode == "quack" {
		return newQuackDuckLakeSink(cfg)
	}
	if cfg.Mode != "embedded" {
		return nil, fmt.Errorf("unsupported DUCKLAKE_MODE %q", cfg.Mode)
	}
	if cfg.CatalogPath == "" {
		return nil, fmt.Errorf("DUCKLAKE_CATALOG_PATH is required")
	}
	if cfg.DataPath == "" {
		return nil, fmt.Errorf("DUCKLAKE_DATA_PATH is required")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.CatalogPath), 0o755); err != nil && filepath.Dir(cfg.CatalogPath) != "." {
		return nil, fmt.Errorf("create DuckLake catalog directory: %w", err)
	}
	if err := os.MkdirAll(cfg.DataPath, 0o755); err != nil {
		return nil, fmt.Errorf("create DuckLake data directory: %w", err)
	}
	if err := validateTypedTableSpecs(); err != nil {
		return nil, err
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open embedded DuckDB: %w", err)
	}
	// Pin the pool to one connection so the loaded extensions/attachment and
	// every transaction (including a failure ROLLBACK) share a single DuckDB
	// session instead of being routed across pooled connections.
	db.SetMaxOpenConns(1)
	sink := &DuckLakeSink{db: db, attachName: sanitizeIdentifier(cfg.AttachName)}
	if err := sink.init(cfg); err != nil {
		db.Close()
		return nil, err
	}
	return sink, nil
}

func newQuackDuckLakeSink(cfg DuckLakeConfig) (*DuckLakeSink, error) {
	if cfg.QuackURI == "" {
		return nil, fmt.Errorf("QUACK_URI is required when DUCKLAKE_MODE=quack")
	}
	if cfg.QuackToken == "" {
		return nil, fmt.Errorf("QUACK_TOKEN is required when DUCKLAKE_MODE=quack")
	}
	if err := validateTypedTableSpecs(); err != nil {
		return nil, err
	}
	if cfg.RemoteTimeout <= 0 {
		cfg.RemoteTimeout = 30 * time.Second
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open DuckDB Quack client: %w", err)
	}
	// Pin the pool to one connection so the remote write script and its failure
	// ROLLBACK land on the same DuckDB/Quack session. This process is long-lived,
	// so a pooled connection left holding an aborted transaction could otherwise
	// be reused for a later batch.
	db.SetMaxOpenConns(1)
	sink := &DuckLakeSink{
		db:            db,
		attachName:    sanitizeIdentifier(cfg.AttachName),
		remoteDB:      sanitizeIdentifier(cfg.QuackRemoteDB),
		remoteCatalog: sanitizeIdentifier(cfg.AttachName),
		remoteMode:    true,
		quackURI:      cfg.QuackURI,
		quackToken:    cfg.QuackToken,
		quackNoSSL:    cfg.QuackDisableSSL,
		remoteTimeout: cfg.RemoteTimeout,
	}
	if err := sink.initQuack(cfg); err != nil {
		db.Close()
		return nil, err
	}
	return sink, nil
}

func (s *DuckLakeSink) init(cfg DuckLakeConfig) error {
	duckDBHome := filepath.Join(cfg.DataPath, ".duckdb")
	if err := os.MkdirAll(duckDBHome, 0o755); err != nil {
		return fmt.Errorf("create DuckDB home directory: %w", err)
	}
	stmts := []string{
		fmt.Sprintf("SET home_directory='%s'", escapeSQLString(duckDBHome)),
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
	if err := s.initBronzeSchema(); err != nil {
		return err
	}
	return nil
}

func (s *DuckLakeSink) initQuack(cfg DuckLakeConfig) error {
	stmts := []string{
		"INSTALL quack",
		"LOAD quack",
		fmt.Sprintf(
			"ATTACH '%s' AS %s (TOKEN '%s', DISABLE_SSL %t)",
			escapeSQLString(s.quackURI),
			s.remoteDB,
			escapeSQLString(s.quackToken),
			s.quackNoSSL,
		),
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("quack init %q: %w", stmt, err)
		}
	}
	if err := s.execRemoteScript(s.remoteInitSQL()); err != nil {
		return fmt.Errorf("quack remote schema init: %w", err)
	}
	return nil
}

func (s *DuckLakeSink) remoteInitSQL() string {
	stmts := []string{
		fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.bronze", s.remoteCatalog),
		qualifyCreateTableSQL(createCatalogMetadataSQL, s.remoteCatalog, ""),
		qualifyCreateTableSQL(createIngestWatermarksSQL, s.remoteCatalog, ""),
		qualifyCreateTableSQL(createSchemaMigrationsSQL, s.remoteCatalog, ""),
		qualifyCreateTableSQL(createLedgerBatchesSQL, s.remoteCatalog, ""),
		qualifyCreateTableSQL(createBronzeRowsSQL, s.remoteCatalog, ""),
	}
	for _, migration := range duckLakeMigrations {
		for _, stmt := range splitSQLStatements(migration.SQL) {
			stmts = append(stmts, qualifyCreateTableSQL(stmt, s.remoteCatalog, "bronze"))
		}
		stmts = append(stmts, fmt.Sprintf(
			`INSERT INTO %s.schema_migrations (version, name, applied_at)
SELECT %d, %s, current_timestamp
WHERE NOT EXISTS (SELECT 1 FROM %s.schema_migrations WHERE version = %d)`,
			s.remoteCatalog,
			migration.Version,
			sqlLiteral(migration.Name),
			s.remoteCatalog,
			migration.Version,
		))
	}
	return strings.Join(stmts, ";\n") + ";"
}

func (s *DuckLakeSink) initBronzeSchema() error {
	stmts := []string{
		"CREATE SCHEMA IF NOT EXISTS bronze",
		createCatalogMetadataSQL,
		createIngestWatermarksSQL,
		createSchemaMigrationsSQL,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("ducklake bronze schema %q: %w", stmt, err)
		}
	}
	return s.applyMigrations()
}

func (s *DuckLakeSink) applyMigrations() error {
	for _, migration := range duckLakeMigrations {
		applied, err := s.migrationApplied(migration)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		tx, err := s.db.Begin()
		if err != nil {
			return fmt.Errorf("begin DuckLake migration %03d: %w", migration.Version, err)
		}
		for _, stmt := range splitSQLStatements(migration.SQL) {
			if _, err := tx.Exec(stmt); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("apply DuckLake migration %03d %s statement %q: %w", migration.Version, migration.Name, stmt, err)
			}
		}
		if err := recordMigrationTx(tx, migration); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := ensureMigrationRecordedTx(tx, migration); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit DuckLake migration %03d %s: %w", migration.Version, migration.Name, err)
		}
	}
	return nil
}

func recordMigrationTx(tx *sql.Tx, migration duckLakeMigration) error {
	if _, err := tx.Exec(
		`INSERT INTO schema_migrations (version, name, applied_at)
SELECT ?, ?, current_timestamp
WHERE NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = ?)`,
		migration.Version,
		migration.Name,
		migration.Version,
	); err != nil {
		return fmt.Errorf("record DuckLake migration %03d %s: %w", migration.Version, migration.Name, err)
	}
	return nil
}

func ensureMigrationRecordedTx(tx *sql.Tx, migration duckLakeMigration) error {
	var count int
	if err := tx.QueryRow("SELECT count(*) FROM schema_migrations WHERE version = ?", migration.Version).Scan(&count); err != nil {
		return fmt.Errorf("verify DuckLake migration %03d %s: %w", migration.Version, migration.Name, err)
	}
	if count == 0 {
		return fmt.Errorf("DuckLake migration %03d %s was not recorded", migration.Version, migration.Name)
	}
	if count > 1 {
		return fmt.Errorf("DuckLake migration %03d %s has duplicate records", migration.Version, migration.Name)
	}
	return nil
}

func (s *DuckLakeSink) migrationApplied(migration duckLakeMigration) (bool, error) {
	var count int
	if err := s.db.QueryRow("SELECT count(*) FROM schema_migrations WHERE version = ?", migration.Version).Scan(&count); err != nil {
		return false, fmt.Errorf("read DuckLake migration %03d %s: %w", migration.Version, migration.Name, err)
	}
	if count > 1 {
		return false, fmt.Errorf("DuckLake migration %03d %s has duplicate records", migration.Version, migration.Name)
	}
	return count == 1, nil
}

func (s *DuckLakeSink) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *DuckLakeSink) WriteBatch(batch *componentsv1.LedgerBatch) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer func() {
		if batch != nil {
			s.recordWriteHealth(batch.LedgerSequence, err)
		}
	}()

	if s.remoteMode {
		return s.writeBatchRemote(batch)
	}

	payloadJSON, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(batch)
	if err != nil {
		return err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin DuckLake transaction: %w", err)
	}
	defer tx.Rollback()

	if err := ensureCatalogNetworkTx(tx, batch.NetworkPassphrase); err != nil {
		return err
	}
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
		"DELETE FROM ingest_watermarks WHERE network_passphrase = ? AND ledger_sequence = ?",
		batch.NetworkPassphrase,
		batch.LedgerSequence,
	); err != nil {
		return fmt.Errorf("delete existing ingest watermark: %w", err)
	}
	if err := deleteTypedRows(tx, batch.LedgerSequence); err != nil {
		return err
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
	if _, err := tx.Exec(
		`INSERT INTO ingest_watermarks (
			network_passphrase,
			ledger_sequence,
			written_at
		) VALUES (?, ?, current_timestamp)`,
		batch.NetworkPassphrase,
		batch.LedgerSequence,
	); err != nil {
		return fmt.Errorf("insert ingest watermark: %w", err)
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

	enrichments := buildTypedRowEnrichments(batch)
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
		if err := insertTypedBronzeRow(tx, row, enrichments); err != nil {
			return fmt.Errorf("insert typed bronze row %d table %s: %w", i, row.TableName, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit DuckLake transaction: %w", err)
	}
	return nil
}

func (s *DuckLakeSink) recordWriteHealth(ledger uint32, err error) {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	s.lastWriteAt = time.Now().UTC()
	s.lastLedger = ledger
	if err != nil {
		s.lastWriteErr = err.Error()
		return
	}
	s.lastWriteErr = ""
}

func (s *DuckLakeSink) healthSnapshot(now time.Time) sinkHealthSnapshot {
	s.healthMu.RLock()
	defer s.healthMu.RUnlock()
	snapshot := sinkHealthSnapshot{
		Healthy:    s.lastWriteErr == "",
		LastLedger: s.lastLedger,
		LastError:  s.lastWriteErr,
	}
	if !s.lastWriteAt.IsZero() {
		snapshot.LastWriteAt = s.lastWriteAt
		snapshot.LastWriteAge = now.Sub(s.lastWriteAt)
	}
	return snapshot
}

type sinkHealthSnapshot struct {
	Healthy      bool
	LastWriteAt  time.Time
	LastWriteAge time.Duration
	LastLedger   uint32
	LastError    string
}

func startSinkHealthServer(port string, sink *DuckLakeSink) error {
	addr := strings.TrimSpace(port)
	if addr == "" {
		return nil
	}
	if !strings.Contains(addr, ":") {
		addr = ":" + addr
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		snapshot := sink.healthSnapshot(time.Now().UTC())
		status := http.StatusOK
		if !snapshot.Healthy {
			status = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(status)
		if snapshot.LastWriteAt.IsZero() {
			_, _ = w.Write([]byte("healthy=true\nlast_write=never\n"))
			return
		}
		_, _ = fmt.Fprintf(
			w,
			"healthy=%t\nlast_ledger=%d\nlast_write_at=%s\nlast_write_age_seconds=%.0f\nlast_error=%s\n",
			snapshot.Healthy,
			snapshot.LastLedger,
			snapshot.LastWriteAt.Format(time.RFC3339),
			snapshot.LastWriteAge.Seconds(),
			snapshot.LastError,
		)
	})
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	server := &http.Server{Handler: mux}
	go func() {
		log.Printf("sink health endpoint listening on %s", addr)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("sink health endpoint failed: %v", err)
		}
	}()
	return nil
}

func (s *DuckLakeSink) writeBatchRemote(batch *componentsv1.LedgerBatch) error {
	sqlText, err := s.remoteWriteSQL(batch)
	if err != nil {
		return err
	}
	log.Printf("remote DuckLake write script for ledger %d is %.2f MiB", batch.LedgerSequence, float64(len(sqlText))/(1024*1024))
	if err := s.execRemoteScript(sqlText); err != nil {
		// Best-effort ROLLBACK on the same pinned connection to clear any
		// lingering transaction before this connection is reused for the next
		// batch. Bound it so a degraded Quack link cannot stall the sink, and
		// log a failed rollback rather than swallowing an unclean state.
		rbCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if rbErr := s.execRemoteScriptContext(rbCtx, "ROLLBACK;"); rbErr != nil {
			log.Printf("remote DuckLake write batch ledger %d: rollback did not confirm clean state: %v", batch.LedgerSequence, rbErr)
		}
		return fmt.Errorf("remote DuckLake write batch ledger %d: %w", batch.LedgerSequence, err)
	}
	return nil
}

func (s *DuckLakeSink) remoteWriteSQL(batch *componentsv1.LedgerBatch) (string, error) {
	networkPassphrase := strings.TrimSpace(batch.NetworkPassphrase)
	if networkPassphrase == "" {
		return "", fmt.Errorf("ledger batch network_passphrase is required")
	}
	payloadJSON, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(batch)
	if err != nil {
		return "", err
	}

	var stmts []string
	stmts = append(stmts, "BEGIN TRANSACTION")
	stmts = append(stmts,
		ensureCatalogNetworkSQL(s.remoteCatalog, networkPassphrase),
		fmt.Sprintf(
			"DELETE FROM %s.bronze_rows WHERE network_passphrase = %s AND ledger_sequence = %d",
			s.remoteCatalog,
			sqlLiteral(networkPassphrase),
			batch.LedgerSequence,
		),
		fmt.Sprintf(
			"DELETE FROM %s.ledger_batches WHERE network_passphrase = %s AND ledger_sequence = %d",
			s.remoteCatalog,
			sqlLiteral(networkPassphrase),
			batch.LedgerSequence,
		),
		fmt.Sprintf(
			"DELETE FROM %s.ingest_watermarks WHERE network_passphrase = %s AND ledger_sequence = %d",
			s.remoteCatalog,
			sqlLiteral(networkPassphrase),
			batch.LedgerSequence,
		),
	)
	for _, spec := range typedTableSpecs {
		if spec.LedgerColumn == "" {
			continue
		}
		stmts = append(stmts, fmt.Sprintf(
			"DELETE FROM %s.bronze.%s WHERE %s = %d",
			s.remoteCatalog,
			spec.TableName,
			quoteIdentifier(spec.LedgerColumn),
			batch.LedgerSequence,
		))
	}
	stmts = append(stmts, fmt.Sprintf(
		`INSERT INTO %s.ledger_batches (
			network_passphrase,
			ledger_sequence,
			closed_at_unix,
			schema_version,
			extraction_version,
			transaction_count,
			operation_count,
			bronze_row_count,
			payload_json
		) VALUES (%s, %d, %d, %s, %s, %d, %d, %d, %s)`,
		s.remoteCatalog,
		sqlLiteral(networkPassphrase),
		batch.LedgerSequence,
		batch.ClosedAtUnix,
		sqlLiteral(batch.SchemaVersion),
		sqlLiteral(batch.ExtractionVersion),
		len(batch.Transactions),
		len(batch.Operations),
		len(batch.BronzeRows),
		sqlLiteral(string(payloadJSON)),
	))
	stmts = append(stmts, fmt.Sprintf(
		`INSERT INTO %s.ingest_watermarks (
			network_passphrase,
			ledger_sequence,
			written_at
		) VALUES (%s, %d, current_timestamp)`,
		s.remoteCatalog,
		sqlLiteral(networkPassphrase),
		batch.LedgerSequence,
	))

	if len(batch.BronzeRows) > 0 {
		values := make([]string, 0, len(batch.BronzeRows))
		for i, row := range batch.BronzeRows {
			values = append(values, fmt.Sprintf("(%s, %d, %d, %d, %s, %s, %s)",
				sqlLiteral(row.NetworkPassphrase),
				row.LedgerSequence,
				row.LedgerRange,
				i,
				sqlLiteral(row.Id),
				sqlLiteral(row.TableName),
				sqlLiteral(row.RowJson),
			))
		}
		stmts = append(stmts, fmt.Sprintf(
			`INSERT INTO %s.bronze_rows (
				network_passphrase,
				ledger_sequence,
				ledger_range,
				row_ordinal,
				bronze_row_id,
				table_name,
				row_json
			) VALUES %s`,
			s.remoteCatalog,
			strings.Join(values, ", "),
		))
	}

	enrichments := buildTypedRowEnrichments(batch)
	typedRows := map[string][]string{}
	for _, row := range batch.BronzeRows {
		valuesSQL, err := typedBronzeValuesSQL(row, enrichments)
		if err != nil {
			return "", fmt.Errorf("prepare typed bronze row table %s: %w", row.TableName, err)
		}
		if valuesSQL != "" {
			typedRows[row.TableName] = append(typedRows[row.TableName], valuesSQL)
		}
	}
	for tableName, rows := range typedRows {
		spec := typedTableSpecs[tableName]
		columns := make([]string, len(spec.Columns))
		for i, col := range spec.Columns {
			columns[i] = quoteIdentifier(col)
		}
		stmts = append(stmts, fmt.Sprintf(
			"INSERT INTO %s.bronze.%s (%s) VALUES %s",
			s.remoteCatalog,
			tableName,
			strings.Join(columns, ", "),
			strings.Join(rows, ", "),
		))
	}
	stmts = append(stmts, "COMMIT")
	return strings.Join(stmts, ";\n") + ";", nil
}

func (s *DuckLakeSink) execRemoteScript(sqlText string) error {
	timeout := s.remoteTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.execRemoteScriptContext(ctx, sqlText)
}

func (s *DuckLakeSink) execRemoteScriptContext(ctx context.Context, sqlText string) error {
	query := fmt.Sprintf("SELECT * FROM %s.query(?)", s.remoteDB)
	if _, err := s.db.ExecContext(ctx, query, sqlText); err != nil {
		if reinitErr := s.reinitRemoteSession(context.Background()); reinitErr != nil {
			log.Printf("remote DuckLake session re-init failed after query error: %v", reinitErr)
		}
		return err
	}
	return nil
}

func (s *DuckLakeSink) reinitRemoteSession(ctx context.Context) error {
	if !s.remoteMode {
		return nil
	}
	timeout := s.remoteTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("DETACH %s", s.remoteDB)); err != nil {
		log.Printf("remote DuckLake detach during re-init did not complete cleanly: %v", err)
	}
	attach := fmt.Sprintf(
		"ATTACH '%s' AS %s (TOKEN '%s', DISABLE_SSL %t)",
		escapeSQLString(s.quackURI),
		s.remoteDB,
		escapeSQLString(s.quackToken),
		s.quackNoSSL,
	)
	if _, err := s.db.ExecContext(ctx, attach); err != nil {
		return fmt.Errorf("remote DuckLake attach: %w", err)
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

func getenvBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	// These flags gate security-relevant transport behavior (e.g. DISABLE_SSL),
	// so an unrecognized value is a misconfiguration we must not silently coerce
	// to false. Accept the common boolean spellings and fail fast on anything else.
	switch strings.ToLower(raw) {
	case "1", "t", "true", "y", "yes", "on":
		return true
	case "0", "f", "false", "n", "no", "off":
		return false
	default:
		log.Fatalf("%s must be a boolean (true/false/1/0/yes/no), got %q", key, raw)
		return fallback // unreachable; log.Fatalf exits
	}
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err == nil {
		return value
	}
	seconds, scanErr := time.ParseDuration(raw + "s")
	if scanErr != nil {
		log.Fatalf("%s must be a duration like 30s or a number of seconds, got %q", key, raw)
	}
	return seconds
}

const createCatalogMetadataSQL = `
CREATE TABLE IF NOT EXISTS catalog_metadata (
	key VARCHAR NOT NULL,
	value VARCHAR NOT NULL,
	updated_at TIMESTAMP NOT NULL
);
`

const createIngestWatermarksSQL = `
CREATE TABLE IF NOT EXISTS ingest_watermarks (
	network_passphrase VARCHAR,
	ledger_sequence UBIGINT,
	written_at TIMESTAMP
);
`

const createSchemaMigrationsSQL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
	version INTEGER NOT NULL,
	name VARCHAR NOT NULL,
	applied_at TIMESTAMP NOT NULL
);
`

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

type typedTableSpec struct {
	TableName           string
	Columns             []string
	RowType             reflect.Type
	LedgerColumn        string
	ColumnOverrides     map[string]string
	ColumnJSONFallbacks map[string]string
	ColumnDefaults      map[string]any
}

func ensureCatalogNetworkTx(tx *sql.Tx, networkPassphrase string) error {
	networkPassphrase = strings.TrimSpace(networkPassphrase)
	if networkPassphrase == "" {
		return fmt.Errorf("ledger batch network_passphrase is required")
	}
	existingCount, existingMin, existingMax, err := readCatalogNetworkMetadataTx(tx)
	if err != nil {
		return fmt.Errorf("read catalog network passphrase: %w", err)
	}
	if existingCount == 0 {
		if _, err := tx.Exec(
			"INSERT INTO catalog_metadata (key, value, updated_at) VALUES ('network_passphrase', ?, current_timestamp)",
			networkPassphrase,
		); err != nil {
			return fmt.Errorf("record catalog network passphrase: %w", err)
		}
		existingCount, existingMin, existingMax, err = readCatalogNetworkMetadataTx(tx)
		if err != nil {
			return fmt.Errorf("read catalog network passphrase after insert: %w", err)
		}
	}
	if existingCount > 1 {
		return fmt.Errorf("catalog network metadata has duplicate network_passphrase keys")
	}
	if !existingMin.Valid || !existingMax.Valid || existingMin.String != existingMax.String {
		return fmt.Errorf("catalog network metadata is invalid for network_passphrase")
	}
	if existingMin.String != networkPassphrase {
		return fmt.Errorf("catalog network mismatch: existing %q, batch %q", existingMin.String, networkPassphrase)
	}
	if _, err := tx.Exec(
		"UPDATE catalog_metadata SET updated_at = current_timestamp WHERE key = 'network_passphrase'",
	); err != nil {
		return fmt.Errorf("refresh catalog network metadata: %w", err)
	}
	return nil
}

func readCatalogNetworkMetadataTx(tx *sql.Tx) (int, sql.NullString, sql.NullString, error) {
	var count int
	var minValue, maxValue sql.NullString
	err := tx.QueryRow(
		"SELECT count(*), min(value), max(value) FROM catalog_metadata WHERE key = 'network_passphrase'",
	).Scan(&count, &minValue, &maxValue)
	return count, minValue, maxValue, err
}

func ensureCatalogNetworkSQL(catalog, networkPassphrase string) string {
	return fmt.Sprintf(`INSERT INTO %s.catalog_metadata (key, value, updated_at)
SELECT 'network_passphrase', %s, current_timestamp
WHERE NOT EXISTS (
	SELECT 1 FROM %s.catalog_metadata WHERE key = 'network_passphrase'
);
SELECT CASE
	WHEN (SELECT count(*) FROM %s.catalog_metadata WHERE key = 'network_passphrase') > 1
		THEN error('catalog network metadata duplicate key')
	WHEN EXISTS (
		SELECT 1 FROM %s.catalog_metadata
		WHERE key = 'network_passphrase' AND value <> %s
	) THEN error('catalog network mismatch')
	ELSE 1
END;
UPDATE %s.catalog_metadata
SET updated_at = current_timestamp
WHERE key = 'network_passphrase'`,
		catalog,
		sqlLiteral(strings.TrimSpace(networkPassphrase)),
		catalog,
		catalog,
		catalog,
		sqlLiteral(strings.TrimSpace(networkPassphrase)),
		catalog,
	)
}

func deleteTypedRows(tx *sql.Tx, ledgerSequence uint32) error {
	for _, spec := range typedTableSpecs {
		if spec.LedgerColumn == "" {
			continue
		}
		if _, err := tx.Exec(
			fmt.Sprintf("DELETE FROM bronze.%s WHERE %s = ?", spec.TableName, quoteIdentifier(spec.LedgerColumn)),
			ledgerSequence,
		); err != nil {
			return fmt.Errorf("delete typed rows from %s: %w", spec.TableName, err)
		}
	}
	return nil
}

type typedRowEnrichment map[string]any
type typedRowEnrichments map[string]typedRowEnrichment

func buildTypedRowEnrichments(batch *componentsv1.LedgerBatch) typedRowEnrichments {
	enrichments := typedRowEnrichments{}
	if batch == nil {
		return enrichments
	}
	for _, tx := range batch.Transactions {
		key := transactionEnrichmentKey(tx.LedgerSequence, tx.TransactionHash)
		enrichments[key] = typedRowEnrichment{
			"tx_envelope": tx.EnvelopeXdr,
			"tx_result":   tx.ResultXdr,
			"tx_meta":     tx.MetaXdr,
		}
	}
	return enrichments
}

func transactionEnrichmentKey(ledgerSequence uint32, transactionHash string) string {
	return fmt.Sprintf("transactions_row_v2:%d:%s", ledgerSequence, transactionHash)
}

func insertTypedBronzeRow(tx *sql.Tx, row *componentsv1.BronzeRow, enrichments typedRowEnrichments) error {
	spec, ok := typedTableSpecs[row.TableName]
	if !ok {
		return nil
	}
	value := reflect.New(spec.RowType)
	if err := json.Unmarshal([]byte(row.RowJson), value.Interface()); err != nil {
		return fmt.Errorf("unmarshal typed row: %w", err)
	}

	values, err := typedValues(spec, value.Elem(), row, enrichments)
	if err != nil {
		return err
	}
	placeholders := make([]string, len(spec.Columns))
	columns := make([]string, len(spec.Columns))
	for i, col := range spec.Columns {
		placeholders[i] = "?"
		columns[i] = quoteIdentifier(col)
	}
	query := fmt.Sprintf(
		"INSERT INTO bronze.%s (%s) VALUES (%s)",
		spec.TableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)
	if _, err := tx.Exec(query, values...); err != nil {
		return fmt.Errorf("insert %s: %w", spec.TableName, err)
	}
	return nil
}

func typedValues(spec typedTableSpec, value reflect.Value, bronzeRow *componentsv1.BronzeRow, enrichments typedRowEnrichments) ([]any, error) {
	values := make([]any, 0, len(spec.Columns))
	jsonValues, err := decodeTypedRowJSON(bronzeRow.RowJson)
	if err != nil {
		return nil, err
	}
	enrichment := typedRowEnrichmentFor(spec, bronzeRow, jsonValues, enrichments)
	for _, col := range spec.Columns {
		if defaultValue, ok := spec.ColumnDefaults[col]; ok {
			values = append(values, defaultValue)
			continue
		}
		if enrichedValue, ok := enrichment[col]; ok {
			values = append(values, enrichedValue)
			continue
		}
		fieldName := columnFieldName(spec, col)
		field := value.FieldByName(fieldName)
		if field.IsValid() {
			sqlValue, err := sqlValue(field)
			if err != nil {
				return nil, fmt.Errorf("column %s.%s: %w", spec.TableName, col, err)
			}
			values = append(values, sqlValue)
			continue
		}
		jsonKey, ok := spec.ColumnJSONFallbacks[col]
		if !ok {
			return nil, fmt.Errorf("column %s.%s has no struct field %s or explicit JSON fallback", spec.TableName, col, fieldName)
		}
		value, ok := jsonValues[jsonKey]
		if !ok {
			values = append(values, nil)
			continue
		}
		values = append(values, value)
	}
	return values, nil
}

func typedRowEnrichmentFor(spec typedTableSpec, bronzeRow *componentsv1.BronzeRow, jsonValues map[string]any, enrichments typedRowEnrichments) typedRowEnrichment {
	if spec.TableName != "transactions_row_v2" {
		return nil
	}
	transactionHash, _ := jsonValues["TransactionHash"].(string)
	if transactionHash == "" {
		transactionHash, _ = jsonValues["transaction_hash"].(string)
	}
	return enrichments[transactionEnrichmentKey(bronzeRow.LedgerSequence, transactionHash)]
}

func decodeTypedRowJSON(rowJSON string) (map[string]any, error) {
	raw := map[string]json.RawMessage{}
	if err := json.Unmarshal([]byte(rowJSON), &raw); err != nil {
		return nil, fmt.Errorf("unmarshal typed row JSON map: %w", err)
	}
	values := make(map[string]any, len(raw))
	for key, data := range raw {
		if string(data) == "null" {
			values[key] = nil
			continue
		}
		var text string
		if err := json.Unmarshal(data, &text); err == nil {
			values[key] = text
			continue
		}
		var boolean bool
		if err := json.Unmarshal(data, &boolean); err == nil {
			values[key] = boolean
			continue
		}
		var number json.Number
		if err := json.Unmarshal(data, &number); err == nil {
			if i, intErr := number.Int64(); intErr == nil {
				values[key] = i
				continue
			}
			if f, floatErr := number.Float64(); floatErr == nil {
				values[key] = f
				continue
			}
		}
		values[key] = string(data)
	}
	return values, nil
}

func columnFieldName(spec typedTableSpec, column string) string {
	if override, ok := spec.ColumnOverrides[column]; ok {
		return override
	}
	return snakeToExported(column)
}

func sqlValue(value reflect.Value) (any, error) {
	if !value.IsValid() {
		return nil, nil
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, nil
		}
		return sqlValue(value.Elem())
	}
	if value.Type() == reflect.TypeOf(time.Time{}) {
		return value.Interface(), nil
	}
	if value.Kind() == reflect.Slice || value.Kind() == reflect.Map || value.Kind() == reflect.Struct {
		data, err := json.Marshal(value.Interface())
		if err != nil {
			return nil, err
		}
		return string(data), nil
	}
	return value.Interface(), nil
}

func typedBronzeValuesSQL(row *componentsv1.BronzeRow, enrichments typedRowEnrichments) (string, error) {
	spec, ok := typedTableSpecs[row.TableName]
	if !ok {
		return "", nil
	}
	value := reflect.New(spec.RowType)
	if err := json.Unmarshal([]byte(row.RowJson), value.Interface()); err != nil {
		return "", fmt.Errorf("unmarshal typed row: %w", err)
	}
	values, err := typedValues(spec, value.Elem(), row, enrichments)
	if err != nil {
		return "", err
	}
	literals := make([]string, len(values))
	for i, value := range values {
		literals[i] = sqlLiteral(value)
	}
	return "(" + strings.Join(literals, ", ") + ")", nil
}

func sqlLiteral(value any) string {
	if value == nil {
		return "NULL"
	}
	switch v := value.(type) {
	case string:
		return "'" + escapeSQLString(v) + "'"
	case []byte:
		return "'" + escapeSQLString(string(v)) + "'"
	case bool:
		if v {
			return "true"
		}
		return "false"
	case time.Time:
		return "TIMESTAMP '" + escapeSQLString(v.UTC().Format("2006-01-02 15:04:05.999999")) + "'"
	case fmt.Stringer:
		return "'" + escapeSQLString(v.String()) + "'"
	default:
		return fmt.Sprint(v)
	}
}

func qualifyCreateTableSQL(sqlText, catalog, schema string) string {
	sqlText = strings.TrimSpace(sqlText)
	if schema != "" {
		return strings.Replace(sqlText, "CREATE TABLE IF NOT EXISTS "+schema+".", "CREATE TABLE IF NOT EXISTS "+catalog+"."+schema+".", 1)
	}
	return strings.Replace(sqlText, "CREATE TABLE IF NOT EXISTS ", "CREATE TABLE IF NOT EXISTS "+catalog+".", 1)
}

func snakeToExported(value string) string {
	parts := strings.Split(strings.Trim(value, `"`), "_")
	var b strings.Builder
	for _, part := range parts {
		switch strings.ToLower(part) {
		case "":
			continue
		case "id":
			b.WriteString("ID")
		case "xdr":
			b.WriteString("XDR")
		case "ttl":
			b.WriteString("TTL")
		case "tx":
			b.WriteString("Tx")
		case "json":
			b.WriteString("JSON")
		case "wasm":
			b.WriteString("Wasm")
		default:
			b.WriteString(strings.ToUpper(part[:1]))
			if len(part) > 1 {
				b.WriteString(part[1:])
			}
		}
	}
	return b.String()
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(strings.Trim(value, `"`), `"`, `""`) + `"`
}

func splitSQLStatements(sqlText string) []string {
	sqlText = strings.ReplaceAll(sqlText, "bronze.", "bronze.")
	var statements []string
	for _, stmt := range strings.Split(sqlText, ";") {
		var cleaned []string
		for _, line := range strings.Split(stmt, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "--") {
				continue
			}
			cleaned = append(cleaned, line)
		}
		stmt = strings.TrimSpace(strings.Join(cleaned, "\n"))
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}
	return statements
}

func validateTypedTableSpecs() error {
	var missing []string
	for _, spec := range typedTableSpecs {
		for _, col := range spec.Columns {
			if _, ok := spec.ColumnDefaults[col]; ok {
				continue
			}
			fieldName := columnFieldName(spec, col)
			if reflect.New(spec.RowType).Elem().FieldByName(fieldName).IsValid() {
				continue
			}
			if _, ok := spec.ColumnJSONFallbacks[col]; ok {
				continue
			}
			missing = append(missing, fmt.Sprintf("%s.%s -> %s", spec.TableName, col, fieldName))
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("typed DuckLake column mappings are incomplete: %s", strings.Join(missing, "; "))
	}
	return nil
}

func tableSpec(table string, row any, ledgerColumn string, columns []string, overrides map[string]string, jsonFallbacks ...map[string]string) typedTableSpec {
	var fallback map[string]string
	if len(jsonFallbacks) > 0 {
		fallback = jsonFallbacks[0]
	}
	return typedTableSpec{
		TableName:           table,
		Columns:             columns,
		RowType:             reflect.TypeOf(row),
		LedgerColumn:        ledgerColumn,
		ColumnOverrides:     overrides,
		ColumnJSONFallbacks: fallback,
		ColumnDefaults:      map[string]any{"version_label": contracts.ExtractionVersion},
	}
}

func jsonFallbacks(columns ...string) map[string]string {
	fallbacks := make(map[string]string, len(columns))
	for _, col := range columns {
		fallbacks[col] = snakeToExported(col)
	}
	return fallbacks
}

var typedTableSpecs = map[string]typedTableSpec{
	"ledgers_row_v2": tableSpec("ledgers_row_v2", extract.LedgerRowData{}, "sequence", []string{
		"sequence", "ledger_hash", "previous_ledger_hash", "closed_at", "protocol_version", "total_coins", "fee_pool", "base_fee", "base_reserve", "max_tx_set_size", "successful_tx_count", "failed_tx_count", "ingestion_timestamp", "ledger_range", "transaction_count", "operation_count", "tx_set_operation_count", "soroban_fee_write1kb", "node_id", "signature", "ledger_header", "bucket_list_size", "live_soroban_state_size", "evicted_keys_count", "soroban_op_count", "total_fee_charged", "contract_events_count", "era_id", "version_label",
	}, nil),
	"transactions_row_v2": tableSpec("transactions_row_v2", extract.TransactionData{}, "ledger_sequence", []string{
		"ledger_sequence", "transaction_hash", "source_account", "fee_charged", "max_fee", "successful", "transaction_result_code", "operation_count", "memo_type", "memo", "created_at", "account_sequence", "ledger_range", "source_account_muxed", "fee_account_muxed", "inner_transaction_hash", "fee_bump_fee", "max_fee_bid", "inner_source_account", "timebounds_min_time", "timebounds_max_time", "ledgerbounds_min", "ledgerbounds_max", "min_sequence_number", "min_sequence_age", "soroban_resources_instructions", "soroban_resources_read_bytes", "soroban_resources_write_bytes", "soroban_data_size_bytes", "soroban_data_resources", "soroban_fee_base", "soroban_fee_resources", "soroban_fee_refund", "soroban_fee_charged", "soroban_fee_wasted", "soroban_host_function_type", "soroban_contract_id", "soroban_contract_events_count", "signatures_count", "new_account", "rent_fee_charged", "tx_envelope", "tx_result", "tx_meta", "tx_fee_meta", "tx_signers", "extra_signers", "era_id", "version_label", "transaction_id",
	}, nil, jsonFallbacks(
		"fee_account_muxed",
		"inner_transaction_hash",
		"fee_bump_fee",
		"max_fee_bid",
		"inner_source_account",
		"ledgerbounds_min",
		"ledgerbounds_max",
		"min_sequence_number",
		"min_sequence_age",
		"soroban_data_size_bytes",
		"soroban_data_resources",
		"soroban_fee_base",
		"soroban_fee_resources",
		"soroban_fee_refund",
		"soroban_fee_charged",
		"soroban_fee_wasted",
		"soroban_contract_events_count",
		"tx_envelope",
		"tx_result",
		"tx_meta",
		"tx_fee_meta",
		"tx_signers",
		"extra_signers",
	)),
	"operations_row_v2": tableSpec("operations_row_v2", extract.OperationData{}, "ledger_sequence", []string{
		"transaction_hash", "operation_index", "ledger_sequence", "source_account", "type", "type_string", "created_at", "transaction_successful", "operation_result_code", "operation_trace_code", "ledger_range", "source_account_muxed", "asset", "asset_type", "asset_code", "asset_issuer", "source_asset", "source_asset_type", "source_asset_code", "source_asset_issuer", "amount", "source_amount", "destination_min", "starting_balance", "destination", "trustline_limit", "trustor", "authorize", "authorize_to_maintain_liabilities", "trust_line_flags", "balance_id", "claimants_count", "sponsored_id", "offer_id", "price", "price_r", "buying_asset", "buying_asset_type", "buying_asset_code", "buying_asset_issuer", "selling_asset", "selling_asset_type", "selling_asset_code", "selling_asset_issuer", "soroban_operation", "soroban_function", "soroban_contract_id", "soroban_auth_required", "bump_to", "set_flags", "clear_flags", "home_domain", "master_weight", "low_threshold", "medium_threshold", "high_threshold", "data_name", "data_value", "era_id", "version_label", "transaction_index", "soroban_arguments_json", "contract_calls_json", "contracts_involved", "max_call_depth", "transaction_id", "operation_id", "soroban_auth_credentials_types", "soroban_auth_addresses",
	}, map[string]string{"type": "OpType"}, jsonFallbacks(
		"operation_trace_code",
		"trustor",
		"authorize",
		"authorize_to_maintain_liabilities",
		"trust_line_flags",
		"claimants_count",
		"soroban_auth_credentials_types",
		"soroban_auth_addresses",
	)),
	"effects_row_v1": tableSpec("effects_row_v1", extract.EffectData{}, "ledger_sequence", []string{
		"ledger_sequence", "transaction_hash", "operation_index", "effect_index", "effect_type", "effect_type_string", "account_id", "amount", "asset_code", "asset_issuer", "asset_type", "trustline_limit", "authorize_flag", "clawback_flag", "signer_account", "signer_weight", "offer_id", "seller_account", "created_at", "ledger_range", "era_id", "version_label", "details_json", "operation_id",
	}, nil),
	"trades_row_v1": tableSpec("trades_row_v1", extract.TradeData{}, "ledger_sequence", []string{
		"ledger_sequence", "transaction_hash", "operation_index", "trade_index", "trade_type", "trade_timestamp", "seller_account", "selling_asset_code", "selling_asset_issuer", "selling_amount", "buyer_account", "buying_asset_code", "buying_asset_issuer", "buying_amount", "price", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"accounts_snapshot_v1": tableSpec("accounts_snapshot_v1", extract.AccountData{}, "ledger_sequence", []string{
		"account_id", "ledger_sequence", "closed_at", "balance", "sequence_number", "num_subentries", "num_sponsoring", "num_sponsored", "home_domain", "master_weight", "low_threshold", "med_threshold", "high_threshold", "flags", "auth_required", "auth_revocable", "auth_immutable", "auth_clawback_enabled", "signers", "sponsor_account", "created_at", "updated_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"trustlines_snapshot_v1": tableSpec("trustlines_snapshot_v1", extract.TrustlineData{}, "ledger_sequence", []string{
		"account_id", "asset_code", "asset_issuer", "asset_type", "balance", "trust_limit", "buying_liabilities", "selling_liabilities", "authorized", "authorized_to_maintain_liabilities", "clawback_enabled", "ledger_sequence", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"account_signers_snapshot_v1": tableSpec("account_signers_snapshot_v1", extract.AccountSignerData{}, "ledger_sequence", []string{
		"account_id", "signer", "ledger_sequence", "weight", "sponsor", "deleted", "closed_at", "ledger_range", "created_at", "era_id", "version_label",
	}, nil),
	"native_balances_snapshot_v1": tableSpec("native_balances_snapshot_v1", extract.NativeBalanceData{}, "ledger_sequence", []string{
		"account_id", "balance", "buying_liabilities", "selling_liabilities", "num_subentries", "num_sponsoring", "num_sponsored", "sequence_number", "last_modified_ledger", "ledger_sequence", "ledger_range", "era_id", "version_label",
	}, nil),
	"offers_snapshot_v1": tableSpec("offers_snapshot_v1", extract.OfferData{}, "ledger_sequence", []string{
		"offer_id", "seller_account", "ledger_sequence", "closed_at", "selling_asset_type", "selling_asset_code", "selling_asset_issuer", "buying_asset_type", "buying_asset_code", "buying_asset_issuer", "amount", "price", "flags", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"liquidity_pools_snapshot_v1": tableSpec("liquidity_pools_snapshot_v1", extract.LiquidityPoolData{}, "ledger_sequence", []string{
		"liquidity_pool_id", "ledger_sequence", "closed_at", "pool_type", "fee", "trustline_count", "total_pool_shares", "asset_a_type", "asset_a_code", "asset_a_issuer", "asset_a_amount", "asset_b_type", "asset_b_code", "asset_b_issuer", "asset_b_amount", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"claimable_balances_snapshot_v1": tableSpec("claimable_balances_snapshot_v1", extract.ClaimableBalanceData{}, "ledger_sequence", []string{
		"balance_id", "sponsor", "ledger_sequence", "closed_at", "asset_type", "asset_code", "asset_issuer", "amount", "claimants_count", "flags", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"contract_events_stream_v1": tableSpec("contract_events_stream_v1", extract.ContractEventData{}, "ledger_sequence", []string{
		"event_id", "contract_id", "ledger_sequence", "transaction_hash", "closed_at", "event_type", "in_successful_contract_call", "successful", "contract_event_xdr", "topics_json", "topics_decoded", "data_xdr", "data_decoded", "topic_count", "operation_index", "event_index", "topic0_decoded", "topic1_decoded", "topic2_decoded", "topic3_decoded", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"contract_data_snapshot_v1": tableSpec("contract_data_snapshot_v1", extract.ContractDataData{}, "ledger_sequence", []string{
		"contract_id", "ledger_sequence", "ledger_key_hash", "contract_key_type", "contract_durability", "asset_code", "asset_issuer", "asset_type", "balance_holder", "balance", "last_modified_ledger", "ledger_entry_change", "deleted", "closed_at", "contract_data_xdr", "created_at", "ledger_range", "token_name", "token_symbol", "token_decimals", "era_id", "version_label",
	}, map[string]string{"contract_id": "ContractId"}),
	"contract_code_snapshot_v1": tableSpec("contract_code_snapshot_v1", extract.ContractCodeData{}, "ledger_sequence", []string{
		"contract_code_hash", "ledger_key_hash", "contract_code_ext_v", "last_modified_ledger", "ledger_entry_change", "deleted", "closed_at", "ledger_sequence", "n_instructions", "n_functions", "n_globals", "n_table_entries", "n_types", "n_data_segments", "n_elem_segments", "n_imports", "n_exports", "n_data_segment_bytes", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"config_settings_snapshot_v1": tableSpec("config_settings_snapshot_v1", extract.ConfigSettingData{}, "ledger_sequence", []string{
		"config_setting_id", "ledger_sequence", "last_modified_ledger", "deleted", "closed_at", "ledger_max_instructions", "tx_max_instructions", "fee_rate_per_instructions_increment", "tx_memory_limit", "ledger_max_read_ledger_entries", "ledger_max_read_bytes", "ledger_max_write_ledger_entries", "ledger_max_write_bytes", "tx_max_read_ledger_entries", "tx_max_read_bytes", "tx_max_write_ledger_entries", "tx_max_write_bytes", "contract_max_size_bytes", "config_setting_xdr", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"ttl_snapshot_v1": tableSpec("ttl_snapshot_v1", extract.TTLData{}, "ledger_sequence", []string{
		"key_hash", "ledger_sequence", "live_until_ledger_seq", "ttl_remaining", "expired", "last_modified_ledger", "deleted", "closed_at", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"evicted_keys_state_v1": tableSpec("evicted_keys_state_v1", extract.EvictedKeyData{}, "ledger_sequence", []string{
		"key_hash", "ledger_sequence", "contract_id", "key_type", "durability", "closed_at", "ledger_range", "created_at", "era_id", "version_label",
	}, nil),
	"restored_keys_state_v1": tableSpec("restored_keys_state_v1", extract.RestoredKeyData{}, "ledger_sequence", []string{
		"key_hash", "ledger_sequence", "contract_id", "key_type", "durability", "restored_from_ledger", "closed_at", "ledger_range", "created_at", "era_id", "version_label",
	}, nil),
	"contract_creations_v1": tableSpec("contract_creations_v1", extract.ContractCreationData{}, "created_ledger", []string{
		"contract_id", "creator_address", "wasm_hash", "created_ledger", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"token_transfers_stream_v1": tableSpec("token_transfers_stream_v1", extract.TokenTransferData{}, "ledger_sequence", []string{
		"ledger_sequence", "transaction_hash", "transaction_id", "operation_id", "operation_index", "event_type", "from", "to", "asset", "asset_type", "asset_code", "asset_issuer", "amount", "amount_raw", "contract_id", "closed_at", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
}
