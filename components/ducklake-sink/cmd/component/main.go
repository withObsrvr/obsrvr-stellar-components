package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	"github.com/withObsrvr/flowctl-sdk/pkg/consumer"
	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	"google.golang.org/protobuf/proto"
)

// The typed bronze machinery lives in pkg/bronze, shared with
// quack-ducklake-server's ingest path. These aliases keep this package's
// call sites and tests unchanged.
type (
	typedTableSpec      = bronze.TypedTableSpec
	typedRowEnrichments = bronze.TypedRowEnrichments
	duckLakeMigration   = bronze.Migration
)

var (
	duckLakeMigrations        = bronze.Migrations
	typedTableSpecs           = bronze.TypedTableSpecs
	validateTypedTableSpecs   = bronze.ValidateTypedTableSpecs
	buildTypedRowEnrichments  = bronze.BuildTypedRowEnrichments
	typedRowInsertValues      = bronze.TypedRowInsertValues
	multiRowInsertSQL         = bronze.MultiRowInsertSQL
	insertTypedBronzeRow      = bronze.InsertTypedBronzeRow
	deleteTypedRows           = bronze.DeleteTypedRows
	ensureCatalogNetworkTx    = bronze.EnsureCatalogNetworkTx
	ensureCatalogNetworkSQL   = bronze.EnsureCatalogNetworkSQL
	sqlLiteral                = bronze.SQLLiteral
	quoteIdentifier           = bronze.QuoteIdentifier
	splitSQLStatements        = bronze.SplitSQLStatements
	qualifyMigrationSQL       = bronze.QualifyMigrationSQL
	recordMigrationTx         = bronze.RecordMigrationTx
	ensureMigrationRecordedTx = bronze.EnsureMigrationRecordedTx
)

const (
	createCatalogMetadataSQL  = bronze.CreateCatalogMetadataSQL
	createIngestWatermarksSQL = bronze.CreateIngestWatermarksSQL
	createSchemaMigrationsSQL = bronze.CreateSchemaMigrationsSQL
	createLedgerBatchesSQL    = bronze.CreateLedgerBatchesSQL
	createBronzeRowsSQL       = bronze.CreateBronzeRowsSQL
)

var writeRetryBackoff = 500 * time.Millisecond

func main() {
	registry := prometheus.NewRegistry()
	sink, err := NewDuckLakeSink(DuckLakeConfigFromEnv())
	if err != nil {
		panic(err)
	}
	defer sink.Close()
	sink.metrics = newSinkMetrics(registry)
	if err := startSinkHealthServer(getenv("HEALTH_PORT", "8089"), sink, registry); err != nil {
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

type ingestRetryObserver interface {
	recordIngestRetry()
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
			if observer, ok := sink.(ingestRetryObserver); ok {
				observer.recordIngestRetry()
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
	Mode              string
	CatalogPath       string
	DataPath          string
	AttachName        string
	QuackURI          string
	QuackToken        string
	QuackRemoteDB     string
	QuackDisableSSL   bool
	RemoteTimeout     time.Duration
	StagingPath       string
	StagingRemotePath string
	IngestEndpoint    string
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
		// Where the sink writes staged Parquet, and the same location as the
		// quack server sees it (they differ when the two processes mount the
		// shared directory at different paths).
		StagingPath:       getenv("DUCKLAKE_STAGING_PATH", "ducklake/staging"),
		StagingRemotePath: getenv("DUCKLAKE_STAGING_REMOTE_PATH", ""),
		IngestEndpoint:    getenv("INGEST_ENDPOINT", "127.0.0.1:9495"),
	}
}

type DuckLakeSink struct {
	db                *sql.DB
	attachName        string
	remoteDB          string
	remoteCatalog     string
	remoteMode        bool
	quackURI          string
	quackToken        string
	quackNoSSL        bool
	remoteTimeout     time.Duration
	stagingPath       string
	stagingRemotePath string
	ingestMode        bool
	ingestEndpoint    string
	ingestToken       string
	ingestConn        *grpc.ClientConn
	ingestStream      componentsv1.BronzeIngestService_IngestLedgerBatchesClient
	metrics           *sinkMetrics
	mu                sync.Mutex
	healthMu          sync.RWMutex
	lastWriteAt       time.Time
	lastWriteErr      string
	lastLedger        uint32
}

func NewDuckLakeSink(cfg DuckLakeConfig) (*DuckLakeSink, error) {
	if cfg.Mode == "" {
		cfg.Mode = "embedded"
	}
	if cfg.AttachName == "" {
		return nil, fmt.Errorf("DUCKLAKE_ATTACH_NAME is required")
	}
	if cfg.Mode == "ingest-rpc" {
		return newIngestRPCSink(cfg)
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
	if cfg.StagingPath == "" {
		return nil, fmt.Errorf("DUCKLAKE_STAGING_PATH is required when DUCKLAKE_MODE=quack")
	}
	if cfg.StagingRemotePath == "" {
		cfg.StagingRemotePath = cfg.StagingPath
	}
	if err := os.MkdirAll(cfg.StagingPath, 0o755); err != nil {
		return nil, fmt.Errorf("create staging directory: %w", err)
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
		db:                db,
		attachName:        sanitizeIdentifier(cfg.AttachName),
		remoteDB:          sanitizeIdentifier(cfg.QuackRemoteDB),
		remoteCatalog:     sanitizeIdentifier(cfg.AttachName),
		remoteMode:        true,
		quackURI:          cfg.QuackURI,
		quackToken:        cfg.QuackToken,
		quackNoSSL:        cfg.QuackDisableSSL,
		remoteTimeout:     cfg.RemoteTimeout,
		stagingPath:       cfg.StagingPath,
		stagingRemotePath: cfg.StagingRemotePath,
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
	if err := s.initLocalStagingSchema(); err != nil {
		return err
	}
	s.sweepStaleStagingDirs()
	if err := s.execRemoteScript(s.remoteInitSQL()); err != nil {
		return fmt.Errorf("quack remote schema init: %w", err)
	}
	return nil
}

// initLocalStagingSchema creates the typed bronze tables in the sink's local
// in-memory DuckDB. Staged batches are inserted here with the same
// parameterized path as embedded mode, copied out as Parquet, and rolled back,
// so the local tables always match the remote schema exactly.
func (s *DuckLakeSink) initLocalStagingSchema() error {
	if _, err := s.db.Exec("CREATE SCHEMA IF NOT EXISTS bronze"); err != nil {
		return fmt.Errorf("create local staging schema: %w", err)
	}
	for _, migration := range duckLakeMigrations {
		for _, stmt := range splitSQLStatements(migration.SQL) {
			if _, err := s.db.Exec(stmt); err != nil {
				return fmt.Errorf("local staging migration %03d %q: %w", migration.Version, stmt, err)
			}
		}
	}
	return nil
}

// sweepStaleStagingDirs removes staged batch directories left behind by
// crashed or failed runs. Anything older than an hour cannot belong to an
// in-flight write.
func (s *DuckLakeSink) sweepStaleStagingDirs() {
	entries, err := os.ReadDir(s.stagingPath)
	if err != nil {
		log.Printf("staging sweep skipped: %v", err)
		return
	}
	cutoff := time.Now().Add(-time.Hour)
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || !info.ModTime().Before(cutoff) {
			continue
		}
		stale := filepath.Join(s.stagingPath, entry.Name())
		if err := os.RemoveAll(stale); err != nil {
			log.Printf("staging sweep could not remove %s: %v", stale, err)
			continue
		}
		log.Printf("staging sweep removed stale %s", stale)
	}
}

func (s *DuckLakeSink) remoteInitSQL() string {
	stmts := []string{
		fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.bronze", s.remoteCatalog),
		qualifyMigrationSQL(createCatalogMetadataSQL, s.remoteCatalog, ""),
		qualifyMigrationSQL(createIngestWatermarksSQL, s.remoteCatalog, ""),
		qualifyMigrationSQL(createSchemaMigrationsSQL, s.remoteCatalog, ""),
		qualifyMigrationSQL(createLedgerBatchesSQL, s.remoteCatalog, ""),
		qualifyMigrationSQL(createBronzeRowsSQL, s.remoteCatalog, ""),
	}
	for _, migration := range duckLakeMigrations {
		for _, stmt := range splitSQLStatements(migration.SQL) {
			stmts = append(stmts, qualifyMigrationSQL(stmt, s.remoteCatalog, "bronze"))
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
	if s.ingestMode {
		s.closeIngest()
	}
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

	if s.ingestMode {
		return s.writeBatchIngest(batch)
	}
	if s.remoteMode {
		return s.writeBatchRemote(batch)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin DuckLake transaction: %w", err)
	}
	defer tx.Rollback()

	if err := ensureCatalogNetworkTx(tx, batch.NetworkPassphrase); err != nil {
		return err
	}
	if err := bronze.DeleteLedgerRowsTx(tx, batch.NetworkPassphrase, batch.LedgerSequence); err != nil {
		return err
	}
	if err := bronze.InsertLedgerBatchRowTx(tx, batch); err != nil {
		return err
	}
	if err := bronze.InsertWatermarkTx(tx, batch); err != nil {
		return err
	}

	enrichments := buildTypedRowEnrichments(batch)
	for i, row := range batch.BronzeRows {
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

func (s *DuckLakeSink) recordIngestRetry() {
	if s.metrics != nil {
		s.metrics.ingestRetries.Inc()
	}
}

func startSinkHealthServer(port string, sink *DuckLakeSink, gatherer prometheus.Gatherer) error {
	addr := strings.TrimSpace(port)
	if addr == "" {
		return nil
	}
	if !strings.Contains(addr, ":") {
		addr = ":" + addr
	}
	mux := newSinkHTTPHandler(sink, gatherer)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	server := &http.Server{Handler: mux}
	go func() {
		log.Printf("sink health and metrics endpoint listening on %s", addr)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("sink health endpoint failed: %v", err)
		}
	}()
	return nil
}

func newSinkHTTPHandler(sink *DuckLakeSink, gatherer prometheus.Gatherer) http.Handler {
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
	if gatherer != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	}
	return mux
}

// stagedTable is one typed bronze table staged as a Parquet file for a batch.
type stagedTable struct {
	TableName  string
	LocalPath  string
	RemotePath string
}

// stageBatchParquet writes the batch's typed rows into per-table Parquet
// files under a unique staging directory. Rows pass through the same
// parameterized insert path as embedded mode into local staging tables, are
// copied out with explicit column lists, and are then rolled back so the
// local tables stay empty.
func (s *DuckLakeSink) stageBatchParquet(batch *componentsv1.LedgerBatch) (string, []stagedTable, error) {
	batchDir := fmt.Sprintf("%d_%d", batch.LedgerSequence, time.Now().UnixNano())
	localDir := filepath.Join(s.stagingPath, batchDir)
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		return "", nil, fmt.Errorf("create batch staging directory: %w", err)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return "", nil, fmt.Errorf("begin staging transaction: %w", err)
	}
	defer tx.Rollback()

	insertPhase := time.Now()
	decoded := bronze.DecodeTypedRows(batch)
	if err := bronze.InsertDecodedRowsChunkedTx(tx, decoded); err != nil {
		return "", nil, fmt.Errorf("stage ledger %d: %w", batch.LedgerSequence, err)
	}

	insertDuration := time.Since(insertPhase)
	copyPhase := time.Now()

	seen := map[string]bool{}
	var names []string
	for _, dr := range decoded {
		if dr.OK && !seen[dr.Spec.TableName] {
			seen[dr.Spec.TableName] = true
			names = append(names, dr.Spec.TableName)
		}
	}
	sort.Strings(names)

	staged := make([]stagedTable, 0, len(names))
	for _, name := range names {
		spec := typedTableSpecs[name]
		columns := make([]string, len(spec.Columns))
		for i, col := range spec.Columns {
			columns[i] = quoteIdentifier(col)
		}
		fileName := name + ".parquet"
		localPath := filepath.Join(localDir, fileName)
		if _, err := tx.Exec(fmt.Sprintf(
			"COPY (SELECT %s FROM bronze.%s) TO '%s' (FORMAT parquet)",
			strings.Join(columns, ", "),
			name,
			escapeSQLString(localPath),
		)); err != nil {
			return "", nil, fmt.Errorf("stage %s to parquet: %w", name, err)
		}
		staged = append(staged, stagedTable{
			TableName:  name,
			LocalPath:  localPath,
			RemotePath: s.stagingRemotePath + "/" + batchDir + "/" + fileName,
		})
	}
	log.Printf("staging for ledger %d: decode+insert %s, copy %s",
		batch.LedgerSequence,
		insertDuration.Round(time.Millisecond),
		time.Since(copyPhase).Round(time.Millisecond))
	// The rollback in the deferred call discards the local rows; the Parquet
	// files written by COPY are filesystem side effects and survive it.
	return localDir, staged, nil
}

func stagedBytes(staged []stagedTable) int64 {
	var total int64
	for _, st := range staged {
		if info, err := os.Stat(st.LocalPath); err == nil {
			total += info.Size()
		}
	}
	return total
}

func (s *DuckLakeSink) writeBatchRemote(batch *componentsv1.LedgerBatch) error {
	stagingDir, staged, err := s.stageBatchParquet(batch)
	if err != nil {
		return err
	}
	sqlText, err := s.remoteWriteSQL(batch, staged)
	if err != nil {
		return err
	}
	log.Printf("remote DuckLake staged parquet for ledger %d is %.2f MiB across %d files",
		batch.LedgerSequence, float64(stagedBytes(staged))/(1024*1024), len(staged))
	log.Printf("remote DuckLake write script for ledger %d is %.2f KiB", batch.LedgerSequence, float64(len(sqlText))/1024)
	start := time.Now()
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
		log.Printf("staged parquet for failed ledger %d left in %s", batch.LedgerSequence, stagingDir)
		return fmt.Errorf("remote DuckLake write batch ledger %d: %w", batch.LedgerSequence, err)
	}
	log.Printf("remote DuckLake write for ledger %d committed in %s", batch.LedgerSequence, time.Since(start).Round(time.Millisecond))
	if err := os.RemoveAll(stagingDir); err != nil {
		log.Printf("staged parquet cleanup for ledger %d failed: %v", batch.LedgerSequence, err)
	}
	return nil
}

func (s *DuckLakeSink) remoteWriteSQL(batch *componentsv1.LedgerBatch, staged []stagedTable) (string, error) {
	networkPassphrase := strings.TrimSpace(batch.NetworkPassphrase)
	if networkPassphrase == "" {
		return "", fmt.Errorf("ledger batch network_passphrase is required")
	}

	var stmts []string
	stmts = append(stmts, "BEGIN TRANSACTION")
	// bronze_rows and payload_json are no longer persisted, but replays must
	// still clear rows written by older sink versions into existing catalogs.
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
		) VALUES (%s, %d, %d, %s, %s, %d, %d, %d, NULL)`,
		s.remoteCatalog,
		sqlLiteral(networkPassphrase),
		batch.LedgerSequence,
		batch.ClosedAtUnix,
		sqlLiteral(batch.SchemaVersion),
		sqlLiteral(batch.ExtractionVersion),
		len(batch.Transactions),
		len(batch.Operations),
		len(batch.BronzeRows),
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

	for _, st := range staged {
		spec, ok := typedTableSpecs[st.TableName]
		if !ok {
			return "", fmt.Errorf("staged table %s has no typed spec", st.TableName)
		}
		columns := make([]string, len(spec.Columns))
		for i, col := range spec.Columns {
			columns[i] = quoteIdentifier(col)
		}
		columnList := strings.Join(columns, ", ")
		stmts = append(stmts, fmt.Sprintf(
			"INSERT INTO %s.bronze.%s (%s) SELECT %s FROM read_parquet('%s')",
			s.remoteCatalog,
			st.TableName,
			columnList,
			columnList,
			escapeSQLString(st.RemotePath),
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
