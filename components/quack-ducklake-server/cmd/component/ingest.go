package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
)

// ingestTokenMetadataKey carries the shared token on the gRPC stream.
const ingestTokenMetadataKey = "x-ingest-token"

// ingestServer commits ledger batches into the DuckLake catalog in-process,
// one transaction per ledger, on a dedicated connection. Exactly one batch is
// in flight at a time (mu); ordering is the client's responsibility and is
// enforced by the per-ledger ack protocol.
type ingestServer struct {
	componentsv1.UnimplementedBronzeIngestServiceServer
	conn  *sql.Conn
	token string

	mu            sync.Mutex
	highWatermark uint32
	// forceReplay is set after a failed or uncertain commit: rows may exist,
	// so the next attempt must take the delete-then-insert path.
	forceReplay bool
}

func newIngestServer(ctx context.Context, db *sql.DB, attachName, token string) (*ingestServer, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open ingest connection: %w", err)
	}
	// The dedicated session needs its own USE: session state does not carry
	// over from the init connection.
	if _, err := conn.ExecContext(ctx, "USE "+attachName); err != nil {
		conn.Close()
		return nil, fmt.Errorf("point ingest session at %s: %w", attachName, err)
	}
	s := &ingestServer{conn: conn, token: token}
	if err := s.ensureSchema(ctx); err != nil {
		conn.Close()
		return nil, err
	}
	var wm sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT max(ledger_sequence) FROM ingest_watermarks").Scan(&wm); err != nil {
		conn.Close()
		return nil, fmt.Errorf("read ingest high watermark: %w", err)
	}
	if wm.Valid {
		s.highWatermark = uint32(wm.Int64)
	}
	log.Printf("ingest service ready; high watermark %d", s.highWatermark)
	return s, nil
}

// ensureSchema bootstraps the same base tables and ordered migrations the
// sink applies, so an ingest-only deployment works against a fresh catalog.
func (s *ingestServer) ensureSchema(ctx context.Context) error {
	stmts := []string{
		"CREATE SCHEMA IF NOT EXISTS bronze",
		bronze.CreateCatalogMetadataSQL,
		bronze.CreateIngestWatermarksSQL,
		bronze.CreateSchemaMigrationsSQL,
		bronze.CreateLedgerBatchesSQL,
		bronze.CreateBronzeRowsSQL,
	}
	for _, stmt := range stmts {
		if _, err := s.conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ingest schema bootstrap %q: %w", stmt, err)
		}
	}
	// Native staging tables in the memory catalog: chunked inserts land here
	// at native-table speed, then one INSERT..SELECT per table moves them
	// into DuckLake. Direct chunked inserts into DuckLake tables pay
	// per-statement catalog overhead (measured ~2.5s/ledger vs ~0.4s here).
	if _, err := s.conn.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS memory.bronze"); err != nil {
		return fmt.Errorf("ingest staging schema: %w", err)
	}
	for _, migration := range bronze.Migrations {
		for _, stmt := range bronze.SplitSQLStatements(migration.SQL) {
			qualified := bronze.QualifyCreateTableSQL(stmt, "memory", "bronze")
			if _, err := s.conn.ExecContext(ctx, qualified); err != nil {
				return fmt.Errorf("ingest staging table %q: %w", qualified, err)
			}
		}
	}
	for _, migration := range bronze.Migrations {
		tx, err := s.conn.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin ingest migration %03d: %w", migration.Version, err)
		}
		for _, stmt := range bronze.SplitSQLStatements(migration.SQL) {
			if _, err := tx.Exec(stmt); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("ingest migration %03d statement %q: %w", migration.Version, stmt, err)
			}
		}
		if err := bronze.RecordMigrationTx(tx, migration); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := bronze.EnsureMigrationRecordedTx(tx, migration); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit ingest migration %03d: %w", migration.Version, err)
		}
	}
	return nil
}

func (s *ingestServer) authorize(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	tokens := md.Get(ingestTokenMetadataKey)
	if len(tokens) != 1 || tokens[0] != s.token {
		return status.Error(codes.Unauthenticated, "invalid ingest token")
	}
	return nil
}

func (s *ingestServer) IngestLedgerBatches(stream componentsv1.BronzeIngestService_IngestLedgerBatchesServer) error {
	if err := s.authorize(stream.Context()); err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if req.Batch == nil {
			return status.Error(codes.InvalidArgument, "batch is required")
		}
		start := time.Now()
		replayed, err := s.commitBatch(stream.Context(), req.Batch)
		if err != nil {
			return status.Errorf(codes.Internal, "ingest ledger %d: %v", req.Batch.LedgerSequence, err)
		}
		log.Printf("ingest committed ledger %d in %s (replayed=%t)",
			req.Batch.LedgerSequence, time.Since(start).Round(time.Millisecond), replayed)
		if err := stream.Send(&componentsv1.IngestLedgerBatchAck{
			LedgerSequence: req.Batch.LedgerSequence,
			Replayed:       replayed,
		}); err != nil {
			return err
		}
	}
}

func (s *ingestServer) commitBatch(ctx context.Context, batch *componentsv1.LedgerBatch) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	decoded := bronze.DecodeTypedRows(batch)
	specs := map[string]bronze.TypedTableSpec{}
	for _, dr := range decoded {
		if dr.Err != nil {
			return false, fmt.Errorf("decode typed rows: %w", dr.Err)
		}
		if dr.OK {
			specs[dr.Spec.TableName] = dr.Spec
		}
	}

	replay := s.forceReplay || batch.LedgerSequence <= s.highWatermark
	var err error
	for attempt := 1; attempt <= 2; attempt++ {
		if err = s.tryCommit(ctx, batch, decoded, specs, replay); err == nil {
			if batch.LedgerSequence > s.highWatermark {
				s.highWatermark = batch.LedgerSequence
			}
			s.forceReplay = false
			return replay, nil
		}
		// A concurrent maintenance flush can abort the transaction; after any
		// failure the commit state is uncertain, so the retry (and the next
		// batch) must take the replay path.
		log.Printf("ingest ledger %d attempt %d failed: %v", batch.LedgerSequence, attempt, err)
		replay = true
	}
	s.forceReplay = true
	return replay, err
}

func (s *ingestServer) tryCommit(ctx context.Context, batch *componentsv1.LedgerBatch, decoded []bronze.DecodedRow, specs map[string]bronze.TypedTableSpec, replay bool) error {
	// Phase 1: stage rows into native memory tables via the Appender — rows
	// enter the engine as typed data with no SQL statement cost. A DuckDB
	// transaction may write to only one attached database, so staging is
	// fully materialized before the DuckLake transaction begins.
	stagingStart := time.Now()
	if err := s.clearStaging(ctx, specs); err != nil {
		return err
	}
	if err := s.stageWithAppender(decoded); err != nil {
		return err
	}
	stagingDuration := time.Since(stagingStart)
	lakeStart := time.Now()

	// Phase 2: one DuckLake transaction; row data arrives via engine-native
	// INSERT..SELECT reads from the staging tables.
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin ingest transaction: %w", err)
	}
	defer tx.Rollback()

	if err := bronze.EnsureCatalogNetworkTx(tx, batch.NetworkPassphrase); err != nil {
		return err
	}
	if replay {
		if err := bronze.DeleteLedgerRowsTx(tx, batch.NetworkPassphrase, batch.LedgerSequence); err != nil {
			return err
		}
	}
	if err := bronze.InsertLedgerBatchRowTx(tx, batch); err != nil {
		return err
	}
	if err := bronze.InsertWatermarkTx(tx, batch); err != nil {
		return err
	}
	prefaceDuration := time.Since(lakeStart)
	transferStart := time.Now()
	for tableName, spec := range specs {
		columns := make([]string, len(spec.Columns))
		for i, col := range spec.Columns {
			columns[i] = bronze.QuoteIdentifier(col)
		}
		columnList := strings.Join(columns, ", ")
		if _, err := tx.Exec(fmt.Sprintf(
			"INSERT INTO bronze.%s (%s) SELECT %s FROM memory.bronze.%s",
			tableName, columnList, columnList, tableName,
		)); err != nil {
			return fmt.Errorf("transfer staged rows for %s: %w", tableName, err)
		}
	}
	transferDuration := time.Since(transferStart)
	commitStart := time.Now()
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit ingest transaction: %w", err)
	}
	log.Printf("ingest phases ledger %d: staging %s, preface %s, transfer %s, commit %s",
		batch.LedgerSequence,
		stagingDuration.Round(time.Millisecond),
		prefaceDuration.Round(time.Millisecond),
		transferDuration.Round(time.Millisecond),
		time.Since(commitStart).Round(time.Millisecond))

	// Phase 3: best-effort staging cleanup; the defensive clear at the start
	// of the next batch covers failures here.
	if err := s.clearStaging(ctx, specs); err != nil {
		log.Printf("staging cleanup after ledger %d: %v", batch.LedgerSequence, err)
	}
	return nil
}

// stageWithAppender writes decoded rows into the memory.bronze staging
// tables through the DuckDB Appender, scoped to each spec's column list so
// value order matches exactly.
func (s *ingestServer) stageWithAppender(decoded []bronze.DecodedRow) error {
	grouped := map[string][][]any{}
	groupSpecs := map[string]bronze.TypedTableSpec{}
	var order []string
	for _, dr := range decoded {
		if !dr.OK {
			continue
		}
		if _, seen := grouped[dr.Spec.TableName]; !seen {
			order = append(order, dr.Spec.TableName)
			groupSpecs[dr.Spec.TableName] = dr.Spec
		}
		grouped[dr.Spec.TableName] = append(grouped[dr.Spec.TableName], dr.Values)
	}
	return s.conn.Raw(func(driverConn any) error {
		dc, ok := driverConn.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type %T", driverConn)
		}
		for _, tableName := range order {
			spec := groupSpecs[tableName]
			appender, err := duckdb.NewAppenderWithColumns(dc, "memory", "bronze", tableName, spec.Columns)
			if err != nil {
				return fmt.Errorf("appender for %s: %w", tableName, err)
			}
			for i, values := range grouped[tableName] {
				row := make([]driver.Value, len(values))
				for j, v := range values {
					row[j] = v
				}
				if err := appender.AppendRow(row...); err != nil {
					_ = appender.Close()
					return fmt.Errorf("append row %d to staging %s: %w", i, tableName, err)
				}
			}
			if err := appender.Close(); err != nil {
				return fmt.Errorf("close appender for %s: %w", tableName, err)
			}
		}
		return nil
	})
}

func (s *ingestServer) clearStaging(ctx context.Context, specs map[string]bronze.TypedTableSpec) error {
	for tableName := range specs {
		if _, err := s.conn.ExecContext(ctx, "DELETE FROM memory.bronze."+tableName); err != nil {
			return fmt.Errorf("clear staging table %s: %w", tableName, err)
		}
	}
	return nil
}

// startIngestServer wires the gRPC listener when INGEST_PORT is configured.
// Returns a stop function.
func startIngestServer(ctx context.Context, db *sql.DB, cfg config) (func(), error) {
	if cfg.IngestPort == "" {
		return func() {}, nil
	}
	srv, err := newIngestServer(ctx, db, cfg.AttachName, cfg.Token)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", ":"+cfg.IngestPort)
	if err != nil {
		srv.conn.Close()
		return nil, fmt.Errorf("listen on ingest port %s: %w", cfg.IngestPort, err)
	}
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(64 * 1024 * 1024))
	componentsv1.RegisterBronzeIngestServiceServer(grpcServer, srv)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Printf("ingest gRPC server stopped: %v", err)
		}
	}()
	log.Printf("bronze ingest service listening on :%s", cfg.IngestPort)
	return func() {
		grpcServer.GracefulStop()
		srv.conn.Close()
	}, nil
}
