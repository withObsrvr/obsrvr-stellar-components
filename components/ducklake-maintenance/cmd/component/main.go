package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// ducklake-maintenance periodically runs DuckLake maintenance through the
// Quack server that owns the lake attachment: flushing inlined data to
// Parquet, merging adjacent small files, and expiring old snapshots.
//
// It intentionally does not delete expired files (ducklake_cleanup_old_files
// / ducklake_delete_orphaned_files); reclaiming storage is a separate,
// riskier operation that interacts with time travel and replica checkpoints.

type config struct {
	QuackURI          string
	QuackToken        string
	QuackRemoteDB     string
	QuackDisableSSL   bool
	AttachName        string
	Interval          time.Duration
	SnapshotRetention time.Duration
	MergeFiles        bool
	RunOnce           bool
	RemoteTimeout     time.Duration
	HealthPort        string
}

func configFromEnv() config {
	return config{
		QuackURI:          getenv("QUACK_URI", "quack:127.0.0.1:9494"),
		QuackToken:        getenv("QUACK_TOKEN", ""),
		QuackRemoteDB:     sanitizeIdentifier(getenv("QUACK_REMOTE_DB", "remote_lake")),
		QuackDisableSSL:   getenvBool("QUACK_DISABLE_SSL", false),
		AttachName:        sanitizeIdentifier(getenv("DUCKLAKE_ATTACH_NAME", "stellar_lake")),
		Interval:          getenvDuration("MAINTENANCE_INTERVAL", 5*time.Minute),
		SnapshotRetention: getenvDuration("SNAPSHOT_RETENTION", 48*time.Hour),
		MergeFiles:        getenvBool("MERGE_ADJACENT_FILES", true),
		RunOnce:           getenvBool("RUN_ONCE", false),
		RemoteTimeout:     getenvDuration("MAINTENANCE_REMOTE_TIMEOUT", 5*time.Minute),
		HealthPort:        getenv("HEALTH_PORT", "8090"),
	}
}

func maintenanceStatements(cfg config) []string {
	stmts := []string{
		fmt.Sprintf("CALL ducklake_flush_inlined_data('%s')", cfg.AttachName),
	}
	if cfg.MergeFiles {
		stmts = append(stmts, fmt.Sprintf("CALL ducklake_merge_adjacent_files('%s')", cfg.AttachName))
	}
	if cfg.SnapshotRetention > 0 {
		stmts = append(stmts, fmt.Sprintf(
			"CALL ducklake_expire_snapshots('%s', older_than => now() - INTERVAL '%d seconds')",
			cfg.AttachName,
			int64(cfg.SnapshotRetention.Seconds()),
		))
	}
	return stmts
}

type maintainer struct {
	db       *sql.DB
	cfg      config
	healthMu sync.RWMutex
	lastRun  time.Time
	lastErr  string
	cycles   uint64
}

func newMaintainer(cfg config) (*maintainer, error) {
	if cfg.QuackToken == "" {
		return nil, fmt.Errorf("QUACK_TOKEN is required")
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open DuckDB Quack client: %w", err)
	}
	// Pin to one connection so the quack extension load and attachment are
	// visible to every maintenance statement.
	db.SetMaxOpenConns(1)
	m := &maintainer{db: db, cfg: cfg}
	for _, stmt := range []string{
		"INSTALL quack",
		"LOAD quack",
		fmt.Sprintf(
			"ATTACH '%s' AS %s (TOKEN '%s', DISABLE_SSL %t)",
			escapeSQLString(cfg.QuackURI),
			cfg.QuackRemoteDB,
			escapeSQLString(cfg.QuackToken),
			cfg.QuackDisableSSL,
		),
	} {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("quack init %q: %w", stmt, err)
		}
	}
	return m, nil
}

func (m *maintainer) runCycle(ctx context.Context) error {
	cycleCtx, cancel := context.WithTimeout(ctx, m.cfg.RemoteTimeout)
	defer cancel()
	var firstErr error
	for _, stmt := range maintenanceStatements(m.cfg) {
		rows, err := m.executeRemote(cycleCtx, stmt)
		if err != nil {
			// Later statements still run: a failing flush must not stop
			// snapshot expiry from bounding catalog growth, and vice versa.
			log.Printf("maintenance statement failed: %s: %v", stmt, err)
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", stmt, err)
			}
			continue
		}
		log.Printf("maintenance ok (%d result rows): %s", rows, stmt)
	}
	m.recordHealth(firstErr)
	return firstErr
}

func (m *maintainer) executeRemote(ctx context.Context, stmt string) (int, error) {
	query := fmt.Sprintf("SELECT * FROM %s.query(?)", m.cfg.QuackRemoteDB)
	rows, err := m.db.QueryContext(ctx, query, stmt)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	return count, rows.Err()
}

func (m *maintainer) recordHealth(err error) {
	m.healthMu.Lock()
	defer m.healthMu.Unlock()
	m.lastRun = time.Now().UTC()
	m.cycles++
	if err != nil {
		m.lastErr = err.Error()
	} else {
		m.lastErr = ""
	}
}

func (m *maintainer) healthHandler(w http.ResponseWriter, _ *http.Request) {
	m.healthMu.RLock()
	defer m.healthMu.RUnlock()
	status := http.StatusOK
	if m.lastErr != "" {
		status = http.StatusServiceUnavailable
	}
	w.WriteHeader(status)
	last := "never"
	age := ""
	if !m.lastRun.IsZero() {
		last = m.lastRun.Format(time.RFC3339)
		age = fmt.Sprintf("\nlast_run_age_seconds=%d", int64(time.Since(m.lastRun).Seconds()))
	}
	fmt.Fprintf(w, "healthy=%t\nlast_run=%s%s\ncycles=%d\nlast_error=%s\n",
		m.lastErr == "", last, age, m.cycles, m.lastErr)
}

func run(ctx context.Context, cfg config) error {
	m, err := newMaintainer(cfg)
	if err != nil {
		return err
	}
	defer m.db.Close()

	if cfg.HealthPort != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", m.healthHandler)
		server := &http.Server{Addr: ":" + strings.TrimPrefix(cfg.HealthPort, ":"), Handler: mux}
		go func() {
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("health server error: %v", err)
			}
		}()
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdownCtx)
		}()
	}

	if err := m.runCycle(ctx); err != nil && cfg.RunOnce {
		return err
	}
	if cfg.RunOnce {
		return nil
	}

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// Errors are recorded in /healthz and retried next tick; a
			// transient Quack outage must not kill the maintenance loop.
			_ = m.runCycle(ctx)
		}
	}
}

func main() {
	cfg := configFromEnv()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		log.Fatalf("ducklake-maintenance: %v", err)
	}
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
	switch strings.ToLower(raw) {
	case "1", "t", "true", "y", "yes", "on":
		return true
	case "0", "f", "false", "n", "no", "off":
		return false
	}
	log.Fatalf("invalid boolean for %s: %q", key, raw)
	return fallback
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		log.Fatalf("invalid duration for %s: %q", key, raw)
	}
	return parsed
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
