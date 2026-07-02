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
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

func main() {
	cfg := configFromEnv()
	if err := run(context.Background(), cfg); err != nil {
		log.Fatal(err)
	}
}

type config struct {
	DBPath              string
	CatalogPath         string
	DataPath            string
	AttachName          string
	URI                 string
	Token               string
	HealthAddr          string
	AllowOtherHostname  bool
	DisableSSL          bool
	Insecure            bool
	EnableExternal      bool
	DisabledFilesystems string
	LockConfiguration   bool
	MemoryLimit         string
	Threads             int
}

func configFromEnv() config {
	insecureMode := getenvBool("QUACK_INSECURE", false)
	return config{
		DBPath:              getenv("QUACK_DUCKDB_PATH", ""),
		CatalogPath:         getenv("DUCKLAKE_CATALOG_PATH", "ducklake/stellar.ducklake"),
		DataPath:            getenv("DUCKLAKE_DATA_PATH", "ducklake/data"),
		AttachName:          sanitizeIdentifier(getenv("DUCKLAKE_ATTACH_NAME", "stellar_lake")),
		URI:                 getenv("QUACK_URI", "quack:127.0.0.1:9494"),
		Token:               getenv("QUACK_TOKEN", ""),
		HealthAddr:          getenv("QUACK_HEALTH_ADDR", ":8088"),
		AllowOtherHostname:  getenvBool("QUACK_ALLOW_OTHER_HOSTNAME", false),
		DisableSSL:          insecureMode || getenvBool("QUACK_DISABLE_SSL", false),
		Insecure:            insecureMode,
		EnableExternal:      getenvBool("QUACK_ENABLE_EXTERNAL_ACCESS", false),
		DisabledFilesystems: getenv("QUACK_DISABLED_FILESYSTEMS", "LocalFileSystem"),
		LockConfiguration:   getenvBool("QUACK_LOCK_CONFIGURATION", true),
		MemoryLimit:         getenv("QUACK_MEMORY_LIMIT", "4GB"),
		Threads:             getenvInt("QUACK_DUCKDB_THREADS", 4),
	}
}

func run(ctx context.Context, cfg config) error {
	if err := validateConfig(cfg); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(cfg.CatalogPath), 0o755); err != nil && filepath.Dir(cfg.CatalogPath) != "." {
		return fmt.Errorf("create DuckLake catalog directory: %w", err)
	}
	if err := os.MkdirAll(cfg.DataPath, 0o755); err != nil {
		return fmt.Errorf("create DuckLake data directory: %w", err)
	}
	duckDBHome := filepath.Join(cfg.DataPath, ".duckdb")
	if err := os.MkdirAll(duckDBHome, 0o755); err != nil {
		return fmt.Errorf("create DuckDB home directory: %w", err)
	}

	db, err := sql.Open("duckdb", cfg.DBPath)
	if err != nil {
		return fmt.Errorf("open DuckDB: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	for _, stmt := range initStatements(cfg, duckDBHome) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("init %q: %w", stmt, err)
		}
	}

	healthServer, err := startHealthServer(sigCtx, cfg.HealthAddr, db, cfg.AttachName, cfg.URI)
	if err != nil {
		return fmt.Errorf("start health server: %w", err)
	}
	if healthServer != nil {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := healthServer.Shutdown(shutdownCtx); err != nil {
				log.Printf("health server shutdown failed: %v", err)
			}
		}()
	}

	log.Printf("serving DuckLake catalog %s as %s on %s", cfg.CatalogPath, cfg.AttachName, cfg.URI)
	serveErr := make(chan error, 1)
	go func() {
		_, err := db.ExecContext(sigCtx, serveSQL(cfg))
		if err == nil && cfg.LockConfiguration {
			_, err = db.ExecContext(sigCtx, lockConfigurationStatement())
		}
		serveErr <- err
	}()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("start quack server: %w", err)
		}
		<-sigCtx.Done()
	case <-sigCtx.Done():
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := db.ExecContext(stopCtx, fmt.Sprintf("CALL quack_stop('%s')", escapeSQLString(cfg.URI))); err != nil {
		log.Printf("quack_stop failed: %v", err)
	}
	return nil
}

func validateConfig(cfg config) error {
	if cfg.Token == "" {
		return fmt.Errorf("QUACK_TOKEN is required")
	}
	if cfg.DisableSSL && !cfg.Insecure {
		return fmt.Errorf("QUACK_DISABLE_SSL=true requires explicit QUACK_INSECURE=true")
	}
	if cfg.AllowOtherHostname && !cfg.Insecure {
		return fmt.Errorf("QUACK_ALLOW_OTHER_HOSTNAME=true requires explicit QUACK_INSECURE=true")
	}
	if cfg.EnableExternal && !cfg.Insecure {
		return fmt.Errorf("QUACK_ENABLE_EXTERNAL_ACCESS=true requires explicit QUACK_INSECURE=true")
	}
	if strings.EqualFold(strings.TrimSpace(cfg.DisabledFilesystems), "none") && !cfg.Insecure {
		return fmt.Errorf("QUACK_DISABLED_FILESYSTEMS=none requires explicit QUACK_INSECURE=true")
	}
	if cfg.MemoryLimit == "" {
		return fmt.Errorf("QUACK_MEMORY_LIMIT is required")
	}
	if cfg.Threads <= 0 {
		return fmt.Errorf("QUACK_DUCKDB_THREADS must be positive")
	}
	return nil
}

func initStatements(cfg config, duckDBHome string) []string {
	stmts := []string{
		fmt.Sprintf("SET home_directory='%s'", escapeSQLString(duckDBHome)),
		"INSTALL ducklake",
		"LOAD ducklake",
		"INSTALL quack",
		"LOAD quack",
		fmt.Sprintf(
			"ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')",
			escapeSQLString(cfg.CatalogPath),
			cfg.AttachName,
			escapeSQLString(cfg.DataPath),
		),
		fmt.Sprintf("USE %s", cfg.AttachName),
	}
	return append(stmts, lockdownStatements(cfg)...)
}

func lockdownStatements(cfg config) []string {
	stmts := []string{}
	if !cfg.EnableExternal {
		stmts = append(stmts, "SET enable_external_access=false")
	}
	if cfg.DisabledFilesystems != "" && !strings.EqualFold(cfg.DisabledFilesystems, "none") {
		stmts = append(stmts, fmt.Sprintf("SET disabled_filesystems='%s'", escapeSQLString(cfg.DisabledFilesystems)))
	}
	stmts = append(stmts,
		fmt.Sprintf("SET memory_limit='%s'", escapeSQLString(cfg.MemoryLimit)),
		fmt.Sprintf("SET threads=%d", cfg.Threads),
	)
	return stmts
}

func lockConfigurationStatement() string {
	return "SET lock_configuration=true"
}

func serveSQL(cfg config) string {
	return fmt.Sprintf(
		"CALL quack_serve('%s', token='%s', allow_other_hostname=%t, disable_ssl=%t)",
		escapeSQLString(cfg.URI),
		escapeSQLString(cfg.Token),
		cfg.AllowOtherHostname,
		cfg.DisableSSL,
	)
}

func startHealthServer(ctx context.Context, addr string, db *sql.DB, attachName, quackURI string) (*http.Server, error) {
	if strings.TrimSpace(addr) == "" {
		return nil, nil
	}
	quackAddr := quackTCPAddress(quackURI)
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		queryCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		var one int
		if err := db.QueryRowContext(queryCtx, healthQuery(attachName)).Scan(&one); err != nil || one != 1 {
			http.Error(w, "unhealthy", http.StatusServiceUnavailable)
			return
		}
		if quackAddr != "" {
			conn, err := net.DialTimeout("tcp", quackAddr, 500*time.Millisecond)
			if err != nil {
				http.Error(w, "unhealthy", http.StatusServiceUnavailable)
				return
			}
			_ = conn.Close()
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}
	server := &http.Server{Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("health server shutdown failed: %v", err)
		}
	}()
	go func() {
		log.Printf("health endpoint listening on %s", addr)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("health endpoint failed: %v", err)
		}
	}()
	return server, nil
}

func healthQuery(attachName string) string {
	return fmt.Sprintf(
		"SELECT 1 FROM information_schema.schemata WHERE catalog_name = '%s' LIMIT 1",
		escapeSQLString(sanitizeIdentifier(attachName)),
	)
}

func quackTCPAddress(uri string) string {
	uri = strings.TrimSpace(uri)
	if strings.HasPrefix(uri, "quack:") {
		return strings.TrimPrefix(uri, "quack:")
	}
	return uri
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		log.Fatalf("%s must be an integer, got %q", key, raw)
	}
	return value
}

func getenvBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	// These flags gate security-relevant transport behavior (e.g. DISABLE_SSL,
	// ALLOW_OTHER_HOSTNAME), so an unrecognized value is a misconfiguration we
	// must not silently coerce to false. Accept the common boolean spellings and
	// fail fast on anything else.
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

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
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
