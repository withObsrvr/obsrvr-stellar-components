package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	_ "github.com/duckdb/duckdb-go/v2"
)

func main() {
	cfg := configFromEnv()
	if err := run(context.Background(), cfg); err != nil {
		log.Fatal(err)
	}
}

type config struct {
	DBPath             string
	CatalogPath        string
	DataPath           string
	AttachName         string
	URI                string
	Token              string
	AllowOtherHostname bool
	DisableSSL         bool
}

func configFromEnv() config {
	return config{
		DBPath:             getenv("QUACK_DUCKDB_PATH", ""),
		CatalogPath:        getenv("DUCKLAKE_CATALOG_PATH", "ducklake/stellar.ducklake"),
		DataPath:           getenv("DUCKLAKE_DATA_PATH", "ducklake/data"),
		AttachName:         sanitizeIdentifier(getenv("DUCKLAKE_ATTACH_NAME", "stellar_lake")),
		URI:                getenv("QUACK_URI", "quack:127.0.0.1:9494"),
		Token:              getenv("QUACK_TOKEN", ""),
		AllowOtherHostname: getenvBool("QUACK_ALLOW_OTHER_HOSTNAME", true),
		DisableSSL:         getenvBool("QUACK_DISABLE_SSL", true),
	}
}

func run(ctx context.Context, cfg config) error {
	if cfg.Token == "" {
		return fmt.Errorf("QUACK_TOKEN is required")
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
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("init %q: %w", stmt, err)
		}
	}

	log.Printf("serving DuckLake catalog %s as %s on %s", cfg.CatalogPath, cfg.AttachName, cfg.URI)
	serveSQL := fmt.Sprintf(
		"CALL quack_serve('%s', token='%s', allow_other_hostname=%t, disable_ssl=%t)",
		escapeSQLString(cfg.URI),
		escapeSQLString(cfg.Token),
		cfg.AllowOtherHostname,
		cfg.DisableSSL,
	)
	if _, err := db.ExecContext(ctx, serveSQL); err != nil {
		return fmt.Errorf("start quack server: %w", err)
	}

	sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-sigCtx.Done()

	if _, err := db.ExecContext(context.Background(), fmt.Sprintf("CALL quack_stop('%s')", escapeSQLString(cfg.URI))); err != nil {
		log.Printf("quack_stop failed: %v", err)
	}
	return nil
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvBool(key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	return value == "1" || value == "true" || value == "yes"
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
