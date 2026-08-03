package main

import (
	"context"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestValidateConfigRequiresExplicitInsecureForPlaintext(t *testing.T) {
	cfg := validTestConfig()
	cfg.DisableSSL = true
	cfg.Insecure = false

	err := validateConfig(cfg)
	if err == nil {
		t.Fatalf("validateConfig succeeded with plaintext and no explicit insecure opt-out")
	}
	if !strings.Contains(err.Error(), "QUACK_INSECURE=true") {
		t.Fatalf("validateConfig error = %q, want QUACK_INSECURE guidance", err)
	}
}

func TestValidateConfigAllowsExplicitInsecureOptOut(t *testing.T) {
	cfg := validTestConfig()
	cfg.DisableSSL = true
	cfg.AllowOtherHostname = true
	cfg.EnableExternal = true
	cfg.DisabledFilesystems = "none"
	cfg.Insecure = true

	if err := validateConfig(cfg); err != nil {
		t.Fatalf("validateConfig: %v", err)
	}
}

func TestValidateConfigRequiresExplicitInsecureForExternalAccess(t *testing.T) {
	cfg := validTestConfig()
	cfg.EnableExternal = true

	err := validateConfig(cfg)
	if err == nil {
		t.Fatalf("validateConfig succeeded with external access and no explicit insecure opt-out")
	}
	if !strings.Contains(err.Error(), "QUACK_INSECURE=true") {
		t.Fatalf("validateConfig error = %q, want QUACK_INSECURE guidance", err)
	}
}

func TestValidateConfigRequiresExplicitInsecureForDisabledFilesystemNone(t *testing.T) {
	cfg := validTestConfig()
	cfg.DisabledFilesystems = "none"

	err := validateConfig(cfg)
	if err == nil {
		t.Fatalf("validateConfig succeeded with disabled_filesystems=none and no explicit insecure opt-out")
	}
	if !strings.Contains(err.Error(), "QUACK_INSECURE=true") {
		t.Fatalf("validateConfig error = %q, want QUACK_INSECURE guidance", err)
	}
}

func TestLockdownStatements(t *testing.T) {
	cfg := validTestConfig()
	cfg.MemoryLimit = "2GB"
	cfg.Threads = 3

	stmts := strings.Join(lockdownStatements(cfg), "\n")
	for _, want := range []string{
		"SET enable_external_access=false",
		"SET disabled_filesystems='LocalFileSystem'",
		"SET memory_limit='2GB'",
		"SET threads=3",
	} {
		if !strings.Contains(stmts, want) {
			t.Fatalf("lockdown statements missing %q in:\n%s", want, stmts)
		}
	}
	if strings.Contains(stmts, "SET lock_configuration=true") {
		t.Fatalf("lockdown statements should not lock before quack_serve starts:\n%s", stmts)
	}
}

func TestLockdownStatementsAllowExplicitLocalCatalogRelaxation(t *testing.T) {
	cfg := validTestConfig()
	cfg.EnableExternal = true
	cfg.DisabledFilesystems = "none"

	stmts := strings.Join(lockdownStatements(cfg), "\n")
	for _, blocked := range []string{
		"SET enable_external_access=false",
		"SET disabled_filesystems=",
	} {
		if strings.Contains(stmts, blocked) {
			t.Fatalf("lockdown statements contained %q after explicit relaxation:\n%s", blocked, stmts)
		}
	}
	for _, want := range []string{
		"SET memory_limit='4GB'",
		"SET threads=4",
	} {
		if !strings.Contains(stmts, want) {
			t.Fatalf("lockdown statements missing %q in:\n%s", want, stmts)
		}
	}
}

func TestInitStatementsAttachCatalogBeforeLockdown(t *testing.T) {
	cfg := validTestConfig()
	stmts := initStatements(cfg, "/tmp/duckdb-home")
	joined := strings.Join(stmts, "\n")

	attachIndex := strings.Index(joined, "ATTACH 'ducklake:/tmp/catalog.ducklake' AS stellar_lake")
	if attachIndex < 0 {
		t.Fatalf("init statements missing attach:\n%s", joined)
	}
	if strings.Contains(joined, "SET lock_configuration=true") {
		t.Fatalf("init statements locked configuration before quack_serve:\n%s", joined)
	}
}

func TestConfigDerivesHiddenDuckLakeMetadataAttachName(t *testing.T) {
	t.Setenv("DUCKLAKE_ATTACH_NAME", "stellar-lake")
	t.Setenv("DUCKLAKE_METADATA_ATTACH_NAME", "")
	cfg := configFromEnv()
	if cfg.AttachName != "stellar_lake" {
		t.Fatalf("attach name = %q", cfg.AttachName)
	}
	if cfg.MetadataAttachName != "__ducklake_metadata_stellar_lake" {
		t.Fatalf("metadata attach name = %q", cfg.MetadataAttachName)
	}
}

func TestInitStatementsApplyCheckpointThreshold(t *testing.T) {
	cfg := validTestConfig()
	cfg.CheckpointThreshold = "1GB"
	joined := strings.Join(initStatements(cfg, "/tmp/duckdb-home"), "\n")
	if !strings.Contains(joined, "SET checkpoint_threshold='1GB'") {
		t.Fatalf("init statements did not apply checkpoint threshold:\n%s", joined)
	}
}

func TestLockConfigurationStatement(t *testing.T) {
	if got := lockConfigurationStatement(); got != "SET lock_configuration=true" {
		t.Fatalf("lockConfigurationStatement = %q", got)
	}
}

func TestServeSQLUsesSecureDefaults(t *testing.T) {
	cfg := validTestConfig()
	sql := serveSQL(cfg)
	if strings.Contains(sql, "disable_ssl=true") {
		t.Fatalf("serve SQL disabled SSL by default: %s", sql)
	}
	if strings.Contains(sql, "allow_other_hostname=true") {
		t.Fatalf("serve SQL allowed other hostnames by default: %s", sql)
	}
}

func TestHealthQueryChecksAttachedCatalog(t *testing.T) {
	query := healthQuery("stellar-lake")
	if !strings.Contains(query, "information_schema.schemata") || !strings.Contains(query, "catalog_name = 'stellar_lake'") {
		t.Fatalf("health query = %q, want sanitized attached catalog schema check", query)
	}
}

func TestQuackTCPAddress(t *testing.T) {
	if got := quackTCPAddress("quack:127.0.0.1:9494"); got != "127.0.0.1:9494" {
		t.Fatalf("quackTCPAddress returned %q", got)
	}
}

func TestStartHealthServerReturnsBindError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	server, err := startHealthServer(context.Background(), listener.Addr().String(), nil, "stellar_lake", "", nil, nil, "")
	if err == nil {
		if server != nil {
			_ = server.Close()
		}
		t.Fatalf("startHealthServer succeeded on an occupied address")
	}
	if !strings.Contains(err.Error(), "listen") {
		t.Fatalf("startHealthServer error = %q, want bind/listen failure", err)
	}
}

func TestMetricsUseIsolatedRegistryAndExposeFileGauges(t *testing.T) {
	catalogPath := filepath.Join(t.TempDir(), "catalog.ducklake")
	if err := os.WriteFile(catalogPath, []byte("catalog-bytes"), 0o600); err != nil {
		t.Fatalf("write catalog fixture: %v", err)
	}
	if err := os.WriteFile(catalogPath+".wal", []byte("wal"), 0o600); err != nil {
		t.Fatalf("write WAL fixture: %v", err)
	}

	registry := prometheus.NewRegistry()
	metrics := newServerMetrics(registry, catalogPath)
	srv := &ingestServer{metrics: metrics}
	srv.observeAcknowledged(123, false, ingestPhaseDurations{
		decode:   10 * time.Millisecond,
		staging:  20 * time.Millisecond,
		preface:  30 * time.Millisecond,
		transfer: 40 * time.Millisecond,
		commit:   50 * time.Millisecond,
		cleanup:  5 * time.Millisecond,
	}, 450*time.Millisecond)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/metrics", nil)
	newServerHTTPHandler(nil, "stellar_lake", "", registry, nil, "").ServeHTTP(recorder, request)
	if recorder.Code != 200 {
		t.Fatalf("metrics status = %d, want 200", recorder.Code)
	}
	body := recorder.Body.String()
	for _, want := range []string{
		`obsrvr_ducklake_ingest_phase_seconds_count{phase="decode"} 1`,
		`obsrvr_ducklake_ingest_phase_seconds_count{phase="total"} 1`,
		`obsrvr_ducklake_ingest_batches_total{replayed="false",result="success"} 1`,
		`obsrvr_ducklake_ingest_over_budget_total 1`,
		`obsrvr_ducklake_ingest_last_ledger 123`,
		`obsrvr_ducklake_checkpoint_duration_seconds_count{result="success",trigger="idle"} 0`,
		`obsrvr_ducklake_checkpoint_total{result="error",trigger="manual"} 0`,
		`obsrvr_ducklake_checkpoint_deferred_total{reason="ingest_active"} 0`,
		`obsrvr_ducklake_checkpoint_last_success_timestamp_seconds 0`,
		`obsrvr_ducklake_catalog_file_bytes 13`,
		`obsrvr_ducklake_catalog_wal_bytes 3`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}

	// A second complete collector set can be registered independently. Using
	// the package-global registry would panic here and leak collectors across tests.
	newServerMetrics(prometheus.NewRegistry(), catalogPath)
}

func validTestConfig() config {
	return config{
		CatalogPath:          "/tmp/catalog.ducklake",
		DataPath:             "/tmp/data",
		AttachName:           "stellar_lake",
		MetadataAttachName:   "__ducklake_metadata_stellar_lake",
		URI:                  "quack:127.0.0.1:9494",
		Token:                "secret",
		HealthAddr:           ":8088",
		AllowOtherHostname:   false,
		DisableSSL:           false,
		Insecure:             false,
		EnableExternal:       false,
		DisabledFilesystems:  "LocalFileSystem",
		LockConfiguration:    true,
		MemoryLimit:          "4GB",
		Threads:              4,
		CheckpointThreshold:  "",
		CheckpointEnabled:    false,
		CheckpointTimeout:    30 * time.Second,
		CheckpointAdminToken: "",
		InlineRowLimit:       1024,
	}
}

func TestInitStatementsApplyInlineRowLimit(t *testing.T) {
	cfg := validTestConfig()
	joined := strings.Join(initStatements(cfg, "/tmp/duckdb-home"), "\n")
	want := "CALL stellar_lake.set_option('data_inlining_row_limit', '1024')"
	if !strings.Contains(joined, want) {
		t.Fatalf("init statements missing %q in:\n%s", want, joined)
	}
	attachIndex := strings.Index(joined, "ATTACH ")
	if optionIndex := strings.Index(joined, want); optionIndex < attachIndex {
		t.Fatalf("set_option must run after ATTACH:\n%s", joined)
	}

	cfg.InlineRowLimit = -1
	joined = strings.Join(initStatements(cfg, "/tmp/duckdb-home"), "\n")
	if strings.Contains(joined, "data_inlining_row_limit") {
		t.Fatalf("negative limit should leave catalog config untouched:\n%s", joined)
	}
}
