package main

import (
	"context"
	"net"
	"strings"
	"testing"
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

	server, err := startHealthServer(context.Background(), listener.Addr().String(), nil, "stellar_lake", "")
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

func validTestConfig() config {
	return config{
		CatalogPath:         "/tmp/catalog.ducklake",
		DataPath:            "/tmp/data",
		AttachName:          "stellar_lake",
		URI:                 "quack:127.0.0.1:9494",
		Token:               "secret",
		HealthAddr:          ":8088",
		AllowOtherHostname:  false,
		DisableSSL:          false,
		Insecure:            false,
		EnableExternal:      false,
		DisabledFilesystems: "LocalFileSystem",
		LockConfiguration:   true,
		MemoryLimit:         "4GB",
		Threads:             4,
		InlineRowLimit:      1024,
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
