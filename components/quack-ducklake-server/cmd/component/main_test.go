package main

import (
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
	cfg.Insecure = true

	if err := validateConfig(cfg); err != nil {
		t.Fatalf("validateConfig: %v", err)
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
	}
}
