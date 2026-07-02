package main

import (
	"strings"
	"testing"
	"time"
)

func TestPostgresConfigFromEnvRequiresDSN(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "")

	_, err := postgresConfigFromEnv()
	if err == nil {
		t.Fatal("postgresConfigFromEnv succeeded without POSTGRES_DSN")
	}
	if !strings.Contains(err.Error(), "POSTGRES_DSN is required") {
		t.Fatalf("postgresConfigFromEnv error = %q", err)
	}
}

func TestPostgresConfigFromEnvParsesPoolSettings(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://example")
	t.Setenv("POSTGRES_MAX_OPEN_CONNS", "3")
	t.Setenv("POSTGRES_MAX_IDLE_CONNS", "2")
	t.Setenv("POSTGRES_CONN_MAX_LIFETIME", "45s")

	cfg, err := postgresConfigFromEnv()
	if err != nil {
		t.Fatalf("postgresConfigFromEnv: %v", err)
	}
	if cfg.MaxOpenConns != 3 || cfg.MaxIdleConns != 2 || cfg.ConnMaxLifetime != 45*time.Second {
		t.Fatalf("config = %+v, want parsed pool settings", cfg)
	}
}

func TestUpsertSQLUsesNaturalKeysAndUpdatesAllNonKeyColumns(t *testing.T) {
	for name, tt := range map[string]struct {
		sql   string
		wants []string
	}{
		"ledger": {
			sql: upsertLedgerSQL,
			wants: []string{
				"on conflict (network_passphrase, ledger_sequence)",
				"id = excluded.id",
				"closed_at_unix = excluded.closed_at_unix",
				"ledger_hash = excluded.ledger_hash",
				"previous_ledger_hash = excluded.previous_ledger_hash",
				"protocol_version = excluded.protocol_version",
				"transaction_count = excluded.transaction_count",
				"schema_version = excluded.schema_version",
				"extraction_version = excluded.extraction_version",
			},
		},
		"transaction": {
			sql: upsertTransactionSQL,
			wants: []string{
				"on conflict (network_passphrase, ledger_sequence, transaction_index)",
				"id = excluded.id",
				"transaction_hash = excluded.transaction_hash",
				"successful = excluded.successful",
				"envelope_xdr = excluded.envelope_xdr",
				"result_xdr = excluded.result_xdr",
				"meta_xdr = excluded.meta_xdr",
			},
		},
		"operation": {
			sql: upsertOperationSQL,
			wants: []string{
				"on conflict (network_passphrase, ledger_sequence, transaction_index, operation_index)",
				"id = excluded.id",
				"transaction_id = excluded.transaction_id",
				"operation_type = excluded.operation_type",
				"operation_xdr = excluded.operation_xdr",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			for _, want := range tt.wants {
				if !strings.Contains(tt.sql, want) {
					t.Fatalf("SQL missing %q:\n%s", want, tt.sql)
				}
			}
			if strings.Contains(tt.sql, "on conflict (id)") {
				t.Fatalf("SQL still conflicts on id:\n%s", tt.sql)
			}
		})
	}
}

func TestBronzeReplaySQLDeletesThenInsertsByLedgerTable(t *testing.T) {
	for _, want := range []string{
		"delete from stellar_bronze_rows",
		"network_passphrase = $1",
		"ledger_sequence = $2",
		"table_name = $3",
	} {
		if !strings.Contains(deleteBronzeRowsSQL, want) {
			t.Fatalf("delete bronze SQL missing %q:\n%s", want, deleteBronzeRowsSQL)
		}
	}
	if strings.Contains(insertBronzeRowSQL, "on conflict") {
		t.Fatalf("bronze insert should rely on delete-then-insert, not conflict update:\n%s", insertBronzeRowSQL)
	}
}

func TestSchemaUsesNaturalPrimaryKeys(t *testing.T) {
	for _, want := range []string{
		"primary key (network_passphrase, ledger_sequence)",
		"primary key (network_passphrase, ledger_sequence, transaction_index)",
		"primary key (network_passphrase, ledger_sequence, transaction_index, operation_index)",
	} {
		if !strings.Contains(schemaSQL, want) {
			t.Fatalf("schema SQL missing %q:\n%s", want, schemaSQL)
		}
	}
	for _, blocked := range []string{
		"create table if not exists stellar_ledgers (\n  id text primary key",
		"create table if not exists stellar_transactions (\n  id text primary key",
		"create table if not exists stellar_operations (\n  id text primary key",
	} {
		if strings.Contains(schemaSQL, blocked) {
			t.Fatalf("schema still makes canonical id the primary key:\n%s", schemaSQL)
		}
	}
}

func TestCleanJSONBStripsNULBytes(t *testing.T) {
	if got := cleanJSONB("{\"bad\":\"a\x00b\"}"); strings.Contains(got, "\x00") {
		t.Fatalf("cleanJSONB left NUL byte in %q", got)
	}
}
