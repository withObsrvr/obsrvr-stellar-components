package main

import (
	"strings"
	"testing"
	"time"
)

func testConfig() config {
	return config{
		QuackURI:          "quack:127.0.0.1:9494",
		QuackToken:        "secret",
		QuackRemoteDB:     "remote_lake",
		AttachName:        "stellar_lake",
		Interval:          5 * time.Minute,
		SnapshotRetention: 48 * time.Hour,
		MergeFiles:        true,
		RemoteTimeout:     5 * time.Minute,
	}
}

func TestMaintenanceStatementsFullCycle(t *testing.T) {
	joined := strings.Join(maintenanceStatements(testConfig()), "\n")
	for _, want := range []string{
		"CALL ducklake_flush_inlined_data('stellar_lake')",
		"CALL ducklake_merge_adjacent_files('stellar_lake')",
		"CALL ducklake_expire_snapshots('stellar_lake', older_than => now() - INTERVAL '172800 seconds')",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("maintenance statements missing %q in:\n%s", want, joined)
		}
	}
	if !strings.HasPrefix(joined, "CALL ducklake_flush_inlined_data") {
		t.Fatalf("flush must run first so merged files include freshly flushed data:\n%s", joined)
	}
}

func TestMaintenanceStatementsRespectToggles(t *testing.T) {
	cfg := testConfig()
	cfg.MergeFiles = false
	cfg.SnapshotRetention = 0
	stmts := maintenanceStatements(cfg)
	if len(stmts) != 1 {
		t.Fatalf("expected only flush statement, got %v", stmts)
	}
	if strings.Contains(stmts[0], "merge_adjacent_files") || strings.Contains(stmts[0], "expire_snapshots") {
		t.Fatalf("disabled operations still rendered: %v", stmts)
	}
}

func TestNewMaintainerRequiresToken(t *testing.T) {
	cfg := testConfig()
	cfg.QuackToken = ""
	if _, err := newMaintainer(cfg); err == nil {
		t.Fatal("newMaintainer accepted empty QUACK_TOKEN")
	}
}
