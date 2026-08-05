package gatekeeper

import (
	"path/filepath"
	"strings"
	"testing"
)

const validProposalYAML = `
api_version: gatekeeper.obsrvr.dev/v1alpha1
proposal_id: prism_asset_daily_volume
agent_id: demo_agent
source:
  relation: stellar_lake.bronze.token_transfers_stream_v1
  snapshot_id: 42
ledger_range:
  start_exclusive: 62079999
  end_inclusive: 62080999
target:
  relation: stellar_lake.silver.asset_daily_volume
  replace_keys: [asset, day]
transformation: |
  SELECT asset, CAST(closed_at AS DATE) AS day, sum(amount) AS volume
  FROM {{source}}
  WHERE ledger_sequence > {{start_ledger}} AND ledger_sequence <= {{end_ledger}}
  GROUP BY asset, day
invariants:
  - name: non_negative_volume
    sql: SELECT count(*) = 0 FROM {{candidate}} WHERE volume < 0
`

func TestParseProposalIsStrictAndStable(t *testing.T) {
	proposal, hashA, err := ParseProposal([]byte(validProposalYAML))
	if err != nil {
		t.Fatalf("parse proposal: %v", err)
	}
	if proposal.Source.SnapshotID != 42 || len(hashA) != 64 {
		t.Fatalf("proposal = %+v hash=%q", proposal, hashA)
	}
	_, hashB, err := ParseProposal([]byte("\n" + validProposalYAML))
	if err != nil {
		t.Fatalf("parse whitespace variant: %v", err)
	}
	if hashA != hashB {
		t.Fatalf("canonical hashes differ: %s != %s", hashA, hashB)
	}
	unknown := strings.Replace(validProposalYAML, "agent_id: demo_agent", "agent_id: demo_agent\nunknown_field: true", 1)
	if _, _, err := ParseProposal([]byte(unknown)); err == nil || !strings.Contains(err.Error(), "field unknown_field") {
		t.Fatalf("unknown field error = %v", err)
	}
}

func TestProposalRejectsUnsafeTransformation(t *testing.T) {
	unsafe := strings.Replace(validProposalYAML,
		"SELECT asset, CAST(closed_at AS DATE) AS day, sum(amount) AS volume",
		"SELECT 1; DROP TABLE stellar_lake.bronze.ledger_batches",
		1)
	if _, _, err := ParseProposal([]byte(unsafe)); err == nil || !strings.Contains(err.Error(), "one statement") {
		t.Fatalf("unsafe transformation error = %v", err)
	}
}

func TestProposalRequiresPinnedTemplateAndReplacementKeys(t *testing.T) {
	withoutSource := strings.Replace(validProposalYAML, "FROM {{source}}", "FROM bronze.token_transfers_stream_v1", 1)
	if _, _, err := ParseProposal([]byte(withoutSource)); err == nil || !strings.Contains(err.Error(), "{{source}}") {
		t.Fatalf("missing source placeholder error = %v", err)
	}
	withoutKeys := strings.Replace(validProposalYAML, "replace_keys: [asset, day]", "replace_keys: []", 1)
	if _, _, err := ParseProposal([]byte(withoutKeys)); err == nil || !strings.Contains(err.Error(), "replace_keys") {
		t.Fatalf("missing keys error = %v", err)
	}
}

func TestPublishedAssetDailyVolumeManifestParses(t *testing.T) {
	path := filepath.Join("..", "..", "manifests", "gatekeeper", "asset-daily-volume.yaml")
	proposal, hash, err := LoadProposal(path)
	if err != nil {
		t.Fatalf("load published manifest: %v", err)
	}
	if proposal.Target.Relation != "stellar_lake.silver.asset_daily_volume" || len(hash) != 64 {
		t.Fatalf("published proposal = %+v hash=%q", proposal, hash)
	}
}
