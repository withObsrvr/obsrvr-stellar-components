package gatekeeper

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type renderedProposal struct {
	Catalog          string
	StagingSchema    string
	CandidateA       string
	CandidateB       string
	ReplayA          string
	ReplayB          string
	SourceRelation   string
	TargetRelation   string
	Transformation   string
	ReplayTransformA string
	ReplayTransformB string
}

func renderProposal(p Proposal, proposalHash string, replay LedgerRange) (renderedProposal, error) {
	sourceParts, err := parseRelation("source.relation", p.Source.Relation)
	if err != nil {
		return renderedProposal{}, err
	}
	targetParts, err := parseRelation("target.relation", p.Target.Relation)
	if err != nil {
		return renderedProposal{}, err
	}
	prefix := proposalHash
	if len(prefix) > 16 {
		prefix = prefix[:16]
	}
	stagingSchema := "gatekeeper_" + prefix
	candidateA := quotedRelation(sourceParts[0], stagingSchema, "candidate_a")
	candidateB := quotedRelation(sourceParts[0], stagingSchema, "candidate_b")
	replayA := quotedRelation(sourceParts[0], stagingSchema, "replay_a")
	replayB := quotedRelation(sourceParts[0], stagingSchema, "replay_b")
	// Keep the version clause inside a derived table. DuckLake's grammar places
	// aliases before AT (VERSION ...), whereas proposal authors naturally write
	// "FROM {{source}} AS name". The derived table makes that ordinary aliasing
	// valid while keeping the version pin impossible to omit.
	source := "(SELECT * FROM " + quotedRelation(sourceParts[0], sourceParts[1], sourceParts[2]) +
		" AT (VERSION => " + strconv.FormatUint(p.Source.SnapshotID, 10) + "))"
	return renderedProposal{
		Catalog:          sourceParts[0],
		StagingSchema:    stagingSchema,
		CandidateA:       candidateA,
		CandidateB:       candidateB,
		ReplayA:          replayA,
		ReplayB:          replayB,
		SourceRelation:   source,
		TargetRelation:   quotedRelation(targetParts[0], targetParts[1], targetParts[2]),
		Transformation:   renderTemplate(p.Transformation, source, candidateA, p.LedgerRange),
		ReplayTransformA: renderTemplate(p.Transformation, source, replayA, replay),
		ReplayTransformB: renderTemplate(p.Transformation, source, replayB, replay),
	}, nil
}

func renderTemplate(template, source, candidate string, ledgerRange LedgerRange) string {
	replacer := strings.NewReplacer(
		"{{source}}", source,
		"{{candidate}}", candidate,
		"{{start_ledger}}", strconv.FormatUint(ledgerRange.StartExclusive, 10),
		"{{end_ledger}}", strconv.FormatUint(ledgerRange.EndInclusive, 10),
	)
	return replacer.Replace(template)
}

func resetStagingSQL(r renderedProposal) string {
	return fmt.Sprintf(`
CREATE SCHEMA IF NOT EXISTS %s.%s;
DROP TABLE IF EXISTS %s;
DROP TABLE IF EXISTS %s;
DROP TABLE IF EXISTS %s;
DROP TABLE IF EXISTS %s;
CREATE TABLE %s AS %s;
CREATE TABLE %s AS %s;
`, quoteIdentifier(r.Catalog), quoteIdentifier(r.StagingSchema), r.CandidateA, r.CandidateB, r.ReplayA, r.ReplayB,
		r.CandidateA, r.Transformation, r.CandidateB, r.Transformation)
}

func replaySQL(r renderedProposal) string {
	return fmt.Sprintf(`
CREATE TABLE %s AS %s;
CREATE TABLE %s AS %s;
`, r.ReplayA, r.ReplayTransformA, r.ReplayB, r.ReplayTransformB)
}

func equalitySQL(left, right string) string {
	return fmt.Sprintf(`SELECT count(*) = 0 FROM (
  (SELECT * FROM %s EXCEPT ALL SELECT * FROM %s)
  UNION ALL
  (SELECT * FROM %s EXCEPT ALL SELECT * FROM %s)
) AS differences`, left, right, right, left)
}

func currentSnapshotSQL(catalog string) string {
	return fmt.Sprintf("SELECT id FROM %s.current_snapshot()", quoteIdentifier(catalog))
}

func rowCountSQL(relation string) string {
	return fmt.Sprintf("SELECT count(*) FROM %s", relation)
}

func invariantSQL(invariant Invariant, r renderedProposal, ledgerRange LedgerRange) string {
	return renderTemplate(invariant.SQL, r.SourceRelation, r.CandidateA, ledgerRange)
}

func bootstrapGovernanceSQL(catalog string) string {
	return fmt.Sprintf(`
CREATE SCHEMA IF NOT EXISTS %[1]s.governance;
CREATE TABLE IF NOT EXISTS %[1]s.governance.promotions (
  promotion_id VARCHAR,
  proposal_id VARCHAR,
  proposal_hash VARCHAR,
  agent_id VARCHAR,
  source_snapshot_id UBIGINT,
  start_ledger_exclusive UBIGINT,
  end_ledger_inclusive UBIGINT,
  target_relation VARCHAR,
  promoted_rows UBIGINT,
  gate_results_json VARCHAR,
  promoted_at TIMESTAMP
);
`, quoteIdentifier(catalog))
}

func promotionSQL(p Proposal, r renderedProposal, receipt PromotionReceipt) (string, error) {
	targetParts, err := parseRelation("target.relation", p.Target.Relation)
	if err != nil {
		return "", err
	}
	conditions := make([]string, 0, len(p.Target.ReplaceKeys))
	for _, key := range p.Target.ReplaceKeys {
		quoted := quoteIdentifier(key)
		conditions = append(conditions, fmt.Sprintf("published.%s IS NOT DISTINCT FROM candidate.%s", quoted, quoted))
	}
	gateJSON, err := json.Marshal(receipt.Gates)
	if err != nil {
		return "", fmt.Errorf("encode gate results: %w", err)
	}
	return fmt.Sprintf(`
BEGIN TRANSACTION;
CREATE SCHEMA IF NOT EXISTS %[1]s.%[2]s;
CREATE TABLE IF NOT EXISTS %[3]s AS SELECT * FROM %[4]s WHERE false;
DELETE FROM %[3]s AS published
USING %[4]s AS candidate
WHERE %[5]s;
INSERT INTO %[3]s SELECT * FROM %[4]s;
INSERT INTO %[1]s.governance.promotions VALUES (
  '%[6]s', '%[7]s', '%[8]s', '%[9]s', %[10]d, %[11]d, %[12]d,
  '%[13]s', %[14]d, '%[15]s', TIMESTAMP '%[16]s'
);
COMMIT;
`, quoteIdentifier(targetParts[0]), quoteIdentifier(targetParts[1]), r.TargetRelation, r.CandidateA,
		strings.Join(conditions, "\n  AND "), escapeSQLString(receipt.PromotionID),
		escapeSQLString(p.ProposalID), escapeSQLString(receipt.ProposalHash), escapeSQLString(p.AgentID),
		p.Source.SnapshotID, p.LedgerRange.StartExclusive, p.LedgerRange.EndInclusive,
		escapeSQLString(p.Target.Relation), receipt.PromotedRows, escapeSQLString(string(gateJSON)),
		receipt.PromotedAt.UTC().Format("2006-01-02 15:04:05.999999")), nil
}

func cleanupSQL(r renderedProposal) string {
	return fmt.Sprintf("DROP SCHEMA IF EXISTS %s.%s CASCADE", quoteIdentifier(r.Catalog), quoteIdentifier(r.StagingSchema))
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func quotedRelation(parts ...string) string {
	quoted := make([]string, len(parts))
	for i, part := range parts {
		quoted[i] = quoteIdentifier(part)
	}
	return strings.Join(quoted, ".")
}

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
