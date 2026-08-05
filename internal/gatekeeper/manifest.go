package gatekeeper

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

const APIVersion = "gatekeeper.obsrvr.dev/v1alpha1"

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Proposal is the declarative contract the gatekeeper verifies and promotes.
// Ledger ranges are half-open: (start_exclusive, end_inclusive].
type Proposal struct {
	APIVersion     string      `yaml:"api_version" json:"api_version"`
	ProposalID     string      `yaml:"proposal_id" json:"proposal_id"`
	AgentID        string      `yaml:"agent_id" json:"agent_id"`
	Source         Source      `yaml:"source" json:"source"`
	LedgerRange    LedgerRange `yaml:"ledger_range" json:"ledger_range"`
	Target         Target      `yaml:"target" json:"target"`
	Transformation string      `yaml:"transformation" json:"transformation"`
	Invariants     []Invariant `yaml:"invariants" json:"invariants"`
}

type Source struct {
	Relation   string `yaml:"relation" json:"relation"`
	SnapshotID uint64 `yaml:"snapshot_id" json:"snapshot_id"`
}

type LedgerRange struct {
	StartExclusive uint64 `yaml:"start_exclusive" json:"start_exclusive"`
	EndInclusive   uint64 `yaml:"end_inclusive" json:"end_inclusive"`
}

type Target struct {
	Relation    string   `yaml:"relation" json:"relation"`
	ReplaceKeys []string `yaml:"replace_keys" json:"replace_keys"`
}

type Invariant struct {
	Name string `yaml:"name" json:"name"`
	SQL  string `yaml:"sql" json:"sql"`
}

func LoadProposal(path string) (Proposal, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Proposal{}, "", fmt.Errorf("read proposal: %w", err)
	}
	proposal, hash, err := ParseProposal(raw)
	if err != nil {
		return Proposal{}, "", fmt.Errorf("parse proposal %s: %w", path, err)
	}
	return proposal, hash, nil
}

func ParseProposal(raw []byte) (Proposal, string, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	var proposal Proposal
	if err := decoder.Decode(&proposal); err != nil {
		return Proposal{}, "", err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return Proposal{}, "", fmt.Errorf("proposal must contain exactly one YAML document")
		}
		return Proposal{}, "", err
	}
	if err := proposal.Validate(); err != nil {
		return Proposal{}, "", err
	}
	canonical, err := json.Marshal(proposal)
	if err != nil {
		return Proposal{}, "", fmt.Errorf("canonicalize proposal: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return proposal, hex.EncodeToString(sum[:]), nil
}

func (p Proposal) Validate() error {
	if p.APIVersion != APIVersion {
		return fmt.Errorf("api_version must be %q", APIVersion)
	}
	if err := validateName("proposal_id", p.ProposalID); err != nil {
		return err
	}
	if strings.TrimSpace(p.AgentID) == "" {
		return fmt.Errorf("agent_id is required")
	}
	sourceParts, err := parseRelation("source.relation", p.Source.Relation)
	if err != nil {
		return err
	}
	targetParts, err := parseRelation("target.relation", p.Target.Relation)
	if err != nil {
		return err
	}
	if sourceParts[0] != targetParts[0] {
		return fmt.Errorf("source and target must use the same catalog for atomic promotion")
	}
	if p.Source.SnapshotID == 0 {
		return fmt.Errorf("source.snapshot_id must be greater than zero")
	}
	if p.LedgerRange.StartExclusive >= p.LedgerRange.EndInclusive {
		return fmt.Errorf("ledger_range must satisfy start_exclusive < end_inclusive")
	}
	if len(p.Target.ReplaceKeys) == 0 {
		return fmt.Errorf("target.replace_keys must contain at least one key")
	}
	seenKeys := make(map[string]struct{}, len(p.Target.ReplaceKeys))
	for _, key := range p.Target.ReplaceKeys {
		if err := validateName("target.replace_keys", key); err != nil {
			return err
		}
		if _, duplicate := seenKeys[key]; duplicate {
			return fmt.Errorf("target.replace_keys contains duplicate %q", key)
		}
		seenKeys[key] = struct{}{}
	}
	if err := validateReadOnlyTemplate("transformation", p.Transformation); err != nil {
		return err
	}
	for _, placeholder := range []string{"{{source}}", "{{start_ledger}}", "{{end_ledger}}"} {
		if !strings.Contains(p.Transformation, placeholder) {
			return fmt.Errorf("transformation must contain %s", placeholder)
		}
	}
	if len(p.Invariants) == 0 {
		return fmt.Errorf("at least one invariant is required")
	}
	seenInvariants := make(map[string]struct{}, len(p.Invariants))
	for i, invariant := range p.Invariants {
		if err := validateName(fmt.Sprintf("invariants[%d].name", i), invariant.Name); err != nil {
			return err
		}
		if _, duplicate := seenInvariants[invariant.Name]; duplicate {
			return fmt.Errorf("duplicate invariant name %q", invariant.Name)
		}
		seenInvariants[invariant.Name] = struct{}{}
		if err := validateReadOnlyTemplate(fmt.Sprintf("invariants[%d].sql", i), invariant.SQL); err != nil {
			return err
		}
		if !strings.Contains(invariant.SQL, "{{candidate}}") {
			return fmt.Errorf("invariant %q must contain {{candidate}}", invariant.Name)
		}
	}
	return nil
}

func validateReadOnlyTemplate(field, value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fmt.Errorf("%s is required", field)
	}
	if strings.Contains(trimmed, ";") {
		return fmt.Errorf("%s must be one statement without a semicolon", field)
	}
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "select ") && !strings.HasPrefix(lower, "select\n") &&
		!strings.HasPrefix(lower, "with ") && !strings.HasPrefix(lower, "with\n") {
		return fmt.Errorf("%s must be a SELECT or WITH query", field)
	}
	return nil
}

func validateName(field, value string) error {
	if !identifierPattern.MatchString(value) {
		return fmt.Errorf("%s %q must match %s", field, value, identifierPattern.String())
	}
	return nil
}

func parseRelation(field, value string) ([3]string, error) {
	parts := strings.Split(strings.TrimSpace(value), ".")
	if len(parts) != 3 {
		return [3]string{}, fmt.Errorf("%s must be catalog.schema.table", field)
	}
	var parsed [3]string
	for i, part := range parts {
		if !identifierPattern.MatchString(part) {
			return [3]string{}, fmt.Errorf("%s part %q must match %s", field, part, identifierPattern.String())
		}
		parsed[i] = part
	}
	return parsed, nil
}
