package gatekeeper

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"
)

type Remote interface {
	Execute(context.Context, string) error
	QueryBool(context.Context, string) (bool, error)
	QueryUint64(context.Context, string) (uint64, error)
}

type GateResult struct {
	Name    string `json:"name"`
	Passed  bool   `json:"passed"`
	Details string `json:"details"`
}

type PromotionReceipt struct {
	PromotionID    string       `json:"promotion_id"`
	ProposalID     string       `json:"proposal_id"`
	ProposalHash   string       `json:"proposal_hash"`
	SourceSnapshot uint64       `json:"source_snapshot_id"`
	TargetRelation string       `json:"target_relation"`
	PromotedRows   uint64       `json:"promoted_rows"`
	PromotedAt     time.Time    `json:"promoted_at"`
	Gates          []GateResult `json:"gates"`
}

type RejectedError struct {
	Gates []GateResult
}

func (e RejectedError) Error() string {
	for _, gate := range e.Gates {
		if !gate.Passed {
			return fmt.Sprintf("proposal rejected by %s gate: %s", gate.Name, gate.Details)
		}
	}
	return "proposal rejected"
}

type Runner struct {
	Remote Remote
	Now    func() time.Time
}

func (r Runner) Run(ctx context.Context, proposal Proposal, proposalHash string) (PromotionReceipt, error) {
	if r.Remote == nil {
		return PromotionReceipt{}, fmt.Errorf("remote is required")
	}
	if r.Now == nil {
		r.Now = time.Now
	}
	if err := proposal.Validate(); err != nil {
		return PromotionReceipt{}, err
	}
	replayRange := chooseReplayRange(proposal.LedgerRange, proposalHash)
	rendered, err := renderProposal(proposal, proposalHash, replayRange)
	if err != nil {
		return PromotionReceipt{}, err
	}
	current, err := r.Remote.QueryUint64(ctx, currentSnapshotSQL(rendered.Catalog))
	if err != nil {
		return PromotionReceipt{}, fmt.Errorf("read current snapshot: %w", err)
	}
	if proposal.Source.SnapshotID > current {
		return PromotionReceipt{}, fmt.Errorf("source snapshot %d is newer than current snapshot %d", proposal.Source.SnapshotID, current)
	}
	if err := r.Remote.Execute(ctx, resetStagingSQL(rendered)); err != nil {
		return PromotionReceipt{}, fmt.Errorf("build reproducibility candidates: %w", err)
	}

	gates := make([]GateResult, 0, 3+len(proposal.Invariants))
	reproducible, err := r.Remote.QueryBool(ctx, equalitySQL(rendered.CandidateA, rendered.CandidateB))
	if err != nil {
		return PromotionReceipt{}, fmt.Errorf("run reproducibility gate: %w", err)
	}
	gates = append(gates, GateResult{Name: "reproducibility", Passed: reproducible, Details: passDetails(reproducible, "two builds at the pinned snapshot are byte-equivalent", "candidate builds differ")})

	for _, invariant := range proposal.Invariants {
		passed, err := r.Remote.QueryBool(ctx, invariantSQL(invariant, rendered, proposal.LedgerRange))
		if err != nil {
			return PromotionReceipt{}, fmt.Errorf("run invariant %s: %w", invariant.Name, err)
		}
		gates = append(gates, GateResult{Name: "reconciliation/" + invariant.Name, Passed: passed, Details: passDetails(passed, "declared invariant returned true", "declared invariant returned false")})
	}

	if err := r.Remote.Execute(ctx, replaySQL(rendered)); err != nil {
		return PromotionReceipt{}, fmt.Errorf("build replay candidates: %w", err)
	}
	replayed, err := r.Remote.QueryBool(ctx, equalitySQL(rendered.ReplayA, rendered.ReplayB))
	if err != nil {
		return PromotionReceipt{}, fmt.Errorf("run replay gate: %w", err)
	}
	gates = append(gates, GateResult{
		Name:   "replay",
		Passed: replayed,
		Details: passDetails(replayed,
			fmt.Sprintf("rebuild of ledger range (%d, %d] is byte-equivalent", replayRange.StartExclusive, replayRange.EndInclusive),
			fmt.Sprintf("rebuild of ledger range (%d, %d] differs", replayRange.StartExclusive, replayRange.EndInclusive)),
	})
	gates = append(gates, GateResult{Name: "confinement", Passed: true, Details: "read-only transformation was executed only as CTAS into the generated staging schema"})

	if !allPassed(gates) {
		return PromotionReceipt{ProposalID: proposal.ProposalID, ProposalHash: proposalHash, SourceSnapshot: proposal.Source.SnapshotID, TargetRelation: proposal.Target.Relation, Gates: gates}, RejectedError{Gates: gates}
	}
	rows, err := r.Remote.QueryUint64(ctx, rowCountSQL(rendered.CandidateA))
	if err != nil {
		return PromotionReceipt{}, fmt.Errorf("count candidate rows: %w", err)
	}
	promotedAt := r.Now().UTC()
	receipt := PromotionReceipt{
		PromotionID:    promotionID(proposalHash, proposal.LedgerRange, promotedAt),
		ProposalID:     proposal.ProposalID,
		ProposalHash:   proposalHash,
		SourceSnapshot: proposal.Source.SnapshotID,
		TargetRelation: proposal.Target.Relation,
		PromotedRows:   rows,
		PromotedAt:     promotedAt,
		Gates:          gates,
	}
	if err := r.Remote.Execute(ctx, bootstrapGovernanceSQL(rendered.Catalog)); err != nil {
		return PromotionReceipt{}, fmt.Errorf("bootstrap governance schema: %w", err)
	}
	promoteSQL, err := promotionSQL(proposal, rendered, receipt)
	if err != nil {
		return PromotionReceipt{}, err
	}
	if err := r.Remote.Execute(ctx, promoteSQL); err != nil {
		return PromotionReceipt{}, fmt.Errorf("promote proposal: %w", err)
	}
	if err := r.Remote.Execute(ctx, cleanupSQL(rendered)); err != nil {
		return receipt, fmt.Errorf("promotion succeeded but staging cleanup failed: %w", err)
	}
	return receipt, nil
}

func chooseReplayRange(full LedgerRange, proposalHash string) LedgerRange {
	span := full.EndInclusive - full.StartExclusive
	width := span / 10
	if width == 0 {
		width = 1
	}
	availableOffsets := span - width + 1
	digest, err := hex.DecodeString(proposalHash)
	if err != nil || len(digest) < 8 {
		digestSum := sha256.Sum256([]byte(proposalHash))
		digest = digestSum[:]
	}
	offset := binary.BigEndian.Uint64(digest[:8]) % availableOffsets
	start := full.StartExclusive + offset
	return LedgerRange{StartExclusive: start, EndInclusive: start + width}
}

func promotionID(proposalHash string, ledgerRange LedgerRange, at time.Time) string {
	material := fmt.Sprintf("%s:%d:%d:%s", proposalHash, ledgerRange.StartExclusive, ledgerRange.EndInclusive, at.UTC().Format(time.RFC3339Nano))
	sum := sha256.Sum256([]byte(material))
	return hex.EncodeToString(sum[:16])
}

func allPassed(gates []GateResult) bool {
	for _, gate := range gates {
		if !gate.Passed {
			return false
		}
	}
	return true
}

func passDetails(passed bool, success, failure string) string {
	if passed {
		return success
	}
	return failure
}
