package gatekeeper

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type fakeRemote struct {
	executed []string
	bools    []bool
	uints    []uint64
	execErr  error
}

func (f *fakeRemote) Execute(_ context.Context, sql string) error {
	f.executed = append(f.executed, sql)
	return f.execErr
}

func (f *fakeRemote) QueryBool(_ context.Context, _ string) (bool, error) {
	if len(f.bools) == 0 {
		return false, errors.New("unexpected bool query")
	}
	value := f.bools[0]
	f.bools = f.bools[1:]
	return value, nil
}

func (f *fakeRemote) QueryUint64(_ context.Context, _ string) (uint64, error) {
	if len(f.uints) == 0 {
		return 0, errors.New("unexpected uint query")
	}
	value := f.uints[0]
	f.uints = f.uints[1:]
	return value, nil
}

func TestRunnerPromotesOnlyAfterAllGatesPass(t *testing.T) {
	p, hash := mustTestProposal(t)
	remote := &fakeRemote{bools: []bool{true, true, true}, uints: []uint64{44, 7}}
	now := time.Date(2026, 8, 4, 13, 0, 0, 0, time.UTC)
	receipt, err := (Runner{Remote: remote, Now: func() time.Time { return now }}).Run(context.Background(), p, hash)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if receipt.PromotedRows != 7 || len(receipt.Gates) != 4 {
		t.Fatalf("receipt = %+v", receipt)
	}
	joined := strings.Join(remote.executed, "\n")
	if !strings.Contains(joined, "governance.promotions") || !strings.Contains(joined, "BEGIN TRANSACTION;") {
		t.Fatalf("promotion not executed atomically:\n%s", joined)
	}
	if !strings.Contains(remote.executed[len(remote.executed)-1], "DROP SCHEMA") {
		t.Fatalf("last statement is not cleanup: %s", remote.executed[len(remote.executed)-1])
	}
}

func TestRunnerFreezesPublishedTableWhenGateFails(t *testing.T) {
	p, hash := mustTestProposal(t)
	remote := &fakeRemote{bools: []bool{false, true, true}, uints: []uint64{44}}
	receipt, err := (Runner{Remote: remote}).Run(context.Background(), p, hash)
	var rejected RejectedError
	if !errors.As(err, &rejected) {
		t.Fatalf("error = %v, want RejectedError", err)
	}
	if receipt.Gates[0].Passed {
		t.Fatalf("reproducibility gate unexpectedly passed: %+v", receipt.Gates)
	}
	for _, statement := range remote.executed {
		if strings.Contains(statement, "governance.promotions") || strings.Contains(statement, "BEGIN TRANSACTION") {
			t.Fatalf("failed proposal reached promotion SQL:\n%s", statement)
		}
	}
}

func TestRunnerRejectsFutureSnapshotBeforeStaging(t *testing.T) {
	p, hash := mustTestProposal(t)
	remote := &fakeRemote{uints: []uint64{41}}
	_, err := (Runner{Remote: remote}).Run(context.Background(), p, hash)
	if err == nil || !strings.Contains(err.Error(), "newer than current snapshot") {
		t.Fatalf("error = %v", err)
	}
	if len(remote.executed) != 0 {
		t.Fatalf("future snapshot mutated staging: %v", remote.executed)
	}
}

func TestChooseReplayRangeIsDeterministicAndBounded(t *testing.T) {
	full := LedgerRange{StartExclusive: 100, EndInclusive: 1100}
	a := chooseReplayRange(full, strings.Repeat("a", 64))
	b := chooseReplayRange(full, strings.Repeat("a", 64))
	if a != b {
		t.Fatalf("ranges differ: %+v != %+v", a, b)
	}
	if a.StartExclusive < full.StartExclusive || a.EndInclusive > full.EndInclusive || a.EndInclusive-a.StartExclusive != 100 {
		t.Fatalf("replay range is not bounded 10%% slice: %+v", a)
	}
}
