package extract

import (
	"fmt"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// LedgerViewInput is the raw-XDR extraction boundary used by full-history
// ingestion. The XDR view and all transaction views borrow xdrBytes from
// NewLedgerViewInput; callers must finish extraction before advancing the
// upstream ledger stream or otherwise reusing that buffer.
//
// Do not copy a LedgerViewInput after first use. Its transaction-view cache is
// synchronized so all view-backed extractors share one ledger transaction
// walk.
type LedgerViewInput struct {
	NetworkPassphrase string
	Sequence          uint32
	ClosedAt          time.Time
	LedgerRange       uint32
	EraID             *string

	view xdr.LedgerCloseMetaView

	transactionsOnce sync.Once
	transactions     []ingest.LedgerTransactionView
	transactionsErr  error
}

// NewLedgerViewInput validates canonical LedgerCloseMeta XDR and creates a
// borrowed view input without decoding the full ledger object graph.
func NewLedgerViewInput(xdrBytes []byte, networkPassphrase string) (*LedgerViewInput, error) {
	view := xdr.LedgerCloseMetaView(xdrBytes)
	if err := view.ValidateFull(); err != nil {
		return nil, fmt.Errorf("validate LedgerCloseMeta XDR: %w", err)
	}
	sequence, err := view.LedgerSequence()
	if err != nil {
		return nil, fmt.Errorf("read ledger sequence: %w", err)
	}
	closeTime, err := view.LedgerCloseTime()
	if err != nil {
		return nil, fmt.Errorf("read ledger %d close time: %w", sequence, err)
	}

	return &LedgerViewInput{
		NetworkPassphrase: networkPassphrase,
		Sequence:          sequence,
		ClosedAt:          time.Unix(closeTime, 0).UTC(),
		LedgerRange:       (sequence / 10000) * 10000,
		view:              view,
	}, nil
}

// Decode materializes the compatibility LedgerInput once a parsed extractor
// is needed. View-backed extractors should operate on LedgerViewInput directly.
func (input *LedgerViewInput) Decode() (*LedgerInput, error) {
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(input.view); err != nil {
		return nil, fmt.Errorf("unmarshal LedgerCloseMeta: %w", err)
	}
	return &LedgerInput{
		LCM:               lcm,
		NetworkPassphrase: input.NetworkPassphrase,
		Sequence:          input.Sequence,
		ClosedAt:          input.ClosedAt,
		LedgerRange:       input.LedgerRange,
		EraID:             input.EraID,
	}, nil
}

// ExtractAllFromXDR is the direct raw-XDR-to-typed-rows entry point. The
// returned rows do not retain the borrowed ledger bytes.
func ExtractAllFromXDR(xdrBytes []byte, networkPassphrase string) (*LedgerData, []error) {
	input, err := NewLedgerInputFromXDR(xdrBytes, networkPassphrase)
	if err != nil {
		return &LedgerData{}, []error{err}
	}
	return ExtractAll(input)
}

// ExtractAllView preserves the complete typed LedgerData surface by decoding
// one compatibility input. Individual view-backed extractors can avoid that
// decode; the full surface should not mix both representations until enough
// table families are view-native to remove the parsed object graph.
func ExtractAllView(input *LedgerViewInput) (*LedgerData, []error) {
	parsed, err := input.Decode()
	if err != nil {
		return &LedgerData{}, []error{err}
	}
	return ExtractAll(parsed)
}

func (input *LedgerViewInput) transactionViews() ([]ingest.LedgerTransactionView, error) {
	input.transactionsOnce.Do(func() {
		input.transactions, input.transactionsErr = ingest.LedgerTransactionViewRange(
			input.view,
			0,
			0,
			input.NetworkPassphrase,
		)
	})
	return input.transactions, input.transactionsErr
}
