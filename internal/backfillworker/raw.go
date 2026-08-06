package backfillworker

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/ingestbatch"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze/columnar"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	extract "github.com/withObsrvr/stellar-extract"
)

// RawLedgerOptions pins the logical schema and the otherwise nondeterministic
// extraction timestamps for one immutable backfill job.
type RawLedgerOptions struct {
	NetworkPassphrase    string
	SchemaVersion        string
	ExtractionVersion    string
	MaterializedAt       time.Time
	ProjectWorkers       int
	DirectContractEvents bool
	DirectColumnarTables bool
}

// RawLedger is one ledger after direct extraction. SourceXDR follows the
// ownership of the input passed to DecodeRawLedger: the sequential path keeps
// the source borrow, while the concurrent pipeline passes an owned copy.
type RawLedger struct {
	NetworkPassphrase    string
	LedgerSequence       uint32
	ClosedAtUnix         int64
	SchemaVersion        string
	ExtractionVersion    string
	TransactionCount     int
	OperationCount       int
	Rows                 []bronze.DecodedRow
	BronzeRowCount       int
	ExtractedData        *extract.LedgerData
	TransactionOverrides bronze.TransactionOverrides
	SourceXDR            []byte
	ViewDuration         time.Duration
	DecodeDuration       time.Duration
	ExtractDuration      time.Duration
	PinDuration          time.Duration
	EnvelopeDuration     time.Duration
	ProjectDuration      time.Duration
	ProcessingDuration   time.Duration
}

type rawLedgerAccumulator struct {
	descriptor ingestbatch.Descriptor
	network    string
	hasher     hash.Hash
}

func newRawLedgerAccumulator() *rawLedgerAccumulator {
	return &rawLedgerAccumulator{hasher: sha256.New()}
}

func (accumulator *rawLedgerAccumulator) Add(ledger *RawLedger) error {
	if ledger == nil {
		return fmt.Errorf("raw ledger %d is nil", accumulator.descriptor.LedgerCount)
	}
	if ledger.NetworkPassphrase == "" {
		return fmt.Errorf("raw ledger %d has empty network passphrase", ledger.LedgerSequence)
	}
	if accumulator.descriptor.LedgerCount == 0 {
		accumulator.descriptor.LedgerStart = ledger.LedgerSequence
		accumulator.network = ledger.NetworkPassphrase
	} else {
		expected := accumulator.descriptor.LedgerEnd + 1
		if ledger.LedgerSequence != expected {
			return fmt.Errorf("raw ledger %d follows %d, want %d", ledger.LedgerSequence, accumulator.descriptor.LedgerEnd, expected)
		}
		if ledger.NetworkPassphrase != accumulator.network {
			return fmt.Errorf("raw ledger %d changes network passphrase", ledger.LedgerSequence)
		}
	}
	var length [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(length[:], uint64(len(ledger.SourceXDR)))
	_, _ = accumulator.hasher.Write(length[:n])
	_, _ = accumulator.hasher.Write(ledger.SourceXDR)
	accumulator.descriptor.LedgerEnd = ledger.LedgerSequence
	accumulator.descriptor.LedgerCount++
	accumulator.descriptor.EncodedBytes += uint64(len(ledger.SourceXDR))
	rowCount := ledger.BronzeRowCount
	if rowCount == 0 {
		rowCount = len(ledger.Rows)
	}
	accumulator.descriptor.BronzeRows += uint64(rowCount)
	return nil
}

func (accumulator *rawLedgerAccumulator) Totals() ingestbatch.Descriptor {
	return accumulator.descriptor
}

func (accumulator *rawLedgerAccumulator) Descriptor() (ingestbatch.Descriptor, error) {
	if accumulator.descriptor.LedgerCount == 0 {
		return ingestbatch.Descriptor{}, fmt.Errorf("raw ledger stream is empty")
	}
	descriptor := accumulator.descriptor
	descriptor.PayloadSHA256 = hex.EncodeToString(accumulator.hasher.Sum(nil))
	descriptor.ID = descriptor.PayloadSHA256
	return descriptor, nil
}

// DecodeRawLedger performs one full LedgerCloseMeta decode through
// stellar-extract and projects its typed rows directly to Appender values.
// It does not construct a LedgerBatch protobuf or per-row JSON.
func DecodeRawLedger(raw []byte, opts RawLedgerOptions) (*RawLedger, error) {
	processingStarted := time.Now()
	if opts.NetworkPassphrase == "" {
		return nil, fmt.Errorf("network passphrase is required")
	}
	if opts.MaterializedAt.IsZero() {
		return nil, fmt.Errorf("pinned materialization timestamp is required")
	}
	if opts.SchemaVersion == "" {
		opts.SchemaVersion = contracts.SchemaVersion
	}
	if opts.ExtractionVersion == "" {
		opts.ExtractionVersion = contracts.ExtractionVersion
	}
	if opts.ProjectWorkers <= 0 {
		opts.ProjectWorkers = 1
	}

	viewStarted := time.Now()
	view, err := extract.NewLedgerViewInput(raw, opts.NetworkPassphrase)
	if err != nil {
		return nil, fmt.Errorf("create ledger view input: %w", err)
	}
	viewDuration := time.Since(viewStarted)
	decodeStarted := time.Now()
	input, err := view.Decode()
	if err != nil {
		return nil, fmt.Errorf("decode ledger %d: %w", view.Sequence, err)
	}
	decodeDuration := time.Since(decodeStarted)
	extractStarted := time.Now()
	data, extractionErrs := extract.ExtractAll(input)
	if len(extractionErrs) > 0 {
		return nil, fmt.Errorf("extract ledger %d: %w", view.Sequence, errors.Join(extractionErrs...))
	}
	extractDuration := time.Since(extractStarted)
	pinStarted := time.Now()
	pinMaterializationTimes(data, opts.MaterializedAt.UTC())
	pinDuration := time.Since(pinStarted)
	envelopeStarted := time.Now()
	overrides, operationCount, err := rawTransactionOverrides(input.LCM)
	if err != nil {
		return nil, fmt.Errorf("ledger %d transaction XDR: %w", view.Sequence, err)
	}
	envelopeDuration := time.Since(envelopeStarted)
	projectStarted := time.Now()
	rowCount := bronze.LedgerDataRowCount(data)
	var rows []bronze.DecodedRow
	if opts.DirectColumnarTables {
		rows = bronze.ProjectLedgerDataExceptWithWorkers(
			data,
			overrides,
			opts.ProjectWorkers,
			columnar.ContractEventsTable,
			columnar.TransactionsTable,
			columnar.OperationsTable,
			columnar.EffectsTable,
			columnar.TokenTransfersTable,
		)
	} else if opts.DirectContractEvents {
		rows = bronze.ProjectLedgerDataExceptWithWorkers(data, overrides, opts.ProjectWorkers, columnar.ContractEventsTable)
	} else {
		rows = bronze.ProjectLedgerDataWithWorkers(data, overrides, opts.ProjectWorkers)
	}
	for index := range rows {
		if rows[index].Err != nil || !rows[index].OK {
			return nil, fmt.Errorf("ledger %d direct row %d (%s): %w", view.Sequence, index, rows[index].Spec.TableName, rows[index].Err)
		}
	}
	projectDuration := time.Since(projectStarted)

	return &RawLedger{
		NetworkPassphrase:    opts.NetworkPassphrase,
		LedgerSequence:       view.Sequence,
		ClosedAtUnix:         view.ClosedAt.Unix(),
		SchemaVersion:        opts.SchemaVersion,
		ExtractionVersion:    opts.ExtractionVersion,
		TransactionCount:     input.LCM.CountTransactions(),
		OperationCount:       operationCount,
		Rows:                 rows,
		BronzeRowCount:       rowCount,
		ExtractedData:        data,
		TransactionOverrides: overrides,
		SourceXDR:            raw,
		ViewDuration:         viewDuration,
		DecodeDuration:       decodeDuration,
		ExtractDuration:      extractDuration,
		PinDuration:          pinDuration,
		EnvelopeDuration:     envelopeDuration,
		ProjectDuration:      projectDuration,
		ProcessingDuration:   time.Since(processingStarted),
	}, nil
}

func rawTransactionOverrides(lcm xdr.LedgerCloseMeta) (bronze.TransactionOverrides, int, error) {
	overrides := make(bronze.TransactionOverrides, lcm.CountTransactions())
	envelopes := lcm.TransactionEnvelopes()
	operationCount := 0
	for index := 0; index < lcm.CountTransactions(); index++ {
		hash := lcm.TransactionHash(index).HexString()
		resultXDR, err := xdr.MarshalBase64(lcm.TransactionResultPair(index))
		if err != nil {
			return nil, 0, fmt.Errorf("marshal transaction result %d: %w", index, err)
		}
		metaXDR, err := xdr.MarshalBase64(lcm.TxApplyProcessing(index))
		if err != nil {
			return nil, 0, fmt.Errorf("marshal transaction meta %d: %w", index, err)
		}
		values := map[string]any{
			"tx_result": resultXDR,
			"tx_meta":   metaXDR,
		}
		if index < len(envelopes) {
			envelopeXDR, err := xdr.MarshalBase64(envelopes[index])
			if err != nil {
				return nil, 0, fmt.Errorf("marshal transaction envelope %d: %w", index, err)
			}
			values["tx_envelope"] = envelopeXDR
			operationCount += len(envelopes[index].Operations())
		}
		overrides[hash] = values
	}
	return overrides, operationCount, nil
}

func pinMaterializationTimes(data *extract.LedgerData, pinned time.Time) {
	if data == nil {
		return
	}
	for index := range data.Ledgers {
		data.Ledgers[index].IngestionTimestamp = pinned
	}
	for index := range data.Accounts {
		data.Accounts[index].CreatedAt = pinned
		data.Accounts[index].UpdatedAt = pinned
	}
	for index := range data.Offers {
		data.Offers[index].CreatedAt = pinned
	}
	for index := range data.Trustlines {
		data.Trustlines[index].CreatedAt = pinned
	}
	for index := range data.AccountSigners {
		data.AccountSigners[index].CreatedAt = pinned
	}
	for index := range data.ClaimableBalances {
		data.ClaimableBalances[index].CreatedAt = pinned
	}
	for index := range data.LiquidityPools {
		data.LiquidityPools[index].CreatedAt = pinned
	}
	for index := range data.ConfigSettings {
		data.ConfigSettings[index].CreatedAt = pinned
	}
	for index := range data.TTLEntries {
		data.TTLEntries[index].CreatedAt = pinned
	}
	for index := range data.ContractEvents {
		data.ContractEvents[index].CreatedAt = pinned
	}
	for index := range data.ContractData {
		data.ContractData[index].CreatedAt = pinned
	}
	for index := range data.ContractCode {
		data.ContractCode[index].CreatedAt = pinned
	}
	for index := range data.TokenTransfers {
		data.TokenTransfers[index].CreatedAt = pinned
	}
	for index := range data.EvictedKeys {
		data.EvictedKeys[index].CreatedAt = pinned
	}
	for index := range data.RestoredKeys {
		data.RestoredKeys[index].CreatedAt = pinned
	}
}
