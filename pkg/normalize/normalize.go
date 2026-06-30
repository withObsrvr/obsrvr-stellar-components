package normalize

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"
	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/ids"
	extract "github.com/withObsrvr/stellar-extract"
)

type Options struct {
	NetworkPassphrase string
	SchemaVersion     string
	ExtractionVersion string
}

func LedgerBatch(lcm xdr.LedgerCloseMeta, opts Options) (*componentsv1.LedgerBatch, error) {
	network := opts.NetworkPassphrase
	if network == "" {
		network = contracts.DefaultNetwork
	}
	schemaVersion := opts.SchemaVersion
	if schemaVersion == "" {
		schemaVersion = contracts.SchemaVersion
	}
	extractionVersion := opts.ExtractionVersion
	if extractionVersion == "" {
		extractionVersion = contracts.ExtractionVersion
	}

	sequence := lcm.LedgerSequence()
	closedAt := lcm.LedgerCloseTime()
	ledgerRange := (sequence / 10000) * 10000
	batch := &componentsv1.LedgerBatch{
		NetworkPassphrase: network,
		LedgerSequence:    sequence,
		ClosedAtUnix:      closedAt,
		SchemaVersion:     schemaVersion,
		ExtractionVersion: extractionVersion,
	}

	batch.Ledgers = append(batch.Ledgers, &componentsv1.LedgerRow{
		Id:                 ids.Ledger(network, sequence),
		NetworkPassphrase:  network,
		LedgerSequence:     sequence,
		ClosedAtUnix:       closedAt,
		LedgerHash:         lcm.LedgerHash().HexString(),
		PreviousLedgerHash: lcm.PreviousLedgerHash().HexString(),
		ProtocolVersion:    lcm.ProtocolVersion(),
		TransactionCount:   uint32(lcm.CountTransactions()),
		SchemaVersion:      schemaVersion,
		ExtractionVersion:  extractionVersion,
	})

	envelopes := lcm.TransactionEnvelopes()
	for i := 0; i < lcm.CountTransactions(); i++ {
		txIndex := uint32(i)
		txHash := lcm.TransactionHash(i).HexString()
		txID := ids.Transaction(network, sequence, txIndex, txHash)

		result := lcm.TransactionResultPair(i)
		resultXDR, err := marshalBase64(result)
		if err != nil {
			return nil, fmt.Errorf("marshal transaction result %d: %w", i, err)
		}

		meta := lcm.TxApplyProcessing(i)
		metaXDR, err := marshalBase64(meta)
		if err != nil {
			return nil, fmt.Errorf("marshal transaction meta %d: %w", i, err)
		}

		envelopeXDR := ""
		if i < len(envelopes) {
			envelopeXDR, err = marshalBase64(envelopes[i])
			if err != nil {
				return nil, fmt.Errorf("marshal transaction envelope %d: %w", i, err)
			}
		}

		batch.Transactions = append(batch.Transactions, &componentsv1.TransactionRow{
			Id:                txID,
			NetworkPassphrase: network,
			LedgerSequence:    sequence,
			TransactionIndex:  txIndex,
			TransactionHash:   txHash,
			Successful:        result.Successful(),
			EnvelopeXdr:       envelopeXDR,
			ResultXdr:         resultXDR,
			MetaXdr:           metaXDR,
		})

		if i < len(envelopes) {
			for opIndex, op := range envelopes[i].Operations() {
				opXDR, err := marshalBase64(op)
				if err != nil {
					return nil, fmt.Errorf("marshal operation %d/%d: %w", i, opIndex, err)
				}
				batch.Operations = append(batch.Operations, &componentsv1.OperationRow{
					Id:                ids.Operation(network, sequence, txIndex, uint32(opIndex)),
					TransactionId:     txID,
					NetworkPassphrase: network,
					LedgerSequence:    sequence,
					TransactionIndex:  txIndex,
					OperationIndex:    uint32(opIndex),
					OperationType:     op.Body.Type.String(),
					OperationXdr:      opXDR,
				})
			}
		}
	}

	input := extract.NewLedgerInput(lcm, network)
	data, errs := extract.ExtractAll(input)
	if len(errs) > 0 {
		return nil, fmt.Errorf("extract bronze rows: %w", errorsJoin(errs))
	}
	appendExtractRows(batch, network, sequence, ledgerRange, data)

	return batch, nil
}

func appendExtractRows(batch *componentsv1.LedgerBatch, network string, sequence, ledgerRange uint32, data *extract.LedgerData) {
	if data == nil {
		return
	}
	appendRows(batch, network, sequence, ledgerRange, "ledgers_row_v2", data.Ledgers)
	appendRows(batch, network, sequence, ledgerRange, "transactions_row_v2", data.Transactions)
	appendRows(batch, network, sequence, ledgerRange, "operations_row_v2", data.Operations)
	appendRows(batch, network, sequence, ledgerRange, "effects_row_v1", data.Effects)
	appendRows(batch, network, sequence, ledgerRange, "trades_row_v1", data.Trades)
	appendRows(batch, network, sequence, ledgerRange, "accounts_snapshot_v1", data.Accounts)
	appendRows(batch, network, sequence, ledgerRange, "offers_snapshot_v1", data.Offers)
	appendRows(batch, network, sequence, ledgerRange, "trustlines_snapshot_v1", data.Trustlines)
	appendRows(batch, network, sequence, ledgerRange, "account_signers_snapshot_v1", data.AccountSigners)
	appendRows(batch, network, sequence, ledgerRange, "claimable_balances_snapshot_v1", data.ClaimableBalances)
	appendRows(batch, network, sequence, ledgerRange, "liquidity_pools_snapshot_v1", data.LiquidityPools)
	appendRows(batch, network, sequence, ledgerRange, "config_settings_snapshot_v1", data.ConfigSettings)
	appendRows(batch, network, sequence, ledgerRange, "ttl_snapshot_v1", data.TTLEntries)
	appendRows(batch, network, sequence, ledgerRange, "native_balances_snapshot_v1", data.NativeBalances)
	appendRows(batch, network, sequence, ledgerRange, "contract_events_stream_v1", data.ContractEvents)
	appendRows(batch, network, sequence, ledgerRange, "contract_data_snapshot_v1", data.ContractData)
	appendRows(batch, network, sequence, ledgerRange, "contract_code_snapshot_v1", data.ContractCode)
	appendRows(batch, network, sequence, ledgerRange, "contract_creations_v1", data.ContractCreations)
	appendRows(batch, network, sequence, ledgerRange, "token_transfers_stream_v1", data.TokenTransfers)
	appendRows(batch, network, sequence, ledgerRange, "evicted_keys_state_v1", data.EvictedKeys)
	appendRows(batch, network, sequence, ledgerRange, "restored_keys_state_v1", data.RestoredKeys)
}

func appendRows[T any](batch *componentsv1.LedgerBatch, network string, sequence, ledgerRange uint32, table string, rows []T) {
	for i, row := range rows {
		rowJSON := normalizeJSON(row)
		batch.BronzeRows = append(batch.BronzeRows, &componentsv1.BronzeRow{
			Id:                bronzeID(network, sequence, table, i, rowJSON),
			TableName:         table,
			NetworkPassphrase: network,
			LedgerSequence:    sequence,
			LedgerRange:       ledgerRange,
			RowJson:           rowJSON,
		})
	}
}

func bronzeID(network string, sequence uint32, table string, index int, rowJSON string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%s:%d:%s", network, sequence, table, index, rowJSON)))
	return hex.EncodeToString(sum[:])
}

func normalizeJSON(value interface{}) string {
	data, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(data)
}

func errorsJoin(errs []error) error {
	return errors.Join(errs...)
}

func marshalBase64(value interface{}) (string, error) {
	return xdr.MarshalBase64(value)
}

func jsonString(value interface{}) string {
	data, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(data)
}
