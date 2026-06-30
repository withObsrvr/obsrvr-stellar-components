package normalize

import (
	"encoding/json"
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"
	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/ids"
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

	return batch, nil
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
