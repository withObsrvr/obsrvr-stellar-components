package extract

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// Version is the pipeline version stamped on each ledger row.
var Version = "dev"

// ExtractLedgers extracts ledger-level metadata from a single LedgerCloseMeta.
// Returns a slice with exactly one LedgerRowData element per ledger.
func ExtractLedgers(input *LedgerInput) ([]LedgerRowData, error) {
	lcm := input.LCM
	ledgerSeq := input.Sequence
	closedAt := input.ClosedAt
	ledgerRange := input.LedgerRange

	// Get ledger header based on version
	var header xdr.LedgerHeaderHistoryEntry
	switch lcm.V {
	case 0:
		header = lcm.MustV0().LedgerHeader
	case 1:
		header = lcm.MustV1().LedgerHeader
	case 2:
		header = lcm.MustV2().LedgerHeader
	default:
		return nil, fmt.Errorf("unknown LedgerCloseMeta version: %d", lcm.V)
	}

	data := LedgerRowData{
		Sequence:           ledgerSeq,
		LedgerHash:         hex.EncodeToString(header.Hash[:]),
		PreviousLedgerHash: hex.EncodeToString(header.Header.PreviousLedgerHash[:]),
		ClosedAt:           closedAt,
		ProtocolVersion:    uint32(header.Header.LedgerVersion),
		TotalCoins:         int64(header.Header.TotalCoins),
		FeePool:            int64(header.Header.FeePool),
		BaseFee:            uint32(header.Header.BaseFee),
		BaseReserve:        uint32(header.Header.BaseReserve),
		MaxTxSetSize:       uint32(header.Header.MaxTxSetSize),
		IngestionTimestamp:  time.Now().UTC(),
		LedgerRange:        ledgerRange,
		PipelineVersion:    Version,
		EraID:              input.EraID,
	}

	// Count transactions and operations based on LedgerCloseMeta version
	var txCount uint32
	var failedCount uint32
	var operationCount uint32
	var txSetOperationCount uint32

	switch lcm.V {
	case 0:
		v0 := lcm.MustV0()
		txCount = uint32(len(v0.TxSet.Txs))
		for _, tx := range v0.TxSet.Txs {
			opCount := uint32(len(tx.Operations()))
			txSetOperationCount += opCount
			operationCount += opCount
		}
	case 1:
		v1 := lcm.MustV1()
		txCount = uint32(len(v1.TxProcessing))
		for _, txApply := range v1.TxProcessing {
			if opResults, ok := txApply.Result.Result.OperationResults(); ok {
				opCount := uint32(len(opResults))
				txSetOperationCount += opCount
				if txApply.Result.Result.Successful() {
					operationCount += opCount
				} else {
					failedCount++
				}
			} else {
				failedCount++
			}
		}
	case 2:
		v2 := lcm.MustV2()
		txCount = uint32(len(v2.TxProcessing))
		for _, txApply := range v2.TxProcessing {
			if opResults, ok := txApply.Result.Result.OperationResults(); ok {
				opCount := uint32(len(opResults))
				txSetOperationCount += opCount
				if txApply.Result.Result.Successful() {
					operationCount += opCount
				} else {
					failedCount++
				}
			} else {
				failedCount++
			}
		}
	}

	data.TransactionCount = int(txCount)
	data.SuccessfulTxCount = int(txCount - failedCount)
	data.FailedTxCount = int(failedCount)
	data.OperationCount = int(operationCount)
	data.TxSetOperationCount = int(txSetOperationCount)

	// --- New fields for DuckLake parity ---

	// (a) soroban_fee_write1kb: from LedgerCloseMetaExt V1
	if lcmV1, ok := lcm.GetV1(); ok {
		if extV1, ok := lcmV1.Ext.GetV1(); ok {
			feeWrite := int64(extV1.SorobanFeeWrite1Kb)
			data.SorobanFeeWrite1kb = &feeWrite
		}
	} else if lcmV2, ok := lcm.GetV2(); ok {
		if extV1, ok := lcmV2.Ext.GetV1(); ok {
			feeWrite := int64(extV1.SorobanFeeWrite1Kb)
			data.SorobanFeeWrite1kb = &feeWrite
		}
	}

	// (b) node_id + signature: from SCP value extension
	if lcValueSig, ok := header.Header.ScpValue.Ext.GetLcValueSignature(); ok {
		if ed25519, ok := xdr.PublicKey(lcValueSig.NodeId).GetEd25519(); ok {
			nodeIDStr := base64.StdEncoding.EncodeToString(ed25519[:])
			data.NodeID = &nodeIDStr
		}
		sigStr := base64.StdEncoding.EncodeToString(lcValueSig.Signature)
		data.Signature = &sigStr
	}

	// (c) ledger_header: full header XDR base64 encoded
	if headerXDR, err := header.Header.MarshalBinary(); err == nil {
		headerStr := base64.StdEncoding.EncodeToString(headerXDR)
		data.LedgerHeader = &headerStr
	}

	// (d) bucket_list_size + (e) live_soroban_state_size: from V1/V2
	// NOTE: Both use TotalByteSizeOfLiveSorobanState (matches stellar-etl behavior)
	if lcmV1, ok := lcm.GetV1(); ok {
		sorobanStateSize := int64(lcmV1.TotalByteSizeOfLiveSorobanState)
		data.BucketListSize = &sorobanStateSize
		data.LiveSorobanStateSize = &sorobanStateSize
	} else if lcmV2, ok := lcm.GetV2(); ok {
		sorobanStateSize := int64(lcmV2.TotalByteSizeOfLiveSorobanState)
		data.BucketListSize = &sorobanStateSize
		data.LiveSorobanStateSize = &sorobanStateSize
	}

	// (f) evicted_keys_count
	if lcmV1, ok := lcm.GetV1(); ok {
		evicted := int32(len(lcmV1.EvictedKeys))
		data.EvictedKeysCount = &evicted
	} else if lcmV2, ok := lcm.GetV2(); ok {
		evicted := int32(len(lcmV2.EvictedKeys))
		data.EvictedKeysCount = &evicted
	}

	// (g) soroban_op_count: count operations with type 24/25/26
	var sorobanOpCount int32
	envelopes := lcm.TransactionEnvelopes()
	for _, env := range envelopes {
		for _, op := range env.Operations() {
			switch op.Body.Type {
			case xdr.OperationTypeInvokeHostFunction,
				xdr.OperationTypeExtendFootprintTtl,
				xdr.OperationTypeRestoreFootprint:
				sorobanOpCount++
			}
		}
	}
	data.SorobanOpCount = &sorobanOpCount

	// (h) total_fee_charged + (i) contract_events_count
	var totalFeeCharged int64
	var contractEventsCount int32

	switch lcm.V {
	case 1:
		for _, txApply := range lcm.MustV1().TxProcessing {
			totalFeeCharged += int64(txApply.Result.Result.FeeCharged)
			contractEventsCount += countLedgerContractEvents(&txApply.TxApplyProcessing)
		}
	case 2:
		for _, txApply := range lcm.MustV2().TxProcessing {
			totalFeeCharged += int64(txApply.Result.Result.FeeCharged)
			contractEventsCount += countLedgerContractEvents(&txApply.TxApplyProcessing)
		}
	}
	data.TotalFeeCharged = &totalFeeCharged
	data.ContractEventsCount = &contractEventsCount

	return []LedgerRowData{data}, nil
}

// countLedgerContractEvents counts contract events in a transaction meta.
func countLedgerContractEvents(meta *xdr.TransactionMeta) int32 {
	switch meta.V {
	case 3:
		v3 := meta.MustV3()
		if v3.SorobanMeta != nil {
			return int32(len(v3.SorobanMeta.Events))
		}
	case 4:
		v4 := meta.MustV4()
		var count int32
		for _, op := range v4.Operations {
			count += int32(len(op.Events))
		}
		return count
	}
	return 0
}
