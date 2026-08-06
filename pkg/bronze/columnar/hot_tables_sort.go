package columnar

import (
	"cmp"
	"slices"

	extract "github.com/withObsrvr/stellar-extract"
)

func SortTransactions(rows []extract.TransactionData) {
	slices.SortStableFunc(rows, func(left, right extract.TransactionData) int {
		if comparison := cmp.Compare(left.LedgerSequence, right.LedgerSequence); comparison != 0 {
			return comparison
		}
		return cmp.Compare(left.TransactionHash, right.TransactionHash)
	})
}

func SortOperations(rows []extract.OperationData) {
	slices.SortStableFunc(rows, func(left, right extract.OperationData) int {
		for _, comparison := range []int{
			cmp.Compare(left.TransactionHash, right.TransactionHash),
			cmp.Compare(left.OperationIndex, right.OperationIndex),
			cmp.Compare(left.LedgerSequence, right.LedgerSequence),
		} {
			if comparison != 0 {
				return comparison
			}
		}
		return 0
	})
}

func SortEffects(rows []extract.EffectData) {
	slices.SortStableFunc(rows, func(left, right extract.EffectData) int {
		for _, comparison := range []int{
			cmp.Compare(left.LedgerSequence, right.LedgerSequence),
			cmp.Compare(left.TransactionHash, right.TransactionHash),
			cmp.Compare(left.OperationIndex, right.OperationIndex),
			cmp.Compare(left.EffectIndex, right.EffectIndex),
			cmp.Compare(left.EffectType, right.EffectType),
			cmp.Compare(left.EffectTypeString, right.EffectTypeString),
		} {
			if comparison != 0 {
				return comparison
			}
		}
		return 0
	})
}

func SortTokenTransfers(rows []extract.TokenTransferData) {
	slices.SortStableFunc(rows, func(left, right extract.TokenTransferData) int {
		for _, comparison := range []int{
			cmp.Compare(left.LedgerSequence, right.LedgerSequence),
			cmp.Compare(left.TransactionHash, right.TransactionHash),
			cmp.Compare(left.TransactionID, right.TransactionID),
			compareOptional(left.OperationID, right.OperationID),
			compareOptional(left.OperationIndex, right.OperationIndex),
			cmp.Compare(left.EventType, right.EventType),
			compareOptional(left.From, right.From),
			compareOptional(left.To, right.To),
			cmp.Compare(left.Asset, right.Asset),
			cmp.Compare(left.AssetType, right.AssetType),
			compareOptional(left.AssetCode, right.AssetCode),
			compareOptional(left.AssetIssuer, right.AssetIssuer),
			cmp.Compare(left.Amount, right.Amount),
			cmp.Compare(left.AmountRaw, right.AmountRaw),
			cmp.Compare(left.ContractID, right.ContractID),
			compareOptional(left.EraID, right.EraID),
		} {
			if comparison != 0 {
				return comparison
			}
		}
		return 0
	})
}

func compareOptional[T cmp.Ordered](left, right *T) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}
	return cmp.Compare(*left, *right)
}
