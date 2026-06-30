package extract

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ExtractTransactions extracts transaction data from a pre-decoded ledger.
func ExtractTransactions(input *LedgerInput) ([]TransactionData, error) {
	lcm := input.LCM
	ledgerSeq := input.Sequence
	closedAt := input.ClosedAt
	ledgerRange := input.LedgerRange

	var transactions []TransactionData

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(input.NetworkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer reader.Close()

	var txIndex int32 = 1 // 1-indexed per TOID convention
	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction in ledger %d: %v", ledgerSeq, err)
			continue
		}

		txData := TransactionData{
			LedgerSequence:        ledgerSeq,
			TransactionHash:       hex.EncodeToString(tx.Result.TransactionHash[:]),
			SourceAccount:         tx.Envelope.SourceAccount().ToAccountId().Address(),
			SourceAccountMuxed:    getMuxedAddress(tx.Envelope.SourceAccount()),
			FeeCharged:            int64(tx.Result.Result.FeeCharged),
			MaxFee:                int64(tx.Envelope.Fee()),
			Successful:            tx.Result.Successful(),
			TransactionResultCode: tx.Result.Result.Result.Code.String(),
			OperationCount:        len(tx.Envelope.Operations()),
			CreatedAt:             closedAt,
			AccountSequence:       int64(tx.Envelope.SeqNum()),
			LedgerRange:           ledgerRange,
			SignaturesCount:       len(tx.Envelope.Signatures()),
			NewAccount:            false,
			EraID:                 input.EraID,
		}

		// Extract timebounds. Stored as BIGINT in v3_bronze_schema.sql, so
		// we keep them as int64 (matching PG/DuckLake) instead of formatting
		// to a string.
		if tb := tx.Envelope.TimeBounds(); tb != nil {
			minTime := int64(tb.MinTime)
			txData.TimeboundsMinTime = &minTime
			maxTime := int64(tb.MaxTime)
			txData.TimeboundsMaxTime = &maxTime
		}

		// Extract Soroban host function type and contract ID from operations
		for _, op := range tx.Envelope.Operations() {
			if op.Body.Type == xdr.OperationTypeInvokeHostFunction {
				if invokeOp, ok := op.Body.GetInvokeHostFunctionOp(); ok {
					fnType := invokeOp.HostFunction.Type.String()
					txData.SorobanHostFunctionType = &fnType
					// Extract contract ID from soroban meta if available
					if invokeOp.HostFunction.Type == xdr.HostFunctionTypeHostFunctionTypeInvokeContract && invokeOp.HostFunction.InvokeContract != nil {
						contractIDStr, err := invokeOp.HostFunction.InvokeContract.ContractAddress.String()
						if err == nil && contractIDStr != "" {
							txData.SorobanContractID = &contractIDStr
						}
					}
				}
				break // Only need the first InvokeHostFunction op
			}
		}

		// Extract memo
		memo := tx.Envelope.Memo()
		switch memo.Type {
		case xdr.MemoTypeMemoNone:
			memoType := "none"
			txData.MemoType = &memoType
		case xdr.MemoTypeMemoText:
			memoType := "text"
			txData.MemoType = &memoType
			if text, ok := memo.GetText(); ok {
				txData.Memo = &text
			}
		case xdr.MemoTypeMemoId:
			memoType := "id"
			txData.MemoType = &memoType
			if id, ok := memo.GetId(); ok {
				memoStr := fmt.Sprintf("%d", id)
				txData.Memo = &memoStr
			}
		case xdr.MemoTypeMemoHash:
			memoType := "hash"
			txData.MemoType = &memoType
			if hash, ok := memo.GetHash(); ok {
				memoStr := hex.EncodeToString(hash[:])
				txData.Memo = &memoStr
			}
		case xdr.MemoTypeMemoReturn:
			memoType := "return"
			txData.MemoType = &memoType
			if ret, ok := memo.GetRetHash(); ok {
				memoStr := hex.EncodeToString(ret[:])
				txData.Memo = &memoStr
			}
		}

		// Check for CREATE_ACCOUNT operation
		for _, op := range tx.Envelope.Operations() {
			if op.Body.Type == xdr.OperationTypeCreateAccount {
				txData.NewAccount = true
				break
			}
		}

		// Extract Soroban rent fee charged (C13)
		if tx.UnsafeMeta.V == 3 {
			v3 := tx.UnsafeMeta.MustV3()
			if v3.SorobanMeta != nil && v3.SorobanMeta.Ext.V == 1 && v3.SorobanMeta.Ext.V1 != nil {
				rentFee := int64(v3.SorobanMeta.Ext.V1.RentFeeCharged)
				txData.RentFeeCharged = &rentFee
			}
		}

		// Extract Soroban resource fields from envelope
		if instructions, ok := tx.SorobanResourcesInstructions(); ok {
			val := int64(instructions)
			txData.SorobanResourcesInstructions = &val
		}
		if readBytes, ok := tx.SorobanResourcesDiskReadBytes(); ok {
			val := int64(readBytes)
			txData.SorobanResourcesReadBytes = &val
		}
		if writeBytes, ok := tx.SorobanResourcesWriteBytes(); ok {
			val := int64(writeBytes)
			txData.SorobanResourcesWriteBytes = &val
		}

		// Compute TOID
		txData.TransactionID = toid.New(int32(ledgerSeq), txIndex, 0).ToInt64()

		transactions = append(transactions, txData)
		txIndex++
	}

	return transactions, nil
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

// decomposeAsset splits an XDR asset into type, code, and issuer components.
func decomposeAsset(asset xdr.Asset) (assetType, assetCode, assetIssuer string) {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "native", "", ""
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		if a4, ok := asset.GetAlphaNum4(); ok {
			code := strings.TrimRight(string(a4.AssetCode[:]), "\x00")
			return "credit_alphanum4", code, a4.Issuer.Address()
		}
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		if a12, ok := asset.GetAlphaNum12(); ok {
			code := strings.TrimRight(string(a12.AssetCode[:]), "\x00")
			return "credit_alphanum12", code, a12.Issuer.Address()
		}
	}
	return "", "", ""
}

// setAssetFields populates decomposed asset fields on an OperationData from an XDR asset.
func setAssetFields(opData *OperationData, asset xdr.Asset) {
	canonical := asset.StringCanonical()
	opData.Asset = &canonical
	aType, aCode, aIssuer := decomposeAsset(asset)
	opData.AssetType = &aType
	if aCode != "" {
		opData.AssetCode = &aCode
	}
	if aIssuer != "" {
		opData.AssetIssuer = &aIssuer
	}
}

// setSourceAssetFields populates source asset fields on an OperationData from an XDR asset.
func setSourceAssetFields(opData *OperationData, asset xdr.Asset) {
	canonical := asset.StringCanonical()
	opData.SourceAsset = &canonical
	aType, aCode, aIssuer := decomposeAsset(asset)
	opData.SourceAssetType = &aType
	if aCode != "" {
		opData.SourceAssetCode = &aCode
	}
	if aIssuer != "" {
		opData.SourceAssetIssuer = &aIssuer
	}
}

// setBuyingAssetFields populates buying asset fields on an OperationData from an XDR asset.
func setBuyingAssetFields(opData *OperationData, asset xdr.Asset) {
	canonical := asset.StringCanonical()
	opData.BuyingAsset = &canonical
	aType, aCode, aIssuer := decomposeAsset(asset)
	opData.BuyingAssetType = &aType
	if aCode != "" {
		opData.BuyingAssetCode = &aCode
	}
	if aIssuer != "" {
		opData.BuyingAssetIssuer = &aIssuer
	}
}

// setSellingAssetFields populates selling asset fields on an OperationData from an XDR asset.
func setSellingAssetFields(opData *OperationData, asset xdr.Asset) {
	canonical := asset.StringCanonical()
	opData.SellingAsset = &canonical
	aType, aCode, aIssuer := decomposeAsset(asset)
	opData.SellingAssetType = &aType
	if aCode != "" {
		opData.SellingAssetCode = &aCode
	}
	if aIssuer != "" {
		opData.SellingAssetIssuer = &aIssuer
	}
}

// getMuxedAddress returns the muxed account string if the account is muxed, nil otherwise.
func getMuxedAddress(account xdr.MuxedAccount) *string {
	if account.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
		addr := account.Address()
		return &addr
	}
	return nil
}

// getAssetString returns the canonical string representation of an XDR asset.
func getAssetString(asset xdr.Asset) string {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "native"
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		if a4, ok := asset.GetAlphaNum4(); ok {
			code := string(a4.AssetCode[:])
			issuer := a4.Issuer.Address()
			return fmt.Sprintf("%s:%s", code, issuer)
		}
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		if a12, ok := asset.GetAlphaNum12(); ok {
			code := string(a12.AssetCode[:])
			issuer := a12.Issuer.Address()
			return fmt.Sprintf("%s:%s", code, issuer)
		}
	}
	return ""
}

// marshalToBase64 encodes an XDR-marshallable value to a base64 string.
func marshalToBase64(v interface{ MarshalBinary() ([]byte, error) }) *string {
	if bytes, err := v.MarshalBinary(); err == nil {
		encoded := base64.StdEncoding.EncodeToString(bytes)
		return &encoded
	}
	return nil
}

// extractContractInvocationDetails extracts contract ID, function name, and arguments
// from an InvokeHostFunction operation.
func extractContractInvocationDetails(op xdr.Operation) (*string, *string, *string, error) {
	invokeOp, ok := op.Body.GetInvokeHostFunctionOp()
	if !ok {
		return nil, nil, nil, nil
	}

	if invokeOp.HostFunction.Type != xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
		return nil, nil, nil, nil
	}

	if invokeOp.HostFunction.InvokeContract == nil {
		return nil, nil, nil, nil
	}

	invokeContract := invokeOp.HostFunction.InvokeContract

	// Extract contract address
	var contractID *string
	contractIDStr, err := invokeContract.ContractAddress.String()
	if err == nil && contractIDStr != "" {
		contractID = &contractIDStr
	}

	// Extract function name
	var functionName *string
	if invokeContract.FunctionName != "" {
		fnName := string(invokeContract.FunctionName)
		functionName = &fnName
	}

	// Extract arguments - encode each ScVal as base64 XDR for portability
	// (avoids dependency on the full ScVal-to-JSON converter)
	args := invokeContract.Args
	var argsJSON []interface{}
	for _, arg := range args {
		argBytes, marshalErr := arg.MarshalBinary()
		if marshalErr != nil {
			log.Printf("Warning: Failed to marshal ScVal arg: %v", marshalErr)
			argsJSON = append(argsJSON, map[string]interface{}{
				"error": marshalErr.Error(),
				"type":  arg.Type.String(),
			})
		} else {
			argsJSON = append(argsJSON, map[string]interface{}{
				"type":    arg.Type.String(),
				"xdr_b64": base64.StdEncoding.EncodeToString(argBytes),
			})
		}
	}

	var argsStr *string
	if len(argsJSON) > 0 {
		argsJSONBytes, marshalErr := json.Marshal(argsJSON)
		if marshalErr != nil {
			return contractID, functionName, nil, fmt.Errorf("failed to marshal arguments to JSON: %w", marshalErr)
		}
		s := string(argsJSONBytes)
		argsStr = &s
	}

	return contractID, functionName, argsStr, nil
}

// integrateCallGraph is a no-op stub to avoid pulling in the full call_graph.go dependency.
// Cross-contract call graph extraction can be added later if needed.
func integrateCallGraph(_ ingest.LedgerTransaction, _ int, _ xdr.Operation, _ *OperationData) error {
	return nil
}
