package extract

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ExtractOperations extracts operation data from a pre-decoded ledger.
func ExtractOperations(input *LedgerInput) ([]OperationData, error) {
	lcm := input.LCM
	ledgerSeq := input.Sequence
	closedAt := input.ClosedAt
	ledgerRange := input.LedgerRange

	var operations []OperationData

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
			log.Printf("Error reading transaction for operations in ledger %d: %v", ledgerSeq, err)
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])
		txSuccessful := tx.Result.Successful()
		txTOID := toid.New(int32(ledgerSeq), txIndex, 0).ToInt64()

		for i, op := range tx.Envelope.Operations() {
			// Get source account (defaults to transaction source if not specified)
			sourceAccount := tx.Envelope.SourceAccount().ToAccountId().Address()
			var sourceAccountMuxed *string
			if op.SourceAccount != nil {
				sourceAccount = op.SourceAccount.ToAccountId().Address()
				sourceAccountMuxed = getMuxedAddress(*op.SourceAccount)
			} else {
				sourceAccountMuxed = getMuxedAddress(tx.Envelope.SourceAccount())
			}

			opData := OperationData{
				TransactionHash:       txHash,
				TransactionIndex:      int(txIndex),
				OperationIndex:        i,
				LedgerSequence:        ledgerSeq,
				SourceAccount:         sourceAccount,
				SourceAccountMuxed:    sourceAccountMuxed,
				OpType:                int(op.Body.Type),
				TypeString:            op.Body.Type.String(),
				CreatedAt:             closedAt,
				TransactionSuccessful: txSuccessful,
				LedgerRange:           ledgerRange,
				TransactionID:         txTOID,
				OperationID:           toid.New(int32(ledgerSeq), txIndex, int32(i+1)).ToInt64(),
				EraID:                 input.EraID,
			}

			// Extract operation-specific fields
			switch op.Body.Type {
			case xdr.OperationTypeCreateAccount:
				if createAcct, ok := op.Body.GetCreateAccountOp(); ok {
					startBal := int64(createAcct.StartingBalance)
					opData.StartingBalance = &startBal
					opData.Amount = &startBal

					dest := createAcct.Destination.Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypePayment:
				if payment, ok := op.Body.GetPaymentOp(); ok {
					amount := int64(payment.Amount)
					opData.Amount = &amount
					setAssetFields(&opData, payment.Asset)

					dest := payment.Destination.ToAccountId().Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypePathPaymentStrictReceive:
				if pp, ok := op.Body.GetPathPaymentStrictReceiveOp(); ok {
					amount := int64(pp.DestAmount)
					opData.Amount = &amount
					setAssetFields(&opData, pp.DestAsset)

					srcAmt := int64(pp.SendMax)
					opData.SourceAmount = &srcAmt
					setSourceAssetFields(&opData, pp.SendAsset)

					dest := pp.Destination.ToAccountId().Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypeManageSellOffer:
				if sellOffer, ok := op.Body.GetManageSellOfferOp(); ok {
					amount := int64(sellOffer.Amount)
					opData.Amount = &amount
					setSellingAssetFields(&opData, sellOffer.Selling)
					setBuyingAssetFields(&opData, sellOffer.Buying)
					offerID := int64(sellOffer.OfferId)
					opData.OfferID = &offerID
					price := fmt.Sprintf("%d/%d", sellOffer.Price.N, sellOffer.Price.D)
					opData.Price = &price
					priceR := fmt.Sprintf("{\"n\":%d,\"d\":%d}", sellOffer.Price.N, sellOffer.Price.D)
					opData.PriceR = &priceR
				}

			case xdr.OperationTypeCreatePassiveSellOffer:
				if passiveOffer, ok := op.Body.GetCreatePassiveSellOfferOp(); ok {
					amount := int64(passiveOffer.Amount)
					opData.Amount = &amount
					setSellingAssetFields(&opData, passiveOffer.Selling)
					setBuyingAssetFields(&opData, passiveOffer.Buying)
					price := fmt.Sprintf("%d/%d", passiveOffer.Price.N, passiveOffer.Price.D)
					opData.Price = &price
					priceR := fmt.Sprintf("{\"n\":%d,\"d\":%d}", passiveOffer.Price.N, passiveOffer.Price.D)
					opData.PriceR = &priceR
				}

			case xdr.OperationTypeSetOptions:
				if setOpts, ok := op.Body.GetSetOptionsOp(); ok {
					if setOpts.HomeDomain != nil {
						hd := string(*setOpts.HomeDomain)
						opData.HomeDomain = &hd
					}
					if setOpts.SetFlags != nil {
						sf := int(*setOpts.SetFlags)
						opData.SetFlags = &sf
					}
					if setOpts.ClearFlags != nil {
						cf := int(*setOpts.ClearFlags)
						opData.ClearFlags = &cf
					}
					if setOpts.MasterWeight != nil {
						mw := int(*setOpts.MasterWeight)
						opData.MasterWeight = &mw
					}
					if setOpts.LowThreshold != nil {
						lt := int(*setOpts.LowThreshold)
						opData.LowThreshold = &lt
					}
					if setOpts.MedThreshold != nil {
						mt := int(*setOpts.MedThreshold)
						opData.MediumThreshold = &mt
					}
					if setOpts.HighThreshold != nil {
						ht := int(*setOpts.HighThreshold)
						opData.HighThreshold = &ht
					}
				}

			case xdr.OperationTypeChangeTrust:
				if changeTrust, ok := op.Body.GetChangeTrustOp(); ok {
					limit := int64(changeTrust.Limit)
					opData.TrustlineLimit = &limit
					// ChangeTrust line can be Asset or LiquidityPoolParameters
					if changeTrust.Line.Type == xdr.AssetTypeAssetTypePoolShare {
						assetStr := "liquidity_pool_shares"
						opData.Asset = &assetStr
						aType := "liquidity_pool_shares"
						opData.AssetType = &aType
					} else {
						asset := changeTrust.Line.ToAsset()
						setAssetFields(&opData, asset)
					}
				}

			case xdr.OperationTypeAllowTrust:
				if allowTrust, ok := op.Body.GetAllowTrustOp(); ok {
					dest := allowTrust.Trustor.Address()
					opData.Destination = &dest
					// Extract asset code from AssetCode union
					var code string
					if ac4, ok := allowTrust.Asset.GetAssetCode4(); ok {
						code = strings.TrimRight(string(ac4[:]), "\x00")
					} else if ac12, ok := allowTrust.Asset.GetAssetCode12(); ok {
						code = strings.TrimRight(string(ac12[:]), "\x00")
					}
					if code != "" {
						opData.AssetCode = &code
					}
					flags := int(allowTrust.Authorize)
					opData.SetFlags = &flags
				}

			case xdr.OperationTypeAccountMerge:
				// AccountMerge destination is in the body itself
				if destAccount, ok := op.Body.GetDestination(); ok {
					dest := destAccount.ToAccountId().Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypeManageData:
				if manageData, ok := op.Body.GetManageDataOp(); ok {
					name := string(manageData.DataName)
					opData.DataName = &name
					if manageData.DataValue != nil {
						val := base64.StdEncoding.EncodeToString(*manageData.DataValue)
						opData.DataValue = &val
					}
				}

			case xdr.OperationTypeBumpSequence:
				if bumpSeq, ok := op.Body.GetBumpSequenceOp(); ok {
					bumpTo := int64(bumpSeq.BumpTo)
					opData.BumpTo = &bumpTo
				}

			case xdr.OperationTypeManageBuyOffer:
				if buyOffer, ok := op.Body.GetManageBuyOfferOp(); ok {
					amount := int64(buyOffer.BuyAmount)
					opData.Amount = &amount
					setSellingAssetFields(&opData, buyOffer.Selling)
					setBuyingAssetFields(&opData, buyOffer.Buying)
					offerID := int64(buyOffer.OfferId)
					opData.OfferID = &offerID
					price := fmt.Sprintf("%d/%d", buyOffer.Price.N, buyOffer.Price.D)
					opData.Price = &price
					priceR := fmt.Sprintf("{\"n\":%d,\"d\":%d}", buyOffer.Price.N, buyOffer.Price.D)
					opData.PriceR = &priceR
				}

			case xdr.OperationTypePathPaymentStrictSend:
				if pp, ok := op.Body.GetPathPaymentStrictSendOp(); ok {
					amount := int64(pp.SendAmount)
					opData.Amount = &amount
					setSourceAssetFields(&opData, pp.SendAsset)
					setAssetFields(&opData, pp.DestAsset)

					destMin := int64(pp.DestMin)
					opData.DestinationMin = &destMin

					dest := pp.Destination.ToAccountId().Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypeCreateClaimableBalance:
				if createCB, ok := op.Body.GetCreateClaimableBalanceOp(); ok {
					amount := int64(createCB.Amount)
					opData.Amount = &amount
					setAssetFields(&opData, createCB.Asset)
				}

			case xdr.OperationTypeClaimClaimableBalance:
				if claimCB, ok := op.Body.GetClaimClaimableBalanceOp(); ok {
					balanceID, err := xdr.MarshalHex(claimCB.BalanceId)
					if err == nil {
						opData.BalanceID = &balanceID
					}
				}

			case xdr.OperationTypeBeginSponsoringFutureReserves:
				if beginSponsoring, ok := op.Body.GetBeginSponsoringFutureReservesOp(); ok {
					sponsored := beginSponsoring.SponsoredId.Address()
					opData.SponsoredID = &sponsored
				}

			case xdr.OperationTypeEndSponsoringFutureReserves:
				// No fields to extract

			case xdr.OperationTypeRevokeSponsorship:
				// Revoke sponsorship - complex type, just record the op type

			case xdr.OperationTypeClawback:
				if clawback, ok := op.Body.GetClawbackOp(); ok {
					amount := int64(clawback.Amount)
					opData.Amount = &amount
					setAssetFields(&opData, clawback.Asset)
					dest := clawback.From.ToAccountId().Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypeSetTrustLineFlags:
				if setTLFlags, ok := op.Body.GetSetTrustLineFlagsOp(); ok {
					dest := setTLFlags.Trustor.Address()
					opData.Destination = &dest
					setAssetFields(&opData, setTLFlags.Asset)
					sf := int(setTLFlags.SetFlags)
					opData.SetFlags = &sf
					cf := int(setTLFlags.ClearFlags)
					opData.ClearFlags = &cf
				}

			case xdr.OperationTypeLiquidityPoolDeposit:
				if lpDeposit, ok := op.Body.GetLiquidityPoolDepositOp(); ok {
					amount := int64(lpDeposit.MaxAmountA)
					opData.Amount = &amount
				}

			case xdr.OperationTypeLiquidityPoolWithdraw:
				if lpWithdraw, ok := op.Body.GetLiquidityPoolWithdrawOp(); ok {
					amount := int64(lpWithdraw.Amount)
					opData.Amount = &amount
				}

			case xdr.OperationTypeExtendFootprintTtl:
				// ExtendFootprintTtl - no operation-specific extraction needed

			case xdr.OperationTypeRestoreFootprint:
				// RestoreFootprint - no operation-specific extraction needed
			}

			// Get operation result code if available
			if txSuccessful {
				if opResults, ok := tx.Result.Result.OperationResults(); ok {
					if i < len(opResults) {
						resultCode := opResults[i].Code.String()
						opData.OperationResultCode = &resultCode
					}
				}
			}

			// Extract contract invocation details for InvokeHostFunction operations (type 24)
			if op.Body.Type == xdr.OperationTypeInvokeHostFunction {
				if invokeOp, ok := op.Body.GetInvokeHostFunctionOp(); ok {
					fnType := invokeOp.HostFunction.Type.String()
					opData.SorobanOperation = &fnType
					// Check if auth is required
					authRequired := len(invokeOp.Auth) > 0
					opData.SorobanAuthRequired = &authRequired
				}
				contractID, functionName, argsJSON, err := extractContractInvocationDetails(op)
				if err != nil {
					log.Printf("Warning: Failed to extract contract invocation details for op %s:%d: %v", txHash, i, err)
				}
				opData.SorobanContractID = contractID
				opData.SorobanFunction = functionName
				opData.SorobanArgumentsJSON = argsJSON

				// Call graph extraction (stubbed out to avoid call_graph.go dependency)
				if err := integrateCallGraph(tx, i, op, &opData); err != nil {
					log.Printf("Warning: Failed to integrate call graph for op %s:%d: %v", txHash, i, err)
				}
			}

			operations = append(operations, opData)
		}

		txIndex++
	}

	return operations, nil
}
