package extract

import (
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ExtractTrades extracts trades from a pre-decoded ledger.
// Extracts trade data from MANAGE_SELL_OFFER, MANAGE_BUY_OFFER, and CREATE_PASSIVE_SELL_OFFER operations.
func ExtractTrades(input *LedgerInput) ([]TradeData, error) {
	lcm := input.LCM
	ledgerSeq := input.Sequence
	closedAt := input.ClosedAt
	ledgerRange := input.LedgerRange

	var trades []TradeData

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(input.NetworkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer reader.Close()

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for trades in ledger %d: %v", ledgerSeq, err)
			continue
		}

		if !tx.Result.Successful() {
			continue // Only process successful transactions
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])

		for opIdx, op := range tx.Envelope.Operations() {
			if opResults, ok := tx.Result.Result.OperationResults(); ok {
				if opIdx >= len(opResults) {
					continue
				}
				opResult := opResults[opIdx]

				tradeIndex := 0
				switch op.Body.Type {
				case xdr.OperationTypeManageSellOffer, xdr.OperationTypeManageBuyOffer,
					xdr.OperationTypeCreatePassiveSellOffer:

					var offerResult *xdr.ManageOfferSuccessResult
					switch opResult.Code {
					case xdr.OperationResultCodeOpInner:
						tr := opResult.MustTr()
						switch tr.Type {
						case xdr.OperationTypeManageSellOffer:
							if r, ok := tr.GetManageSellOfferResult(); ok && r.Code == xdr.ManageSellOfferResultCodeManageSellOfferSuccess {
								result := r.MustSuccess()
								offerResult = &result
							}
						case xdr.OperationTypeManageBuyOffer:
							if r, ok := tr.GetManageBuyOfferResult(); ok && r.Code == xdr.ManageBuyOfferResultCodeManageBuyOfferSuccess {
								result := r.MustSuccess()
								offerResult = &result
							}
						case xdr.OperationTypeCreatePassiveSellOffer:
							if r, ok := tr.GetCreatePassiveSellOfferResult(); ok && r.Code == xdr.ManageSellOfferResultCodeManageSellOfferSuccess {
								result := r.MustSuccess()
								offerResult = &result
							}
						}
					}

					if offerResult != nil {
						for _, claimAtom := range offerResult.OffersClaimed {
							var sellerAccount, sellingAmount, buyingAmount string
							var sellingCode, sellingIssuer, buyingCode, buyingIssuer *string

							switch claimAtom.Type {
							case xdr.ClaimAtomTypeClaimAtomTypeOrderBook:
								ob := claimAtom.MustOrderBook()
								sellerAccount = ob.SellerId.Address()
								sellingAmount = fmt.Sprintf("%d", ob.AmountSold)
								buyingAmount = fmt.Sprintf("%d", ob.AmountBought)

								// Parse selling asset
								if ob.AssetSold.Type != xdr.AssetTypeAssetTypeNative {
									switch ob.AssetSold.Type {
									case xdr.AssetTypeAssetTypeCreditAlphanum4:
										a4 := ob.AssetSold.MustAlphaNum4()
										code := strings.TrimRight(string(a4.AssetCode[:]), "\x00")
										issuer := a4.Issuer.Address()
										sellingCode = &code
										sellingIssuer = &issuer
									case xdr.AssetTypeAssetTypeCreditAlphanum12:
										a12 := ob.AssetSold.MustAlphaNum12()
										code := strings.TrimRight(string(a12.AssetCode[:]), "\x00")
										issuer := a12.Issuer.Address()
										sellingCode = &code
										sellingIssuer = &issuer
									}
								}

								// Parse buying asset
								if ob.AssetBought.Type != xdr.AssetTypeAssetTypeNative {
									switch ob.AssetBought.Type {
									case xdr.AssetTypeAssetTypeCreditAlphanum4:
										a4 := ob.AssetBought.MustAlphaNum4()
										code := strings.TrimRight(string(a4.AssetCode[:]), "\x00")
										issuer := a4.Issuer.Address()
										buyingCode = &code
										buyingIssuer = &issuer
									case xdr.AssetTypeAssetTypeCreditAlphanum12:
										a12 := ob.AssetBought.MustAlphaNum12()
										code := strings.TrimRight(string(a12.AssetCode[:]), "\x00")
										issuer := a12.Issuer.Address()
										buyingCode = &code
										buyingIssuer = &issuer
									}
								}

							case xdr.ClaimAtomTypeClaimAtomTypeV0:
								v0 := claimAtom.MustV0()
								sellerAccount = fmt.Sprintf("%x", v0.SellerEd25519)
								sellingAmount = fmt.Sprintf("%d", v0.AmountSold)
								buyingAmount = fmt.Sprintf("%d", v0.AmountBought)
							}

							buyerAccount := tx.Envelope.SourceAccount().ToAccountId().Address()

							trades = append(trades, TradeData{
								LedgerSequence:     ledgerSeq,
								TransactionHash:    txHash,
								OperationIndex:     opIdx,
								TradeIndex:         tradeIndex,
								TradeType:          "orderbook",
								TradeTimestamp:     closedAt,
								SellerAccount:      sellerAccount,
								SellingAssetCode:   sellingCode,
								SellingAssetIssuer: sellingIssuer,
								SellingAmount:      sellingAmount,
								BuyerAccount:       buyerAccount,
								BuyingAssetCode:    buyingCode,
								BuyingAssetIssuer:  buyingIssuer,
								BuyingAmount:       buyingAmount,
								Price:              fmt.Sprintf("%s/%s", buyingAmount, sellingAmount),
								CreatedAt:          closedAt,
								LedgerRange:        ledgerRange,
								EraID:              input.EraID,
							})
							tradeIndex++
						}
					}
				}
			}
		}
	}

	return trades, nil
}
