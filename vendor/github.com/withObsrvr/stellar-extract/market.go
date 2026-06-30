package extract

import (
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ExtractOffers extracts offer snapshots from a ledger.
// Pattern: LedgerChangeReader + Filter by LedgerEntryTypeOffer + Map deduplication.
func ExtractOffers(input *LedgerInput) ([]OfferData, error) {
	var offers []OfferData

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(input.NetworkPassphrase, input.LCM)
	if err != nil {
		log.Printf("Failed to create change reader for offers: %v", err)
		return offers, nil
	}
	defer reader.Close()

	offerMap := make(map[int64]*OfferData)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for offers: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeOffer {
			continue
		}

		var offerEntry *xdr.OfferEntry
		if change.Post != nil {
			if oe, ok := change.Post.Data.GetOffer(); ok {
				offerEntry = &oe
			}
		}

		if offerEntry == nil {
			continue
		}

		offerData := extractOfferDataFromEntry(offerEntry, input.Sequence, input.ClosedAt, input.LedgerRange, input.EraID)
		offerCopy := offerData
		offerMap[offerData.OfferID] = &offerCopy
	}

	for _, offer := range offerMap {
		offers = append(offers, *offer)
	}

	return offers, nil
}

// extractOfferDataFromEntry extracts all fields from an OfferEntry.
func extractOfferDataFromEntry(entry *xdr.OfferEntry, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32, eraID *string) OfferData {
	offerID := int64(entry.OfferId)
	sellerAccount := entry.SellerId.Address()

	sellingType, sellingCode, sellingIssuer := parseAsset(entry.Selling)
	buyingType, buyingCode, buyingIssuer := parseAsset(entry.Buying)

	amount := strconv.FormatInt(int64(entry.Amount), 10)
	priceStr := fmt.Sprintf("%d/%d", entry.Price.N, entry.Price.D)
	flags := uint32(entry.Flags)

	now := time.Now().UTC()

	return OfferData{
		OfferID:        offerID,
		SellerAccount:  sellerAccount,
		LedgerSequence: ledgerSeq,
		ClosedAt:       closedAt,

		SellingAssetType:   sellingType,
		SellingAssetCode:   sellingCode,
		SellingAssetIssuer: sellingIssuer,

		BuyingAssetType:   buyingType,
		BuyingAssetCode:   buyingCode,
		BuyingAssetIssuer: buyingIssuer,

		Amount: amount,
		Price:  priceStr,
		Flags:  flags,

		CreatedAt:   now,
		LedgerRange: ledgerRange,
		EraID:       eraID,
	}
}

// parseAsset extracts asset type, code, and issuer from xdr.Asset.
// Returns (type, code, issuer) where code and issuer are nullable.
func parseAsset(asset xdr.Asset) (assetType string, code *string, issuer *string) {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "native", nil, nil

	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		a4 := asset.MustAlphaNum4()
		c := strings.TrimRight(string(a4.AssetCode[:]), "\x00")
		i := a4.Issuer.Address()
		return "credit_alphanum4", &c, &i

	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		a12 := asset.MustAlphaNum12()
		c := strings.TrimRight(string(a12.AssetCode[:]), "\x00")
		i := a12.Issuer.Address()
		return "credit_alphanum12", &c, &i

	case xdr.AssetTypeAssetTypePoolShare:
		poolType := "liquidity_pool_shares"
		poolCode := "POOL_SHARE"
		poolIssuer := "pool"
		return poolType, &poolCode, &poolIssuer

	default:
		return "unknown", nil, nil
	}
}

// ExtractClaimableBalances extracts claimable balance state from a ledger.
// Protocol 14+ feature for deferred payments and multi-party asset distribution.
func ExtractClaimableBalances(input *LedgerInput) ([]ClaimableBalanceData, error) {
	var balances []ClaimableBalanceData

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(input.NetworkPassphrase, input.LCM)
	if err != nil {
		log.Printf("Failed to create change reader for claimable balances: %v", err)
		return balances, nil
	}
	defer reader.Close()

	balanceMap := make(map[string]*ClaimableBalanceData)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for claimable balances: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeClaimableBalance {
			continue
		}

		var balanceEntry *xdr.ClaimableBalanceEntry
		var ledgerEntry *xdr.LedgerEntry
		if change.Post != nil {
			ledgerEntry = change.Post
			if cb, ok := change.Post.Data.GetClaimableBalance(); ok {
				balanceEntry = &cb
			}
		}

		if balanceEntry == nil {
			continue
		}

		var balanceID string
		switch balanceEntry.BalanceId.Type {
		case xdr.ClaimableBalanceIdTypeClaimableBalanceIdTypeV0:
			hashBytes := balanceEntry.BalanceId.MustV0()
			balanceID = hex.EncodeToString(hashBytes[:])
		default:
			log.Printf("Unknown ClaimableBalanceId type: %v", balanceEntry.BalanceId.Type)
			continue
		}

		var sponsor string
		sponsorDesc := ledgerEntry.SponsoringID()
		if sponsorDesc != nil {
			sponsor = sponsorDesc.Address()
		}

		assetType, assetCode, assetIssuer := parseAsset(balanceEntry.Asset)
		amount := int64(balanceEntry.Amount)
		claimantsCount := int32(len(balanceEntry.Claimants))

		now := time.Now().UTC()

		balance := ClaimableBalanceData{
			BalanceID:      balanceID,
			Sponsor:        sponsor,
			LedgerSequence: input.Sequence,
			ClosedAt:       input.ClosedAt,

			AssetType:   assetType,
			AssetCode:   assetCode,
			AssetIssuer: assetIssuer,
			Amount:      amount,

			ClaimantsCount: claimantsCount,
			Flags:          0,

			CreatedAt:   now,
			LedgerRange: input.LedgerRange,
			EraID:       input.EraID,
		}

		balanceMap[balanceID] = &balance
	}

	for _, balance := range balanceMap {
		balances = append(balances, *balance)
	}

	return balances, nil
}

// ExtractLiquidityPools extracts liquidity pool state from a ledger.
// Protocol 18+ feature for AMM (Automated Market Maker) pools.
func ExtractLiquidityPools(input *LedgerInput) ([]LiquidityPoolData, error) {
	var pools []LiquidityPoolData

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(input.NetworkPassphrase, input.LCM)
	if err != nil {
		log.Printf("Failed to create change reader for liquidity pools: %v", err)
		return pools, nil
	}
	defer reader.Close()

	poolMap := make(map[string]*LiquidityPoolData)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for liquidity pools: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeLiquidityPool {
			continue
		}

		var poolEntry *xdr.LiquidityPoolEntry
		if change.Post != nil {
			if lp, ok := change.Post.Data.GetLiquidityPool(); ok {
				poolEntry = &lp
			}
		}

		if poolEntry == nil {
			continue
		}

		poolID := hex.EncodeToString(poolEntry.LiquidityPoolId[:])

		if poolEntry.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
			log.Printf("Unknown liquidity pool type: %v", poolEntry.Body.Type)
			continue
		}

		cp := poolEntry.Body.MustConstantProduct()

		assetAType, assetACode, assetAIssuer := parseAsset(cp.Params.AssetA)
		assetBType, assetBCode, assetBIssuer := parseAsset(cp.Params.AssetB)

		assetAAmount := int64(cp.ReserveA)
		assetBAmount := int64(cp.ReserveB)
		totalPoolShares := int64(cp.TotalPoolShares)
		trustlineCount := int32(cp.PoolSharesTrustLineCount)
		fee := int32(cp.Params.Fee)

		now := time.Now().UTC()

		pool := LiquidityPoolData{
			LiquidityPoolID: poolID,
			LedgerSequence:  input.Sequence,
			ClosedAt:        input.ClosedAt,

			PoolType: "constant_product",
			Fee:      fee,

			TrustlineCount:  trustlineCount,
			TotalPoolShares: totalPoolShares,

			AssetAType:   assetAType,
			AssetACode:   assetACode,
			AssetAIssuer: assetAIssuer,
			AssetAAmount: assetAAmount,

			AssetBType:   assetBType,
			AssetBCode:   assetBCode,
			AssetBIssuer: assetBIssuer,
			AssetBAmount: assetBAmount,

			CreatedAt:   now,
			LedgerRange: input.LedgerRange,
			EraID:       input.EraID,
		}

		poolMap[poolID] = &pool
	}

	for _, pool := range poolMap {
		pools = append(pools, *pool)
	}

	return pools, nil
}
