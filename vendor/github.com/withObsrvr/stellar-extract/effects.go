package extract

// Comprehensive effects extraction adapted from stellar-etl's TransformEffect.
// Covers all 50+ Stellar effect types across all operation types.
// Ported from stellar-history-loader/go/extractors_effects.go.

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/contractevents"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Effect type constants matching Horizon/stellar-etl conventions
const (
	EffectAccountCreated                     = 0
	EffectAccountRemoved                     = 1
	EffectAccountCredited                    = 2
	EffectAccountDebited                     = 3
	EffectAccountThresholdsUpdated           = 4
	EffectAccountHomeDomainUpdated           = 5
	EffectAccountFlagsUpdated                = 6
	EffectAccountInflationDestinationUpdated = 7
	EffectSignerCreated                      = 10
	EffectSignerRemoved                      = 11
	EffectSignerUpdated                      = 12
	EffectTrustlineCreated                   = 20
	EffectTrustlineRemoved                   = 21
	EffectTrustlineUpdated                   = 22
	EffectTrustlineFlagsUpdated              = 26
	EffectOfferCreated                       = 30
	EffectOfferRemoved                       = 31
	EffectOfferUpdated                       = 32
	EffectTrade                              = 33
	EffectDataCreated                        = 40
	EffectDataRemoved                        = 41
	EffectDataUpdated                        = 42
	EffectSequenceBumped                     = 43
	EffectClaimableBalanceCreated            = 50
	EffectClaimableBalanceClaimantCreated    = 51
	EffectClaimableBalanceClaimed            = 52
	EffectAccountSponsorshipCreated          = 60
	EffectAccountSponsorshipUpdated          = 61
	EffectAccountSponsorshipRemoved          = 62
	EffectTrustlineSponsorshipCreated        = 63
	EffectTrustlineSponsorshipUpdated        = 64
	EffectTrustlineSponsorshipRemoved        = 65
	EffectDataSponsorshipCreated             = 66
	EffectDataSponsorshipUpdated             = 67
	EffectDataSponsorshipRemoved             = 68
	EffectClaimableBalanceSponsorshipCreated = 69
	EffectClaimableBalanceSponsorshipUpdated = 70
	EffectClaimableBalanceSponsorshipRemoved = 71
	EffectSignerSponsorshipCreated           = 72
	EffectSignerSponsorshipUpdated           = 73
	EffectSignerSponsorshipRemoved           = 74
	EffectClaimableBalanceClawedBack         = 80
	EffectLiquidityPoolDeposited             = 90
	EffectLiquidityPoolWithdrew              = 91
	EffectLiquidityPoolTrade                 = 92
	EffectLiquidityPoolCreated               = 93
	EffectLiquidityPoolRemoved               = 94
	EffectLiquidityPoolRevoked               = 95
	EffectContractCredited                   = 96
	EffectContractDebited                    = 97
	EffectExtendFootprintTtl                 = 98
	EffectRestoreFootprint                   = 99
)

var effectTypeNames = map[int]string{
	EffectAccountCreated:                     "account_created",
	EffectAccountRemoved:                     "account_removed",
	EffectAccountCredited:                    "account_credited",
	EffectAccountDebited:                     "account_debited",
	EffectAccountThresholdsUpdated:           "account_thresholds_updated",
	EffectAccountHomeDomainUpdated:           "account_home_domain_updated",
	EffectAccountFlagsUpdated:                "account_flags_updated",
	EffectAccountInflationDestinationUpdated: "account_inflation_destination_updated",
	EffectSignerCreated:                      "signer_created",
	EffectSignerRemoved:                      "signer_removed",
	EffectSignerUpdated:                      "signer_updated",
	EffectTrustlineCreated:                   "trustline_created",
	EffectTrustlineRemoved:                   "trustline_removed",
	EffectTrustlineUpdated:                   "trustline_updated",
	EffectTrustlineFlagsUpdated:              "trustline_flags_updated",
	EffectOfferCreated:                       "offer_created",
	EffectOfferRemoved:                       "offer_removed",
	EffectOfferUpdated:                       "offer_updated",
	EffectTrade:                              "trade",
	EffectDataCreated:                        "data_created",
	EffectDataRemoved:                        "data_removed",
	EffectDataUpdated:                        "data_updated",
	EffectSequenceBumped:                     "sequence_bumped",
	EffectClaimableBalanceCreated:            "claimable_balance_created",
	EffectClaimableBalanceClaimantCreated:    "claimable_balance_claimant_created",
	EffectClaimableBalanceClaimed:            "claimable_balance_claimed",
	EffectAccountSponsorshipCreated:          "account_sponsorship_created",
	EffectAccountSponsorshipUpdated:          "account_sponsorship_updated",
	EffectAccountSponsorshipRemoved:          "account_sponsorship_removed",
	EffectTrustlineSponsorshipCreated:        "trustline_sponsorship_created",
	EffectTrustlineSponsorshipUpdated:        "trustline_sponsorship_updated",
	EffectTrustlineSponsorshipRemoved:        "trustline_sponsorship_removed",
	EffectDataSponsorshipCreated:             "data_sponsorship_created",
	EffectDataSponsorshipUpdated:             "data_sponsorship_updated",
	EffectDataSponsorshipRemoved:             "data_sponsorship_removed",
	EffectClaimableBalanceSponsorshipCreated: "claimable_balance_sponsorship_created",
	EffectClaimableBalanceSponsorshipUpdated: "claimable_balance_sponsorship_updated",
	EffectClaimableBalanceSponsorshipRemoved: "claimable_balance_sponsorship_removed",
	EffectSignerSponsorshipCreated:           "signer_sponsorship_created",
	EffectSignerSponsorshipUpdated:           "signer_sponsorship_updated",
	EffectSignerSponsorshipRemoved:           "signer_sponsorship_removed",
	EffectClaimableBalanceClawedBack:         "claimable_balance_clawed_back",
	EffectLiquidityPoolDeposited:             "liquidity_pool_deposited",
	EffectLiquidityPoolWithdrew:              "liquidity_pool_withdrew",
	EffectLiquidityPoolTrade:                 "liquidity_pool_trade",
	EffectLiquidityPoolCreated:               "liquidity_pool_created",
	EffectLiquidityPoolRemoved:               "liquidity_pool_removed",
	EffectLiquidityPoolRevoked:               "liquidity_pool_revoked",
	EffectContractCredited:                   "contract_credited",
	EffectContractDebited:                    "contract_debited",
	EffectExtendFootprintTtl:                 "extend_footprint_ttl",
	EffectRestoreFootprint:                   "restore_footprint",
}

// Internal effect record before conversion to EffectData
type effectRecord struct {
	Address    string
	EffectType int
	Details    map[string]interface{}
}

type opWrapper struct {
	index     uint32
	tx        ingest.LedgerTransaction
	op        xdr.Operation
	ledgerSeq uint32
	network   string
	closedAt  time.Time
}

func (o *opWrapper) sourceAccount() *xdr.MuxedAccount {
	if o.op.SourceAccount != nil {
		return o.op.SourceAccount
	}
	ret := o.tx.Envelope.SourceAccount()
	return &ret
}

func (o *opWrapper) operationResult() *xdr.OperationResultTr {
	results, _ := o.tx.Result.OperationResults()
	tr := results[o.index].MustTr()
	return &tr
}

func (o *opWrapper) id() int64 {
	return toid.New(int32(o.ledgerSeq), int32(o.tx.Index), int32(o.index+1)).ToInt64()
}

type liquidityPoolDelta struct {
	ReserveA        xdr.Int64
	ReserveB        xdr.Int64
	TotalPoolShares xdr.Int64
}

var errLPNotFound = fmt.Errorf("liquidity pool change not found")

func (o *opWrapper) getLiquidityPoolDelta(lpID *xdr.PoolId) (*xdr.LiquidityPoolEntry, *liquidityPoolDelta, error) {
	changes, err := o.tx.GetOperationChanges(o.index)
	if err != nil {
		return nil, nil, err
	}
	for _, c := range changes {
		if c.Type != xdr.LedgerEntryTypeLiquidityPool {
			continue
		}
		var lp *xdr.LiquidityPoolEntry
		var preA, preB, preShares xdr.Int64
		if c.Pre != nil {
			if lpID != nil && c.Pre.Data.LiquidityPool.LiquidityPoolId != *lpID {
				continue
			}
			lp = c.Pre.Data.LiquidityPool
			if lp.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
				return nil, nil, fmt.Errorf("unexpected LP body type %d", lp.Body.Type)
			}
			cp := lp.Body.ConstantProduct
			preA, preB, preShares = cp.ReserveA, cp.ReserveB, cp.TotalPoolShares
		}
		var postA, postB, postShares xdr.Int64
		if c.Post != nil {
			if lpID != nil && c.Post.Data.LiquidityPool.LiquidityPoolId != *lpID {
				continue
			}
			lp = c.Post.Data.LiquidityPool
			if lp.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
				return nil, nil, fmt.Errorf("unexpected LP body type %d", lp.Body.Type)
			}
			cp := lp.Body.ConstantProduct
			postA, postB, postShares = cp.ReserveA, cp.ReserveB, cp.TotalPoolShares
		}
		return lp, &liquidityPoolDelta{
			ReserveA:        postA - preA,
			ReserveB:        postB - preB,
			TotalPoolShares: postShares - preShares,
		}, nil
	}
	return nil, nil, errLPNotFound
}

// effects accumulator
type efx struct {
	records []effectRecord
	op      *opWrapper
}

func (e *efx) add(address string, effectType int, details map[string]interface{}) {
	e.records = append(e.records, effectRecord{Address: address, EffectType: effectType, Details: details})
}

func (e *efx) addMuxed(account *xdr.MuxedAccount, effectType int, details map[string]interface{}) {
	accID := account.ToAccountId()
	e.add(accID.Address(), effectType, details)
}

func (e *efx) addUnmuxed(account *xdr.AccountId, effectType int, details map[string]interface{}) {
	e.add(account.Address(), effectType, details)
}

// Helper: add asset details to a map
func addAssetDetails(result map[string]interface{}, a xdr.Asset, prefix string) {
	var assetType, code, issuer string
	_ = a.Extract(&assetType, &code, &issuer)
	result[prefix+"asset_type"] = assetType
	if a.Type != xdr.AssetTypeAssetTypeNative {
		result[prefix+"asset_code"] = code
		result[prefix+"asset_issuer"] = issuer
	}
}

func poolIDToString(id xdr.PoolId) string {
	return xdr.Hash(id).HexString()
}

func liquidityPoolDetails(lp *xdr.LiquidityPoolEntry) map[string]interface{} {
	cp := lp.Body.ConstantProduct
	return map[string]interface{}{
		"id":               poolIDToString(lp.LiquidityPoolId),
		"fee_bp":           uint32(cp.Params.Fee),
		"type":             "constant_product",
		"total_trustlines": strconv.FormatInt(int64(cp.PoolSharesTrustLineCount), 10),
		"total_shares":     amount.String(cp.TotalPoolShares),
		"reserves": []map[string]string{
			{"asset": cp.Params.AssetA.StringCanonical(), "amount": amount.String(cp.ReserveA)},
			{"asset": cp.Params.AssetB.StringCanonical(), "amount": amount.String(cp.ReserveB)},
		},
	}
}

func setAuthFlagDetails(details map[string]interface{}, flags xdr.AccountFlags, setValue bool) {
	if flags.IsAuthRequired() {
		details["auth_required_flag"] = setValue
	}
	if flags.IsAuthRevocable() {
		details["auth_revocable_flag"] = setValue
	}
	if flags.IsAuthImmutable() {
		details["auth_immutable_flag"] = setValue
	}
	if flags.IsAuthClawbackEnabled() {
		details["auth_clawback_enabled_flag"] = setValue
	}
}

func setTrustLineFlagDetails(details map[string]interface{}, flags xdr.TrustLineFlags, setValue bool) {
	if flags.IsAuthorized() {
		details["authorized_flag"] = setValue
	}
	if flags.IsAuthorizedToMaintainLiabilitiesFlag() {
		details["authorized_to_maintain_liabilities"] = setValue
	}
	if flags.IsClawbackEnabledFlag() {
		details["clawback_enabled_flag"] = setValue
	}
}

// ---- Per-operation effect generators ----

func (e *efx) addAccountCreatedEffects() {
	op := e.op.op.Body.MustCreateAccountOp()
	e.addUnmuxed(&op.Destination, EffectAccountCreated, map[string]interface{}{
		"starting_balance": amount.String(op.StartingBalance),
	})
	e.addMuxed(e.op.sourceAccount(), EffectAccountDebited, map[string]interface{}{
		"asset_type": "native", "amount": amount.String(op.StartingBalance),
	})
	e.addUnmuxed(&op.Destination, EffectSignerCreated, map[string]interface{}{
		"public_key": op.Destination.Address(), "weight": keypair.DefaultSignerWeight,
	})
}

func (e *efx) addPaymentEffects() {
	op := e.op.op.Body.MustPaymentOp()
	details := map[string]interface{}{"amount": amount.String(op.Amount)}
	addAssetDetails(details, op.Asset, "")
	e.addMuxed(&op.Destination, EffectAccountCredited, details)
	debitDetails := map[string]interface{}{"amount": amount.String(op.Amount)}
	addAssetDetails(debitDetails, op.Asset, "")
	e.addMuxed(e.op.sourceAccount(), EffectAccountDebited, debitDetails)
}

func (e *efx) addPathPaymentStrictReceiveEffects() {
	op := e.op.op.Body.MustPathPaymentStrictReceiveOp()
	result := e.op.operationResult().MustPathPaymentStrictReceiveResult()
	resultSuccess := result.MustSuccess()

	creditDetails := map[string]interface{}{"amount": amount.String(op.DestAmount)}
	addAssetDetails(creditDetails, op.DestAsset, "")
	e.addMuxed(&op.Destination, EffectAccountCredited, creditDetails)

	debitDetails := map[string]interface{}{"amount": amount.String(result.SendAmount())}
	addAssetDetails(debitDetails, op.SendAsset, "")
	e.addMuxed(e.op.sourceAccount(), EffectAccountDebited, debitDetails)

	e.addTradeEffects(*e.op.sourceAccount(), resultSuccess.Offers)
}

func (e *efx) addPathPaymentStrictSendEffects() {
	op := e.op.op.Body.MustPathPaymentStrictSendOp()
	result := e.op.operationResult().MustPathPaymentStrictSendResult()
	resultSuccess := result.MustSuccess()

	creditDetails := map[string]interface{}{"amount": amount.String(result.DestAmount())}
	addAssetDetails(creditDetails, op.DestAsset, "")
	e.addMuxed(&op.Destination, EffectAccountCredited, creditDetails)

	debitDetails := map[string]interface{}{"amount": amount.String(op.SendAmount)}
	addAssetDetails(debitDetails, op.SendAsset, "")
	e.addMuxed(e.op.sourceAccount(), EffectAccountDebited, debitDetails)

	e.addTradeEffects(*e.op.sourceAccount(), resultSuccess.Offers)
}

func (e *efx) addManageSellOfferEffects() {
	result := e.op.operationResult().MustManageSellOfferResult().MustSuccess()
	e.addTradeEffects(*e.op.sourceAccount(), result.OffersClaimed)
}

func (e *efx) addManageBuyOfferEffects() {
	result := e.op.operationResult().MustManageBuyOfferResult().MustSuccess()
	e.addTradeEffects(*e.op.sourceAccount(), result.OffersClaimed)
}

func (e *efx) addCreatePassiveSellOfferEffects() {
	result := e.op.operationResult()
	var claims []xdr.ClaimAtom
	if result.Type == xdr.OperationTypeManageSellOffer {
		claims = result.MustManageSellOfferResult().MustSuccess().OffersClaimed
	} else {
		claims = result.MustCreatePassiveSellOfferResult().MustSuccess().OffersClaimed
	}
	e.addTradeEffects(*e.op.sourceAccount(), claims)
}

func (e *efx) addTradeEffects(buyer xdr.MuxedAccount, claims []xdr.ClaimAtom) {
	for _, claim := range claims {
		if claim.AmountSold() == 0 && claim.AmountBought() == 0 {
			continue
		}
		if claim.Type == xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool {
			e.addLPTradeEffect(claim)
			continue
		}
		seller := claim.SellerId()
		bd := map[string]interface{}{
			"offer_id":      claim.OfferId(),
			"seller":        seller.Address(),
			"bought_amount": amount.String(claim.AmountSold()),
			"sold_amount":   amount.String(claim.AmountBought()),
		}
		addAssetDetails(bd, claim.AssetSold(), "bought_")
		addAssetDetails(bd, claim.AssetBought(), "sold_")

		sd := map[string]interface{}{
			"offer_id":      claim.OfferId(),
			"bought_amount": amount.String(claim.AmountBought()),
			"sold_amount":   amount.String(claim.AmountSold()),
		}
		buyerAccID := buyer.ToAccountId()
		sd["seller"] = buyerAccID.Address()
		addAssetDetails(sd, claim.AssetBought(), "bought_")
		addAssetDetails(sd, claim.AssetSold(), "sold_")

		e.addMuxed(&buyer, EffectTrade, bd)
		e.addUnmuxed(&seller, EffectTrade, sd)
		e.addMuxed(&buyer, EffectOfferUpdated, bd)
		e.addUnmuxed(&seller, EffectOfferUpdated, sd)
		e.addMuxed(&buyer, EffectOfferRemoved, bd)
		e.addUnmuxed(&seller, EffectOfferRemoved, sd)
	}
}

func (e *efx) addLPTradeEffect(claim xdr.ClaimAtom) {
	lp, delta, err := e.op.getLiquidityPoolDelta(&claim.LiquidityPool.LiquidityPoolId)
	if err != nil {
		return
	}
	details := map[string]interface{}{
		"liquidity_pool": liquidityPoolDetails(lp),
		"sold":           map[string]string{"asset": claim.LiquidityPool.AssetSold.StringCanonical(), "amount": amount.String(claim.LiquidityPool.AmountSold)},
		"bought":         map[string]string{"asset": claim.LiquidityPool.AssetBought.StringCanonical(), "amount": amount.String(claim.LiquidityPool.AmountBought)},
	}
	_ = delta
	e.addMuxed(e.op.sourceAccount(), EffectLiquidityPoolTrade, details)
}

func (e *efx) addSetOptionsEffects() {
	source := e.op.sourceAccount()
	op := e.op.op.Body.MustSetOptionsOp()

	if op.HomeDomain != nil {
		e.addMuxed(source, EffectAccountHomeDomainUpdated, map[string]interface{}{"home_domain": string(*op.HomeDomain)})
	}

	thresholds := map[string]interface{}{}
	if op.LowThreshold != nil {
		thresholds["low_threshold"] = *op.LowThreshold
	}
	if op.MedThreshold != nil {
		thresholds["med_threshold"] = *op.MedThreshold
	}
	if op.HighThreshold != nil {
		thresholds["high_threshold"] = *op.HighThreshold
	}
	if len(thresholds) > 0 {
		e.addMuxed(source, EffectAccountThresholdsUpdated, thresholds)
	}

	flagDetails := map[string]interface{}{}
	if op.SetFlags != nil {
		setAuthFlagDetails(flagDetails, xdr.AccountFlags(*op.SetFlags), true)
	}
	if op.ClearFlags != nil {
		setAuthFlagDetails(flagDetails, xdr.AccountFlags(*op.ClearFlags), false)
	}
	if len(flagDetails) > 0 {
		e.addMuxed(source, EffectAccountFlagsUpdated, flagDetails)
	}

	if op.InflationDest != nil {
		e.addMuxed(source, EffectAccountInflationDestinationUpdated, map[string]interface{}{"inflation_destination": op.InflationDest.Address()})
	}

	// Signer changes
	changes, err := e.op.tx.GetOperationChanges(e.op.index)
	if err != nil {
		return
	}
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeAccount || change.Pre == nil || change.Post == nil {
			continue
		}
		preAcct := change.Pre.Data.MustAccount()
		postAcct := change.Post.Data.MustAccount()
		before := (&preAcct).SignerSummary()
		after := (&postAcct).SignerSummary()
		if reflect.DeepEqual(before, after) {
			continue
		}

		sorted := make([]string, 0, len(before))
		for s := range before {
			sorted = append(sorted, s)
		}
		sort.Strings(sorted)

		for _, addy := range sorted {
			weight, ok := after[addy]
			if !ok {
				e.addMuxed(source, EffectSignerRemoved, map[string]interface{}{"public_key": addy})
				continue
			}
			if weight != before[addy] {
				e.addMuxed(source, EffectSignerUpdated, map[string]interface{}{"public_key": addy, "weight": weight})
			}
		}

		afterSorted := make([]string, 0, len(after))
		for s := range after {
			afterSorted = append(afterSorted, s)
		}
		sort.Strings(afterSorted)
		for _, addy := range afterSorted {
			if _, ok := before[addy]; ok {
				continue
			}
			e.addMuxed(source, EffectSignerCreated, map[string]interface{}{"public_key": addy, "weight": after[addy]})
		}
	}
}

func (e *efx) addChangeTrustEffects() {
	source := e.op.sourceAccount()
	op := e.op.op.Body.MustChangeTrustOp()
	changes, err := e.op.tx.GetOperationChanges(e.op.index)
	if err != nil {
		return
	}
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeTrustline {
			continue
		}
		var effect int
		var trustLine xdr.TrustLineEntry
		switch {
		case change.Pre == nil && change.Post != nil:
			effect = EffectTrustlineCreated
			trustLine = *change.Post.Data.TrustLine
		case change.Pre != nil && change.Post == nil:
			effect = EffectTrustlineRemoved
			trustLine = *change.Pre.Data.TrustLine
		case change.Pre != nil && change.Post != nil:
			effect = EffectTrustlineUpdated
			trustLine = *change.Post.Data.TrustLine
		default:
			continue
		}
		if op.Line.Type != trustLine.Asset.Type {
			continue
		}
		details := map[string]interface{}{"limit": amount.String(op.Limit)}
		if trustLine.Asset.Type == xdr.AssetTypeAssetTypePoolShare {
			details["asset_type"] = "liquidity_pool"
			if trustLine.Asset.LiquidityPoolId != nil {
				details["liquidity_pool_id"] = poolIDToString(*trustLine.Asset.LiquidityPoolId)
			}
		} else {
			addAssetDetails(details, trustLine.Asset.ToAsset(), "")
		}
		e.addMuxed(source, effect, details)
		break
	}
}

func (e *efx) addAllowTrustEffects() {
	source := e.op.sourceAccount()
	op := e.op.op.Body.MustAllowTrustOp()
	asset := op.Asset.ToAsset(source.ToAccountId())
	details := map[string]interface{}{"trustor": op.Trustor.Address()}
	addAssetDetails(details, asset, "")
	e.addMuxed(source, EffectTrustlineFlagsUpdated, details)
}

func (e *efx) addAccountMergeEffects() {
	source := e.op.sourceAccount()
	dest := e.op.op.Body.MustDestination()
	result := e.op.operationResult().MustAccountMergeResult()
	details := map[string]interface{}{"amount": amount.String(result.MustSourceAccountBalance()), "asset_type": "native"}
	e.addMuxed(source, EffectAccountDebited, details)
	creditDetails := map[string]interface{}{"amount": amount.String(result.MustSourceAccountBalance()), "asset_type": "native"}
	e.addMuxed(&dest, EffectAccountCredited, creditDetails)
	e.addMuxed(source, EffectAccountRemoved, map[string]interface{}{})
}

func (e *efx) addInflationEffects() {
	payouts := e.op.operationResult().MustInflationResult().MustPayouts()
	for _, payout := range payouts {
		e.addUnmuxed(&payout.Destination, EffectAccountCredited, map[string]interface{}{
			"amount": amount.String(payout.Amount), "asset_type": "native",
		})
	}
}

func (e *efx) addManageDataEffects() {
	source := e.op.sourceAccount()
	op := e.op.op.Body.MustManageDataOp()
	details := map[string]interface{}{"name": string(op.DataName)}
	effect := 0
	changes, err := e.op.tx.GetOperationChanges(e.op.index)
	if err != nil {
		return
	}
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeData {
			continue
		}
		if change.Post != nil {
			raw := change.Post.Data.MustData().DataValue
			details["value"] = base64.StdEncoding.EncodeToString(raw)
		}
		switch {
		case change.Pre == nil && change.Post != nil:
			effect = EffectDataCreated
		case change.Pre != nil && change.Post == nil:
			effect = EffectDataRemoved
		case change.Pre != nil && change.Post != nil:
			effect = EffectDataUpdated
		}
		break
	}
	e.addMuxed(source, effect, details)
}

func (e *efx) addBumpSequenceEffects() {
	source := e.op.sourceAccount()
	changes, err := e.op.tx.GetOperationChanges(e.op.index)
	if err != nil {
		return
	}
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeAccount || change.Pre == nil || change.Post == nil {
			continue
		}
		beforeSeq := change.Pre.Data.MustAccount().SeqNum
		afterSeq := change.Post.Data.MustAccount().SeqNum
		if beforeSeq != afterSeq {
			e.addMuxed(source, EffectSequenceBumped, map[string]interface{}{"new_seq": afterSeq})
		}
		break
	}
}

func (e *efx) addCreateClaimableBalanceEffects() {
	source := e.op.sourceAccount()
	changes, err := e.op.tx.GetOperationChanges(e.op.index)
	if err != nil {
		return
	}
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeClaimableBalance || change.Post == nil {
			continue
		}
		cb := change.Post.Data.MustClaimableBalance()
		id, _ := xdr.MarshalHex(cb.BalanceId)
		e.addMuxed(source, EffectClaimableBalanceCreated, map[string]interface{}{
			"balance_id": id, "amount": amount.String(cb.Amount), "asset": cb.Asset.StringCanonical(),
		})
		for _, c := range cb.Claimants {
			cv0 := c.MustV0()
			e.addUnmuxed(&cv0.Destination, EffectClaimableBalanceClaimantCreated, map[string]interface{}{
				"balance_id": id, "amount": amount.String(cb.Amount), "asset": cb.Asset.StringCanonical(),
			})
		}
		debitDetails := map[string]interface{}{"amount": amount.String(cb.Amount)}
		addAssetDetails(debitDetails, cb.Asset, "")
		e.addMuxed(source, EffectAccountDebited, debitDetails)
		break
	}
}

func (e *efx) addClaimClaimableBalanceEffects() {
	op := e.op.op.Body.MustClaimClaimableBalanceOp()
	balanceID, _ := xdr.MarshalHex(op.BalanceId)
	source := e.op.sourceAccount()
	changes, err := e.op.tx.GetOperationChanges(e.op.index)
	if err != nil {
		return
	}
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeClaimableBalance || change.Pre == nil || change.Post != nil {
			continue
		}
		cb := change.Pre.Data.MustClaimableBalance()
		preID, _ := xdr.MarshalHex(cb.BalanceId)
		if preID != balanceID {
			continue
		}
		e.addMuxed(source, EffectClaimableBalanceClaimed, map[string]interface{}{
			"balance_id": balanceID, "amount": amount.String(cb.Amount), "asset": cb.Asset.StringCanonical(),
		})
		creditDetails := map[string]interface{}{"amount": amount.String(cb.Amount)}
		addAssetDetails(creditDetails, cb.Asset, "")
		e.addMuxed(source, EffectAccountCredited, creditDetails)
		break
	}
}

func (e *efx) addClawbackEffects() {
	op := e.op.op.Body.MustClawbackOp()
	source := e.op.sourceAccount()
	details := map[string]interface{}{"amount": amount.String(op.Amount)}
	addAssetDetails(details, op.Asset, "")
	creditDetails := map[string]interface{}{"amount": amount.String(op.Amount)}
	addAssetDetails(creditDetails, op.Asset, "")
	e.addMuxed(source, EffectAccountCredited, creditDetails)
	e.addMuxed(&op.From, EffectAccountDebited, details)
}

func (e *efx) addClawbackClaimableBalanceEffects() {
	op := e.op.op.Body.MustClawbackClaimableBalanceOp()
	balanceID, _ := xdr.MarshalHex(op.BalanceId)
	source := e.op.sourceAccount()
	e.addMuxed(source, EffectClaimableBalanceClawedBack, map[string]interface{}{"balance_id": balanceID})

	changes, err := e.op.tx.GetOperationChanges(e.op.index)
	if err != nil {
		return
	}
	for _, c := range changes {
		if c.Type == xdr.LedgerEntryTypeClaimableBalance && c.Post == nil && c.Pre != nil {
			cb := c.Pre.Data.MustClaimableBalance()
			creditDetails := map[string]interface{}{"amount": amount.String(cb.Amount)}
			addAssetDetails(creditDetails, cb.Asset, "")
			e.addMuxed(source, EffectAccountCredited, creditDetails)
			break
		}
	}
}

func (e *efx) addSetTrustLineFlagsEffects() {
	source := e.op.sourceAccount()
	op := e.op.op.Body.MustSetTrustLineFlagsOp()
	details := map[string]interface{}{"trustor": op.Trustor.Address()}
	addAssetDetails(details, op.Asset, "")
	setTrustLineFlagDetails(details, xdr.TrustLineFlags(op.SetFlags), true)
	setTrustLineFlagDetails(details, xdr.TrustLineFlags(op.ClearFlags), false)
	e.addMuxed(source, EffectTrustlineFlagsUpdated, details)
}

func (e *efx) addLiquidityPoolDepositEffects() {
	op := e.op.op.Body.MustLiquidityPoolDepositOp()
	lp, delta, err := e.op.getLiquidityPoolDelta(&op.LiquidityPoolId)
	if err != nil {
		return
	}
	cp := lp.Body.ConstantProduct
	e.addMuxed(e.op.sourceAccount(), EffectLiquidityPoolDeposited, map[string]interface{}{
		"liquidity_pool": liquidityPoolDetails(lp),
		"reserves_deposited": []map[string]string{
			{"asset": cp.Params.AssetA.StringCanonical(), "amount": amount.String(delta.ReserveA)},
			{"asset": cp.Params.AssetB.StringCanonical(), "amount": amount.String(delta.ReserveB)},
		},
		"shares_received": amount.String(delta.TotalPoolShares),
	})
}

func (e *efx) addLiquidityPoolWithdrawEffects() {
	op := e.op.op.Body.MustLiquidityPoolWithdrawOp()
	lp, delta, err := e.op.getLiquidityPoolDelta(&op.LiquidityPoolId)
	if err != nil {
		return
	}
	cp := lp.Body.ConstantProduct
	e.addMuxed(e.op.sourceAccount(), EffectLiquidityPoolWithdrew, map[string]interface{}{
		"liquidity_pool": liquidityPoolDetails(lp),
		"reserves_received": []map[string]string{
			{"asset": cp.Params.AssetA.StringCanonical(), "amount": amount.String(-delta.ReserveA)},
			{"asset": cp.Params.AssetB.StringCanonical(), "amount": amount.String(-delta.ReserveB)},
		},
		"shares_redeemed": amount.String(-delta.TotalPoolShares),
	})
}

func (e *efx) addInvokeHostFunctionEffects() {
	if e.op.network == "" {
		return
	}
	contractEvents, err := e.op.tx.GetContractEvents()
	if err != nil {
		return
	}
	source := e.op.sourceAccount()
	for _, event := range contractEvents {
		evt, err := contractevents.NewStellarAssetContractEvent(&event, e.op.network)
		if err != nil {
			continue
		}
		details := map[string]interface{}{}
		addAssetDetails(details, evt.GetAsset(), "")

		switch evt.GetType() {
		case contractevents.EventTypeTransfer:
			details["contract_event_type"] = "transfer"
			te := evt.(*contractevents.TransferEvent)
			details["amount"] = amount.String128(te.Amount)
			toDetails := map[string]interface{}{}
			for k, v := range details {
				toDetails[k] = v
			}
			if strkey.IsValidEd25519PublicKey(te.From) {
				e.add(te.From, EffectAccountDebited, details)
			} else {
				details["contract"] = te.From
				e.addMuxed(source, EffectContractDebited, details)
			}
			if strkey.IsValidEd25519PublicKey(te.To) {
				e.add(te.To, EffectAccountCredited, toDetails)
			} else {
				toDetails["contract"] = te.To
				e.addMuxed(source, EffectContractCredited, toDetails)
			}
		case contractevents.EventTypeMint:
			details["contract_event_type"] = "mint"
			me := evt.(*contractevents.MintEvent)
			details["amount"] = amount.String128(me.Amount)
			if strkey.IsValidEd25519PublicKey(me.To) {
				e.add(me.To, EffectAccountCredited, details)
			} else {
				details["contract"] = me.To
				e.addMuxed(source, EffectContractCredited, details)
			}
		case contractevents.EventTypeClawback:
			details["contract_event_type"] = "clawback"
			ce := evt.(*contractevents.ClawbackEvent)
			details["amount"] = amount.String128(ce.Amount)
			if strkey.IsValidEd25519PublicKey(ce.From) {
				e.add(ce.From, EffectAccountDebited, details)
			} else {
				details["contract"] = ce.From
				e.addMuxed(source, EffectContractDebited, details)
			}
		case contractevents.EventTypeBurn:
			details["contract_event_type"] = "burn"
			be := evt.(*contractevents.BurnEvent)
			details["amount"] = amount.String128(be.Amount)
			if strkey.IsValidEd25519PublicKey(be.From) {
				e.add(be.From, EffectAccountDebited, details)
			} else {
				details["contract"] = be.From
				e.addMuxed(source, EffectContractDebited, details)
			}
		}
	}
}

func (e *efx) addExtendFootprintTtlEffects() {
	op := e.op.op.Body.MustExtendFootprintTtlOp()
	changes, err := e.op.tx.GetOperationChanges(e.op.index)
	if err != nil {
		return
	}
	entries := make([]string, 0)
	for _, change := range changes {
		if change.Post == nil || change.Post.Data.Type != xdr.LedgerEntryTypeTtl {
			continue
		}
		v := change.Post.Data.MustTtl()
		var key xdr.LedgerKey
		if err := key.SetTtl(v.KeyHash); err != nil {
			continue
		}
		b64, err := xdr.MarshalBase64(key)
		if err != nil {
			continue
		}
		entries = append(entries, b64)
	}
	e.addMuxed(e.op.sourceAccount(), EffectExtendFootprintTtl, map[string]interface{}{
		"entries": entries, "extend_to": op.ExtendTo,
	})
}

func (e *efx) addRestoreFootprintEffects() {
	changes, err := e.op.tx.GetOperationChanges(e.op.index)
	if err != nil {
		return
	}
	entries := make([]string, 0)
	for _, change := range changes {
		if change.Post == nil || change.Post.Data.Type != xdr.LedgerEntryTypeTtl {
			continue
		}
		v := change.Post.Data.MustTtl()
		var key xdr.LedgerKey
		if err := key.SetTtl(v.KeyHash); err != nil {
			continue
		}
		b64, err := xdr.MarshalBase64(key)
		if err != nil {
			continue
		}
		entries = append(entries, b64)
	}
	e.addMuxed(e.op.sourceAccount(), EffectRestoreFootprint, map[string]interface{}{"entries": entries})
}

// Cross-cutting: sponsorship effects from ledger entry changes
var sponsoringEffectsTable = map[xdr.LedgerEntryType]struct {
	created, updated, removed int
}{
	xdr.LedgerEntryTypeAccount:          {EffectAccountSponsorshipCreated, EffectAccountSponsorshipUpdated, EffectAccountSponsorshipRemoved},
	xdr.LedgerEntryTypeTrustline:        {EffectTrustlineSponsorshipCreated, EffectTrustlineSponsorshipUpdated, EffectTrustlineSponsorshipRemoved},
	xdr.LedgerEntryTypeData:             {EffectDataSponsorshipCreated, EffectDataSponsorshipUpdated, EffectDataSponsorshipRemoved},
	xdr.LedgerEntryTypeClaimableBalance: {EffectClaimableBalanceSponsorshipCreated, EffectClaimableBalanceSponsorshipUpdated, EffectClaimableBalanceSponsorshipRemoved},
}

func (e *efx) addSponsorshipEffects(change ingest.Change) {
	entry, found := sponsoringEffectsTable[change.Type]
	if !found {
		return
	}

	var effectType int
	details := map[string]interface{}{}

	switch {
	case (change.Pre == nil || change.Pre.SponsoringID() == nil) && (change.Post != nil && change.Post.SponsoringID() != nil):
		effectType = entry.created
		details["sponsor"] = (*change.Post.SponsoringID()).Address()
	case (change.Pre != nil && change.Pre.SponsoringID() != nil) && (change.Post == nil || change.Post.SponsoringID() == nil):
		effectType = entry.removed
		details["former_sponsor"] = (*change.Pre.SponsoringID()).Address()
	case (change.Pre != nil && change.Pre.SponsoringID() != nil) && (change.Post != nil && change.Post.SponsoringID() != nil):
		pre := (*change.Pre.SponsoringID()).Address()
		post := (*change.Post.SponsoringID()).Address()
		if pre == post {
			return
		}
		effectType = entry.updated
		details["new_sponsor"] = post
		details["former_sponsor"] = pre
	default:
		return
	}

	var data xdr.LedgerEntryData
	if change.Post != nil {
		data = change.Post.Data
	} else {
		data = change.Pre.Data
	}

	switch change.Type {
	case xdr.LedgerEntryTypeAccount:
		a := data.MustAccount().AccountId
		e.addUnmuxed(&a, effectType, details)
	case xdr.LedgerEntryTypeTrustline:
		tl := data.MustTrustLine()
		e.addUnmuxed(&tl.AccountId, effectType, details)
	case xdr.LedgerEntryTypeData:
		details["data_name"] = string(data.MustData().DataName)
		e.addMuxed(e.op.sourceAccount(), effectType, details)
	case xdr.LedgerEntryTypeClaimableBalance:
		id, _ := xdr.MarshalHex(data.MustClaimableBalance().BalanceId)
		details["balance_id"] = id
		e.addMuxed(e.op.sourceAccount(), effectType, details)
	}
}

// Cross-cutting: LP lifecycle effects
func (e *efx) addLPLifecycleEffects(change ingest.Change) {
	if change.Type != xdr.LedgerEntryTypeLiquidityPool {
		return
	}
	switch {
	case change.Pre == nil && change.Post != nil:
		e.addMuxed(e.op.sourceAccount(), EffectLiquidityPoolCreated, map[string]interface{}{
			"liquidity_pool": liquidityPoolDetails(change.Post.Data.LiquidityPool),
		})
	case change.Pre != nil && change.Post == nil:
		poolID := change.Pre.Data.LiquidityPool.LiquidityPoolId
		e.addMuxed(e.op.sourceAccount(), EffectLiquidityPoolRemoved, map[string]interface{}{
			"liquidity_pool_id": poolIDToString(poolID),
		})
	}
}

// ---- Main extractor function ----

// ExtractEffects extracts effect data from a ledger.
// Covers all 50+ Stellar effect types across all operation types.
func ExtractEffects(input *LedgerInput) ([]EffectData, error) {
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(input.NetworkPassphrase, input.LCM)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer reader.Close()

	var allEffects []EffectData

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for effects in ledger %d: %v", input.Sequence, err)
			continue
		}

		if !tx.Result.Successful() {
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])

		for opi, op := range tx.Envelope.Operations() {
			ow := &opWrapper{
				index:     uint32(opi),
				tx:        tx,
				op:        op,
				ledgerSeq: input.Sequence,
				network:   input.NetworkPassphrase,
				closedAt:  input.ClosedAt,
			}

			e := &efx{op: ow}

			// Operation-specific effects
			switch op.Body.Type {
			case xdr.OperationTypeCreateAccount:
				e.addAccountCreatedEffects()
			case xdr.OperationTypePayment:
				e.addPaymentEffects()
			case xdr.OperationTypePathPaymentStrictReceive:
				e.addPathPaymentStrictReceiveEffects()
			case xdr.OperationTypePathPaymentStrictSend:
				e.addPathPaymentStrictSendEffects()
			case xdr.OperationTypeManageSellOffer:
				e.addManageSellOfferEffects()
			case xdr.OperationTypeManageBuyOffer:
				e.addManageBuyOfferEffects()
			case xdr.OperationTypeCreatePassiveSellOffer:
				e.addCreatePassiveSellOfferEffects()
			case xdr.OperationTypeSetOptions:
				e.addSetOptionsEffects()
			case xdr.OperationTypeChangeTrust:
				e.addChangeTrustEffects()
			case xdr.OperationTypeAllowTrust:
				e.addAllowTrustEffects()
			case xdr.OperationTypeAccountMerge:
				e.addAccountMergeEffects()
			case xdr.OperationTypeInflation:
				e.addInflationEffects()
			case xdr.OperationTypeManageData:
				e.addManageDataEffects()
			case xdr.OperationTypeBumpSequence:
				e.addBumpSequenceEffects()
			case xdr.OperationTypeCreateClaimableBalance:
				e.addCreateClaimableBalanceEffects()
			case xdr.OperationTypeClaimClaimableBalance:
				e.addClaimClaimableBalanceEffects()
			case xdr.OperationTypeClawback:
				e.addClawbackEffects()
			case xdr.OperationTypeClawbackClaimableBalance:
				e.addClawbackClaimableBalanceEffects()
			case xdr.OperationTypeSetTrustLineFlags:
				e.addSetTrustLineFlagsEffects()
			case xdr.OperationTypeLiquidityPoolDeposit:
				e.addLiquidityPoolDepositEffects()
			case xdr.OperationTypeLiquidityPoolWithdraw:
				e.addLiquidityPoolWithdrawEffects()
			case xdr.OperationTypeInvokeHostFunction:
				e.addInvokeHostFunctionEffects()
			case xdr.OperationTypeExtendFootprintTtl:
				e.addExtendFootprintTtlEffects()
			case xdr.OperationTypeRestoreFootprint:
				e.addRestoreFootprintEffects()
			case xdr.OperationTypeBeginSponsoringFutureReserves,
				xdr.OperationTypeEndSponsoringFutureReserves,
				xdr.OperationTypeRevokeSponsorship:
				// Effects obtained indirectly from ledger entries
			}

			// Cross-cutting effects from ledger entry changes
			changes, err := tx.GetOperationChanges(uint32(opi))
			if err == nil {
				for _, change := range changes {
					e.addSponsorshipEffects(change)
					e.addLPLifecycleEffects(change)
				}
			}

			operationID := toid.New(int32(input.Sequence), int32(tx.Index), int32(opi+1)).ToInt64()

			// Convert internal records to EffectData
			for idx, rec := range e.records {
				var detailsJSON *string
				if len(rec.Details) > 0 {
					if b, err := json.Marshal(rec.Details); err == nil {
						s := string(b)
						detailsJSON = &s
					}
				}

				// Extract common fields from details for backward compatibility
				var amt, assetCode, assetIssuer, assetType *string
				if v, ok := rec.Details["amount"]; ok {
					s := fmt.Sprintf("%v", v)
					amt = &s
				}
				if v, ok := rec.Details["asset_code"]; ok {
					s := fmt.Sprintf("%v", v)
					assetCode = &s
				}
				if v, ok := rec.Details["asset_issuer"]; ok {
					s := fmt.Sprintf("%v", v)
					assetIssuer = &s
				}
				if v, ok := rec.Details["asset_type"]; ok {
					s := fmt.Sprintf("%v", v)
					assetType = &s
				}

				typeName := effectTypeNames[rec.EffectType]
				if typeName == "" {
					typeName = fmt.Sprintf("unknown_%d", rec.EffectType)
				}

				opID := operationID
				allEffects = append(allEffects, EffectData{
					LedgerSequence:   input.Sequence,
					TransactionHash:  txHash,
					OperationIndex:   opi,
					EffectIndex:      idx,
					EffectType:       rec.EffectType,
					EffectTypeString: typeName,
					AccountID:        &rec.Address,
					Amount:           amt,
					AssetCode:        assetCode,
					AssetIssuer:      assetIssuer,
					AssetType:        assetType,
					DetailsJSON:      detailsJSON,
					OperationID:      &opID,
					CreatedAt:        input.ClosedAt,
					LedgerRange:      input.LedgerRange,
					EraID:            input.EraID,
				})
			}
		}
	}

	return allEffects, nil
}
