package extract

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ExtractAccounts extracts account snapshots from a ledger.
// Pattern: LedgerChangeReader + Filter by LedgerEntryTypeAccount + Map deduplication
func ExtractAccounts(input *LedgerInput) ([]AccountData, error) {
	var accounts []AccountData

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(input.NetworkPassphrase, input.LCM)
	if err != nil {
		log.Printf("Failed to create change reader for accounts: %v", err)
		return accounts, nil
	}
	defer reader.Close()

	// Map-based deduplication: same account can change multiple times per ledger
	accountMap := make(map[string]*AccountData)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for accounts: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		var accountEntry *xdr.AccountEntry
		if change.Post != nil {
			if ae, ok := change.Post.Data.GetAccount(); ok {
				accountEntry = &ae
			}
		}

		if accountEntry == nil {
			continue
		}

		accountData := extractAccountData(accountEntry, input.Sequence, input.ClosedAt, input.LedgerRange, input.EraID)
		accountCopy := accountData
		accountMap[accountData.AccountID] = &accountCopy
	}

	for _, account := range accountMap {
		accounts = append(accounts, *account)
	}

	return accounts, nil
}

// extractAccountData extracts all fields from an AccountEntry.
func extractAccountData(entry *xdr.AccountEntry, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32, eraID *string) AccountData {
	accountID := entry.AccountId.Address()

	// Balance (convert from stroops to XLM string)
	balance := amount.String(entry.Balance)

	// Account Settings
	sequenceNumber := uint64(entry.SeqNum)
	numSubentries := uint32(entry.NumSubEntries)
	numSponsoring := uint32(0)
	numSponsored := uint32(0)

	var homeDomain *string
	if entry.HomeDomain != "" {
		hd := string(entry.HomeDomain)
		homeDomain = &hd
	}

	// Thresholds (stored as [4]byte)
	masterWeight := uint32(entry.Thresholds[0])
	lowThreshold := uint32(entry.Thresholds[1])
	medThreshold := uint32(entry.Thresholds[2])
	highThreshold := uint32(entry.Thresholds[3])

	// Flags (bitmask)
	flags := uint32(entry.Flags)
	authRequired := (flags & uint32(xdr.AccountFlagsAuthRequiredFlag)) != 0
	authRevocable := (flags & uint32(xdr.AccountFlagsAuthRevocableFlag)) != 0
	authImmutable := (flags & uint32(xdr.AccountFlagsAuthImmutableFlag)) != 0
	authClawbackEnabled := (flags & uint32(xdr.AccountFlagsAuthClawbackEnabledFlag)) != 0

	// Signers (extract as JSON array)
	var signersJSON *string
	if len(entry.Signers) > 0 {
		type SignerData struct {
			Key    string `json:"key"`
			Weight uint32 `json:"weight"`
		}
		var signers []SignerData
		for _, signer := range entry.Signers {
			signers = append(signers, SignerData{
				Key:    signer.Key.Address(),
				Weight: uint32(signer.Weight),
			})
		}
		if jsonBytes, err := json.Marshal(signers); err == nil {
			jsonStr := string(jsonBytes)
			signersJSON = &jsonStr
		}
	}

	// Sponsorship (from extensions)
	var sponsorAccount *string

	// Extract sponsorship counts (Protocol 14+)
	if ext, ok := entry.Ext.GetV1(); ok {
		if ext2, ok := ext.Ext.GetV2(); ok {
			numSponsoring = uint32(ext2.NumSponsoring)
			numSponsored = uint32(ext2.NumSponsored)
		}
	}

	now := time.Now().UTC()

	return AccountData{
		AccountID:      accountID,
		LedgerSequence: ledgerSeq,
		ClosedAt:       closedAt,
		Balance:        balance,
		SequenceNumber: sequenceNumber,
		NumSubentries:  numSubentries,
		NumSponsoring:  numSponsoring,
		NumSponsored:   numSponsored,
		HomeDomain:     homeDomain,
		MasterWeight:   masterWeight,
		LowThreshold:   lowThreshold,
		MedThreshold:   medThreshold,
		HighThreshold:  highThreshold,
		Flags:               flags,
		AuthRequired:        authRequired,
		AuthRevocable:       authRevocable,
		AuthImmutable:       authImmutable,
		AuthClawbackEnabled: authClawbackEnabled,
		Signers:        signersJSON,
		SponsorAccount: sponsorAccount,
		CreatedAt:      now,
		UpdatedAt:      now,
		LedgerRange:    ledgerRange,
		EraID:          eraID,
	}
}

// ExtractTrustlines extracts trustline snapshots from a ledger.
// Pattern: LedgerChangeReader + Filter by LedgerEntryTypeTrustline + Map deduplication
func ExtractTrustlines(input *LedgerInput) ([]TrustlineData, error) {
	var trustlines []TrustlineData

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(input.NetworkPassphrase, input.LCM)
	if err != nil {
		log.Printf("Failed to create change reader for trustlines: %v", err)
		return trustlines, nil
	}
	defer reader.Close()

	// Key: accountID:assetCode:assetIssuer
	trustlineMap := make(map[string]*TrustlineData)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for trustlines: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeTrustline {
			continue
		}

		var trustlineEntry *xdr.TrustLineEntry
		if change.Post != nil {
			if tl, ok := change.Post.Data.GetTrustLine(); ok {
				trustlineEntry = &tl
			}
		}

		if trustlineEntry == nil {
			continue
		}

		trustlineData := extractTrustlineData(trustlineEntry, input.Sequence, input.LedgerRange, input.EraID)

		key := fmt.Sprintf("%s:%s:%s", trustlineData.AccountID, trustlineData.AssetCode, trustlineData.AssetIssuer)
		trustlineCopy := trustlineData
		trustlineMap[key] = &trustlineCopy
	}

	for _, trustline := range trustlineMap {
		trustlines = append(trustlines, *trustline)
	}

	return trustlines, nil
}

// extractTrustlineData extracts all fields from a TrustLineEntry.
func extractTrustlineData(entry *xdr.TrustLineEntry, ledgerSeq uint32, ledgerRange uint32, eraID *string) TrustlineData {
	accountID := entry.AccountId.Address()

	var assetCode, assetIssuer, assetType string
	switch entry.Asset.Type {
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		a4 := entry.Asset.MustAlphaNum4()
		assetCode = strings.TrimRight(string(a4.AssetCode[:]), "\x00")
		assetIssuer = a4.Issuer.Address()
		assetType = "credit_alphanum4"
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		a12 := entry.Asset.MustAlphaNum12()
		assetCode = strings.TrimRight(string(a12.AssetCode[:]), "\x00")
		assetIssuer = a12.Issuer.Address()
		assetType = "credit_alphanum12"
	case xdr.AssetTypeAssetTypePoolShare:
		assetCode = "POOL_SHARE"
		assetIssuer = "pool"
		assetType = "liquidity_pool_shares"
	}

	balance := amount.String(entry.Balance)
	trustLimit := amount.String(entry.Limit)
	buyingLiabilities := "0"
	sellingLiabilities := "0"

	// Extract liabilities (Protocol 10+)
	if ext, ok := entry.Ext.GetV1(); ok {
		buyingLiabilities = amount.String(ext.Liabilities.Buying)
		sellingLiabilities = amount.String(ext.Liabilities.Selling)
	}

	// Authorization flags
	flags := uint32(entry.Flags)
	authorized := (flags & uint32(xdr.TrustLineFlagsAuthorizedFlag)) != 0
	authorizedToMaintainLiabilities := (flags & uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)) != 0
	clawbackEnabled := (flags & uint32(xdr.TrustLineFlagsTrustlineClawbackEnabledFlag)) != 0

	now := time.Now().UTC()

	return TrustlineData{
		AccountID:                       accountID,
		AssetCode:                       assetCode,
		AssetIssuer:                     assetIssuer,
		AssetType:                       assetType,
		Balance:                         balance,
		TrustLimit:                      trustLimit,
		BuyingLiabilities:               buyingLiabilities,
		SellingLiabilities:              sellingLiabilities,
		Authorized:                      authorized,
		AuthorizedToMaintainLiabilities: authorizedToMaintainLiabilities,
		ClawbackEnabled:                 clawbackEnabled,
		LedgerSequence:                  ledgerSeq,
		CreatedAt:                       now,
		LedgerRange:                     ledgerRange,
		EraID:                           eraID,
	}
}

// ExtractAccountSigners extracts account signers from account ledger entries.
// Uses map-based deduplication by accountID:signerKey to handle multiple
// change stages per ledger (fee processing, operation changes, post-apply, upgrades).
func ExtractAccountSigners(input *LedgerInput) ([]AccountSignerData, error) {
	var signersList []AccountSignerData

	changeReader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(input.NetworkPassphrase, input.LCM)
	if err != nil {
		log.Printf("Failed to create change reader for account signers: %v", err)
		return signersList, nil
	}
	defer changeReader.Close()

	// Map-based deduplication: key is "accountID:signerKey"
	signerMap := make(map[string]*AccountSignerData)

	for {
		change, err := changeReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for account signers: %v", err)
			continue
		}

		var accountEntry *xdr.AccountEntry
		var deleted bool

		switch change.Type {
		case xdr.LedgerEntryTypeAccount:
			if change.Post != nil {
				account := change.Post.Data.MustAccount()
				accountEntry = &account
				deleted = false
			} else if change.Pre != nil {
				account := change.Pre.Data.MustAccount()
				accountEntry = &account
				deleted = true
			}
		default:
			continue
		}

		if accountEntry == nil {
			continue
		}

		accountID := accountEntry.AccountId.Address()

		// Get signer sponsoring IDs if available (Protocol 14+)
		var sponsorIDs []xdr.SponsorshipDescriptor
		if accountEntry.Ext.V == 1 {
			v1 := accountEntry.Ext.MustV1()
			if v1.Ext.V == 2 {
				v2 := v1.Ext.MustV2()
				sponsorIDs = v2.SignerSponsoringIDs
			}
		}

		// Process each signer -- last-write-wins per (account, signer) pair
		for i, signer := range accountEntry.Signers {
			var sponsor string
			if i < len(sponsorIDs) && sponsorIDs[i] != nil {
				sponsor = sponsorIDs[i].Address()
			}

			signerKey := signer.Key.Address()
			dedupeKey := accountID + ":" + signerKey

			signerData := AccountSignerData{
				AccountID:      accountID,
				Signer:         signerKey,
				LedgerSequence: input.Sequence,
				Weight:         uint32(signer.Weight),
				Sponsor:        sponsor,
				Deleted:        deleted,
				ClosedAt:       input.ClosedAt,
				LedgerRange:    input.LedgerRange,
				CreatedAt:      time.Now().UTC(),
				EraID:          input.EraID,
			}

			signerMap[dedupeKey] = &signerData
		}
	}

	for _, s := range signerMap {
		signersList = append(signersList, *s)
	}

	return signersList, nil
}

// ExtractNativeBalances extracts XLM-only balances from a ledger.
func ExtractNativeBalances(input *LedgerInput) ([]NativeBalanceData, error) {
	var nativeBalancesList []NativeBalanceData

	changeReader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(input.NetworkPassphrase, input.LCM)
	if err != nil {
		log.Printf("Failed to create change reader for native balances: %v", err)
		return nativeBalancesList, nil
	}
	defer changeReader.Close()

	// Track unique native balances by account_id (deduplicate within ledger)
	nativeBalancesMap := make(map[string]*NativeBalanceData)

	for {
		change, err := changeReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for native balances: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		var accountEntry *xdr.AccountEntry
		var lastModifiedLedger uint32

		if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeAccount {
			entry, _ := change.Post.Data.GetAccount()
			accountEntry = &entry
			lastModifiedLedger = uint32(change.Post.LastModifiedLedgerSeq)
		} else if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeAccount {
			entry, _ := change.Pre.Data.GetAccount()
			accountEntry = &entry
			lastModifiedLedger = uint32(change.Pre.LastModifiedLedgerSeq)
		}

		if accountEntry == nil {
			continue
		}

		accountID := accountEntry.AccountId.Address()

		// Balance and liabilities (Protocol 10+ fields)
		balance := int64(accountEntry.Balance)
		buyingLiabilities := int64(0)
		sellingLiabilities := int64(0)

		if ext, ok := accountEntry.Ext.GetV1(); ok {
			buyingLiabilities = int64(ext.Liabilities.Buying)
			sellingLiabilities = int64(ext.Liabilities.Selling)
		}

		// Account metadata
		numSubentries := int32(accountEntry.NumSubEntries)
		numSponsoring := int32(0)
		numSponsored := int32(0)

		// Extract sponsoring/sponsored counts (Protocol 14+)
		if ext, ok := accountEntry.Ext.GetV1(); ok {
			if ext2, ok := ext.Ext.GetV2(); ok {
				numSponsoring = int32(ext2.NumSponsoring)
				numSponsored = int32(ext2.NumSponsored)
			}
		}

		// Sequence number (nullable)
		seqNum := int64(accountEntry.SeqNum)
		var sequenceNumber *int64
		if seqNum > 0 {
			sequenceNumber = &seqNum
		}

		data := NativeBalanceData{
			AccountID:          accountID,
			Balance:            balance,
			BuyingLiabilities:  buyingLiabilities,
			SellingLiabilities: sellingLiabilities,
			NumSubentries:      numSubentries,
			NumSponsoring:      numSponsoring,
			NumSponsored:       numSponsored,
			SequenceNumber:     sequenceNumber,
			LastModifiedLedger: int64(lastModifiedLedger),
			LedgerSequence:     int64(input.Sequence),
			LedgerRange:        int64(input.LedgerRange),
			EraID:              input.EraID,
		}

		nativeBalancesMap[accountID] = &data
	}

	for _, data := range nativeBalancesMap {
		nativeBalancesList = append(nativeBalancesList, *data)
	}

	return nativeBalancesList, nil
}
