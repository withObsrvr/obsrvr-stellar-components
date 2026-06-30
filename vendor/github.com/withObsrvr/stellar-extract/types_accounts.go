package extract

import "time"

// AccountData represents account snapshot state (accounts_snapshot_v1)
type AccountData struct {
	AccountID      string
	LedgerSequence uint32
	ClosedAt       time.Time
	Balance        string
	SequenceNumber uint64
	NumSubentries  uint32
	NumSponsoring  uint32
	NumSponsored   uint32
	HomeDomain     *string
	MasterWeight   uint32
	LowThreshold   uint32
	MedThreshold   uint32
	HighThreshold  uint32
	Flags               uint32
	AuthRequired        bool
	AuthRevocable       bool
	AuthImmutable       bool
	AuthClawbackEnabled bool
	Signers        *string
	SponsorAccount *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	LedgerRange    uint32
	EraID          *string
}

// TrustlineData represents trustline snapshot state (trustlines_snapshot_v1)
type TrustlineData struct {
	AccountID                       string
	AssetCode                       string
	AssetIssuer                     string
	AssetType                       string
	Balance                         string
	TrustLimit                      string
	BuyingLiabilities               string
	SellingLiabilities              string
	Authorized                      bool
	AuthorizedToMaintainLiabilities bool
	ClawbackEnabled                 bool
	LedgerSequence                  uint32
	CreatedAt                       time.Time
	LedgerRange                     uint32
	EraID                           *string
}

// AccountSignerData represents account signer snapshot state (account_signers_snapshot_v1)
type AccountSignerData struct {
	AccountID      string
	Signer         string
	LedgerSequence uint32
	Weight         uint32
	Sponsor        string
	Deleted        bool
	ClosedAt       time.Time
	LedgerRange    uint32
	CreatedAt      time.Time
	EraID          *string
}

// NativeBalanceData represents XLM-only balances snapshot (native_balances_snapshot_v1)
type NativeBalanceData struct {
	AccountID          string
	Balance            int64
	BuyingLiabilities  int64
	SellingLiabilities int64
	NumSubentries      int32
	NumSponsoring      int32
	NumSponsored       int32
	SequenceNumber     *int64
	LastModifiedLedger int64
	LedgerSequence     int64
	LedgerRange        int64
	EraID              *string
}

// OfferData represents DEX offer snapshot state (offers_snapshot_v1)
type OfferData struct {
	OfferID            int64
	SellerAccount      string
	LedgerSequence     uint32
	ClosedAt           time.Time
	SellingAssetType   string
	SellingAssetCode   *string
	SellingAssetIssuer *string
	BuyingAssetType    string
	BuyingAssetCode    *string
	BuyingAssetIssuer  *string
	Amount             string
	Price              string
	Flags              uint32
	CreatedAt          time.Time
	LedgerRange        uint32
	EraID              *string
}
