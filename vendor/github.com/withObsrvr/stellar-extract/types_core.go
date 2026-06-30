package extract

import "time"

// LedgerRowData represents a single ledger row (ledgers_row_v2)
type LedgerRowData struct {
	Sequence             uint32
	LedgerHash           string
	PreviousLedgerHash   string
	ClosedAt             time.Time
	ProtocolVersion      uint32
	TotalCoins           int64
	FeePool              int64
	BaseFee              uint32
	BaseReserve          uint32
	MaxTxSetSize         uint32
	TransactionCount     int
	OperationCount       int
	SuccessfulTxCount    int
	FailedTxCount        int
	TxSetOperationCount  int
	SorobanFeeWrite1kb   *int64
	NodeID               *string
	Signature            *string
	LedgerHeader         *string
	BucketListSize       *int64
	LiveSorobanStateSize *int64
	EvictedKeysCount     *int32
	SorobanOpCount       *int32
	TotalFeeCharged      *int64
	ContractEventsCount  *int32
	IngestionTimestamp    time.Time
	LedgerRange          uint32
	PipelineVersion      string
	EraID                *string
}

// TransactionData represents a single transaction
type TransactionData struct {
	LedgerSequence        uint32
	TransactionHash       string
	SourceAccount         string
	SourceAccountMuxed    *string
	FeeCharged            int64
	MaxFee                int64
	Successful            bool
	TransactionResultCode string
	OperationCount        int
	MemoType              *string
	Memo                  *string
	CreatedAt             time.Time
	AccountSequence       int64
	LedgerRange           uint32
	SignaturesCount       int
	NewAccount            bool
	TimeboundsMinTime     *int64
	TimeboundsMaxTime     *int64
	// Soroban fields
	SorobanHostFunctionType *string
	SorobanContractID       *string
	RentFeeCharged          *int64
	// Soroban resource fields
	SorobanResourcesInstructions *int64
	SorobanResourcesReadBytes    *int64
	SorobanResourcesWriteBytes   *int64
	// TOID
	TransactionID int64
	EraID         *string
}

// OperationData represents a single operation
type OperationData struct {
	TransactionHash       string
	TransactionIndex      int
	OperationIndex        int
	LedgerSequence        uint32
	SourceAccount         string
	SourceAccountMuxed    *string
	OpType                int
	TypeString            string
	CreatedAt             time.Time
	TransactionSuccessful bool
	OperationResultCode   *string
	LedgerRange           uint32
	// Core operation fields
	Amount      *int64
	Asset       *string
	Destination *string
	// Decomposed asset fields
	AssetType   *string
	AssetCode   *string
	AssetIssuer *string
	// Source asset (path payments)
	SourceAsset       *string
	SourceAssetType   *string
	SourceAssetCode   *string
	SourceAssetIssuer *string
	SourceAmount      *int64
	DestinationMin    *int64
	StartingBalance   *int64
	// Trustline
	TrustlineLimit *int64
	// Offer fields
	OfferID *int64
	Price   *string
	PriceR  *string
	// Buying/Selling assets (offers)
	BuyingAsset        *string
	BuyingAssetType    *string
	BuyingAssetCode    *string
	BuyingAssetIssuer  *string
	SellingAsset       *string
	SellingAssetType   *string
	SellingAssetCode   *string
	SellingAssetIssuer *string
	// Set options
	SetFlags        *int
	ClearFlags      *int
	HomeDomain      *string
	MasterWeight    *int
	LowThreshold   *int
	MediumThreshold *int
	HighThreshold   *int
	// Manage data
	DataName  *string
	DataValue *string
	// Claimable balance
	BalanceID *string
	// Sponsorship
	SponsoredID *string
	// Bump sequence
	BumpTo *int64
	// Soroban
	SorobanAuthRequired  *bool
	SorobanOperation     *string
	SorobanContractID    *string
	SorobanFunction      *string
	SorobanArgumentsJSON *string
	// Call graph fields
	ContractCallsJSON *string
	ContractsInvolved []string
	MaxCallDepth      *int
	// TOID
	TransactionID int64
	OperationID   int64
	EraID         *string
}

// EffectData represents a single effect (state changes from operations)
type EffectData struct {
	LedgerSequence  uint32
	TransactionHash string
	OperationIndex  int
	EffectIndex     int
	EffectType       int
	EffectTypeString string
	AccountID        *string
	Amount           *string
	AssetCode        *string
	AssetIssuer      *string
	AssetType        *string
	TrustlineLimit   *string
	AuthorizeFlag    *bool
	ClawbackFlag     *bool
	SignerAccount    *string
	SignerWeight     *int
	OfferID          *int64
	SellerAccount    *string
	OperationID      *int64
	DetailsJSON      *string
	CreatedAt        time.Time
	LedgerRange      uint32
	EraID            *string
}

// TradeData represents a single trade execution (DEX trades)
type TradeData struct {
	LedgerSequence     uint32
	TransactionHash    string
	OperationIndex     int
	TradeIndex         int
	TradeType          string
	TradeTimestamp     time.Time
	SellerAccount      string
	SellingAssetCode   *string
	SellingAssetIssuer *string
	SellingAmount      string
	BuyerAccount       string
	BuyingAssetCode    *string
	BuyingAssetIssuer  *string
	BuyingAmount       string
	Price              string
	CreatedAt          time.Time
	LedgerRange        uint32
	EraID              *string
}
