package extract

import "time"

// ClaimableBalanceData represents claimable balance snapshot state (claimable_balances_snapshot_v1)
type ClaimableBalanceData struct {
	BalanceID      string
	Sponsor        string
	LedgerSequence uint32
	ClosedAt       time.Time
	AssetType      string
	AssetCode      *string
	AssetIssuer    *string
	Amount         int64
	ClaimantsCount int32
	Flags          uint32
	CreatedAt      time.Time
	LedgerRange    uint32
	EraID          *string
}

// LiquidityPoolData represents liquidity pool snapshot state (liquidity_pools_snapshot_v1)
type LiquidityPoolData struct {
	LiquidityPoolID string
	LedgerSequence  uint32
	ClosedAt        time.Time
	PoolType        string
	Fee             int32
	TrustlineCount  int32
	TotalPoolShares int64
	AssetAType      string
	AssetACode      *string
	AssetAIssuer    *string
	AssetAAmount    int64
	AssetBType      string
	AssetBCode      *string
	AssetBIssuer    *string
	AssetBAmount    int64
	CreatedAt       time.Time
	LedgerRange     uint32
	EraID           *string
}

// ConfigSettingData represents network configuration settings snapshot (config_settings_snapshot_v1)
type ConfigSettingData struct {
	ConfigSettingID                 int32
	LedgerSequence                  uint32
	LastModifiedLedger              int32
	Deleted                         bool
	ClosedAt                        time.Time
	LedgerMaxInstructions           *int64
	TxMaxInstructions               *int64
	FeeRatePerInstructionsIncrement *int64
	TxMemoryLimit                   *uint32
	LedgerMaxReadLedgerEntries      *uint32
	LedgerMaxReadBytes              *uint32
	LedgerMaxWriteLedgerEntries     *uint32
	LedgerMaxWriteBytes             *uint32
	TxMaxReadLedgerEntries          *uint32
	TxMaxReadBytes                  *uint32
	TxMaxWriteLedgerEntries         *uint32
	TxMaxWriteBytes                 *uint32
	ContractMaxSizeBytes            *uint32
	ConfigSettingXDR                string
	CreatedAt                       time.Time
	LedgerRange                     uint32
	EraID                           *string
}

// TTLData represents time-to-live entries snapshot (ttl_snapshot_v1)
type TTLData struct {
	KeyHash            string
	LedgerSequence     uint32
	LiveUntilLedgerSeq uint32
	TTLRemaining       int64
	Expired            bool
	LastModifiedLedger int32
	Deleted            bool
	ClosedAt           time.Time
	CreatedAt          time.Time
	LedgerRange        uint32
	EraID              *string
}

// EvictedKeyData represents evicted storage keys state (evicted_keys_state_v1)
type EvictedKeyData struct {
	KeyHash        string
	LedgerSequence uint32
	ContractID     string
	KeyType        string
	Durability     string
	ClosedAt       time.Time
	LedgerRange    uint32
	CreatedAt      time.Time
	EraID          *string
}

// RestoredKeyData represents restored storage keys state (restored_keys_state_v1)
type RestoredKeyData struct {
	KeyHash            string
	LedgerSequence     uint32
	ContractID         string
	KeyType            string
	Durability         string
	RestoredFromLedger uint32
	ClosedAt           time.Time
	LedgerRange        uint32
	CreatedAt          time.Time
	EraID              *string
}
