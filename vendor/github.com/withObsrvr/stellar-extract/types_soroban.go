package extract

import "time"

// ContractEventData represents Soroban contract events stream (contract_events_stream_v1)
type ContractEventData struct {
	EventID                  string
	ContractID               *string // C-address (strkey-encoded), e.g. "CAAAA..."
	LedgerSequence           uint32
	TransactionHash          string
	ClosedAt                 time.Time
	EventType                string
	InSuccessfulContractCall bool
	Successful               bool   // whether the parent transaction succeeded
	ContractEventXDR         string // base64-encoded XDR of the full ContractEvent (for reprocessing)
	TopicsJSON               string
	TopicsDecoded            string
	DataXDR                  string
	DataDecoded              string
	TopicCount               int32
	Topic0Decoded            *string
	Topic1Decoded            *string
	Topic2Decoded            *string
	Topic3Decoded            *string
	OperationIndex           uint32
	EventIndex               uint32
	CreatedAt                time.Time
	LedgerRange              uint32
	EraID                    *string
}

// ContractDataData represents Soroban contract data snapshot (contract_data_snapshot_v1)
type ContractDataData struct {
	ContractId         string
	LedgerSequence     uint32
	LedgerKeyHash      string
	ContractKeyType    string
	ContractDurability string
	AssetCode          *string
	AssetIssuer        *string
	AssetType          *string
	BalanceHolder      *string
	Balance            *string
	LastModifiedLedger int32
	LedgerEntryChange  int32
	Deleted            bool
	ClosedAt           time.Time
	ContractDataXDR    string
	TokenName          *string
	TokenSymbol        *string
	TokenDecimals      *int32
	CreatedAt          time.Time
	LedgerRange        uint32
	EraID              *string
}

// ContractCodeData represents Soroban contract code snapshot (contract_code_snapshot_v1)
type ContractCodeData struct {
	ContractCodeHash   string
	LedgerKeyHash      string
	ContractCodeExtV   int32
	LastModifiedLedger int32
	LedgerEntryChange  int32
	Deleted            bool
	ClosedAt           time.Time
	LedgerSequence     uint32
	NInstructions      *int64
	NFunctions         *int64
	NGlobals           *int64
	NTableEntries      *int64
	NTypes             *int64
	NDataSegments      *int64
	NElemSegments      *int64
	NImports           *int64
	NExports           *int64
	NDataSegmentBytes  *int64
	CreatedAt          time.Time
	LedgerRange        uint32
	EraID              *string
}

// ContractCreationData represents a contract creation event
type ContractCreationData struct {
	ContractID     string
	CreatorAddress string
	WasmHash       *string
	CreatedLedger  uint32
	CreatedAt      time.Time
	LedgerRange    uint32
	EraID          *string
}

// ContractCall represents a single cross-contract call in the call graph
type ContractCall struct {
	FromContract   string      `json:"from_contract"`
	ToContract     string      `json:"to_contract"`
	FunctionName   string      `json:"function"`
	Arguments      interface{} `json:"arguments,omitempty"`
	CallDepth      int         `json:"call_depth"`
	ExecutionOrder int         `json:"execution_order"`
	Successful     bool        `json:"successful"`
}

// CallGraphResult contains the extracted call graph for an operation
type CallGraphResult struct {
	Calls             []ContractCall
	ContractsInvolved []string
	MaxDepth          int
}

// WASMMetadata holds parsed metadata from a WASM binary
type WASMMetadata struct {
	NInstructions     *int64
	NFunctions        *int64
	NGlobals          *int64
	NTableEntries     *int64
	NTypes            *int64
	NDataSegments     *int64
	NElemSegments     *int64
	NImports          *int64
	NExports          *int64
	NDataSegmentBytes *int64
}
