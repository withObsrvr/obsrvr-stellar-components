package contracts

const (
	RawLedgerEventType   = "stellar.ledger.v1"
	LedgerBatchEventType = "stellar.ledger.batch.v1"
	ProtoContentType     = "application/protobuf"
	SchemaVersion        = "v1"
	ExtractionVersion    = "normalize-go-v0.1.0"
	DefaultComponentID   = "stellar-ledger-processor"
	DefaultNetwork       = "unknown"
	DefaultJSONLPath     = "ledger_batches.jsonl"
)
