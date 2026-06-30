# stellar-extract

Shared Go library for extracting typed rows from Stellar ledger data. Single source of truth for bronze-layer extraction logic across the Obsrvr data platform.

## Install

```bash
go get github.com/withObsrvr/stellar-extract@latest
```

Only dependency: `github.com/stellar/go-stellar-sdk v0.5.0`

## Quick start

```go
package main

import (
    "fmt"
    "log"

    extract "github.com/withObsrvr/stellar-extract"
    "github.com/stellar/go-stellar-sdk/xdr"
)

func main() {
    // From raw XDR bytes (streaming ingester, gRPC, protobuf)
    input, err := extract.NewLedgerInputFromXDR(xdrBytes, "Test SDF Network ; September 2015")
    if err != nil {
        log.Fatal(err)
    }

    // Or from an already-decoded LedgerCloseMeta (history loader, nebu)
    // input := extract.NewLedgerInput(lcm, "Test SDF Network ; September 2015")

    // Extract everything at once (runs all 16 extractors concurrently)
    data, errs := extract.ExtractAll(input)
    for _, e := range errs {
        log.Printf("warning: %v", e)
    }

    fmt.Printf("ledgers=%d transactions=%d operations=%d effects=%d\n",
        len(data.Ledgers), len(data.Transactions), len(data.Operations), len(data.Effects))

    // Or extract a single table
    transfers, err := extract.ExtractTokenTransfers(input)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("token_transfers=%d\n", len(transfers))
}
```

## API

### Entry points

```go
// Create input from decoded LedgerCloseMeta
extract.NewLedgerInput(lcm xdr.LedgerCloseMeta, networkPassphrase string) *LedgerInput

// Create input from raw XDR bytes
extract.NewLedgerInputFromXDR(xdrBytes []byte, networkPassphrase string) (*LedgerInput, error)

// Run all 16 extractors concurrently
extract.ExtractAll(input *LedgerInput) (*LedgerData, []error)
```

### Individual extractors

Every extractor has the same signature: `func Extract*(input *LedgerInput) ([]TypeData, error)`

| Function | Output type | Bronze table |
|---|---|---|
| `ExtractLedgers` | `[]LedgerRowData` | `ledgers_row_v2` |
| `ExtractTransactions` | `[]TransactionData` | `transactions_row_v2` |
| `ExtractOperations` | `[]OperationData` | `operations_row_v2` |
| `ExtractEffects` | `[]EffectData` | `effects_row_v1` |
| `ExtractTrades` | `[]TradeData` | `trades_row_v1` |
| `ExtractAccounts` | `[]AccountData` | `accounts_snapshot_v1` |
| `ExtractTrustlines` | `[]TrustlineData` | `trustlines_snapshot_v1` |
| `ExtractAccountSigners` | `[]AccountSignerData` | `account_signers_snapshot_v1` |
| `ExtractNativeBalances` | `[]NativeBalanceData` | `native_balances_snapshot_v1` |
| `ExtractContractEvents` | `[]ContractEventData` | `contract_events_stream_v1` |
| `ExtractContractData` | `[]ContractDataData` | `contract_data_snapshot_v1` |
| `ExtractContractCode` | `[]ContractCodeData` | `contract_code_snapshot_v1` |
| `ExtractContractCreations` | `[]ContractCreationData` | `contract_creations_v1` |
| `ExtractTokenTransfers` | `[]TokenTransferData` | `token_transfers_stream_v1` |
| `ExtractEvictedKeys` | `[]EvictedKeyData` | `evicted_keys_state_v1` |
| `ExtractRestoredKeys` | `[]RestoredKeyData` | `restored_keys_state_v1` |

### LedgerInput

```go
type LedgerInput struct {
    LCM               xdr.LedgerCloseMeta
    NetworkPassphrase string
    Sequence          uint32    // auto-set from LCM
    ClosedAt          time.Time // auto-set from LCM
    LedgerRange       uint32    // partition key, default: floor(seq/10000)
    EraID             *string   // optional DuckLake era identifier
}
```

`Sequence`, `ClosedAt`, and `LedgerRange` are populated automatically by `NewLedgerInput` / `NewLedgerInputFromXDR`. Override `LedgerRange` or set `EraID` after creation if needed.

## Usage patterns

### Obsrvr Lake: history loader

Replace 16 local extractor files with a single library import:

```go
input := extract.NewLedgerInput(lcm, networkPassphrase)
input.LedgerRange = customRange
input.EraID = &eraID
data, errs := extract.ExtractAll(input)
// Write data.Transactions to Parquet, data.Effects to Parquet, etc.
```

### Obsrvr Lake: streaming ingester

Replace `(w *Writer) extract*` methods:

```go
input, _ := extract.NewLedgerInputFromXDR(rawLedger.LedgerCloseMetaXdr, networkPassphrase)
data, errs := extract.ExtractAll(input)
// Batch insert data.Transactions into PG, data.Effects into PG, etc.
```

### Nebu processor

Use a single extractor for a focused processor:

```go
func (p *Processor) ProcessLedger(lcm xdr.LedgerCloseMeta) error {
    input := extract.NewLedgerInput(lcm, p.networkPassphrase)
    transfers, err := extract.ExtractTokenTransfers(input)
    if err != nil {
        return err
    }
    for _, t := range transfers {
        p.emit(toProtobuf(t))
    }
    return nil
}
```

### flowctl-sdk processor

Zero-code extraction via the stellar helper:

```go
func main() {
    stellar.Run(func(np string, lcm xdr.LedgerCloseMeta) (proto.Message, error) {
        input := extract.NewLedgerInput(lcm, np)
        events, err := extract.ExtractContractEvents(input)
        if err != nil {
            return nil, err
        }
        return toProtobuf(events), nil
    })
}
```

## File layout

```
types_core.go        LedgerRowData, TransactionData, OperationData, EffectData, TradeData
types_accounts.go    AccountData, TrustlineData, AccountSignerData, NativeBalanceData, OfferData
types_soroban.go     ContractEventData, ContractDataData, ContractCodeData, ContractCreationData, WASMMetadata
types_state.go       ClaimableBalanceData, LiquidityPoolData, ConfigSettingData, TTLData, EvictedKeyData, RestoredKeyData
types_tokens.go      TokenTransferData

extract.go           LedgerInput, LedgerData, NewLedgerInput, NewLedgerInputFromXDR, ExtractAll
ledgers.go           ExtractLedgers
transactions.go      ExtractTransactions + helpers
operations.go        ExtractOperations
effects.go           ExtractEffects (50+ effect types)
trades.go            ExtractTrades
accounts.go          ExtractAccounts, ExtractTrustlines, ExtractAccountSigners, ExtractNativeBalances
soroban.go           ExtractContractEvents, ExtractContractData, ExtractContractCode, ExtractContractCreations, ExtractRestoredKeys
scval_converter.go   ConvertScValToJSON
token_transfers.go   ExtractTokenTransfers
evicted_keys.go      ExtractEvictedKeys
```

## stellar-etl parity

Based on comparison with [stellar-etl](https://github.com/stellar/stellar-etl) (SDF's official ETL pipeline for BigQuery), the following features have been adopted:

- **`ContractEventXDR`** — base64-encoded XDR of the full `ContractEvent`, enabling reprocessing without re-reading the archive
- **`Successful`** — whether the parent transaction succeeded (broader than `InSuccessfulContractCall` which only tracks the contract call context)
- **C-address contract IDs** — contract IDs encoded via `strkey.Encode(strkey.VersionByteContract, ...)` producing `C...` addresses matching block explorers, instead of raw hex

## Design principles

- **Extraction only.** The library converts `xdr.LedgerCloseMeta` into typed Go structs. It doesn't know about Parquet, PostgreSQL, gRPC, protobuf, or any output format. Callers own serialization.
- **One input type.** Every extractor takes `*LedgerInput`. Callers construct it from XDR bytes or a decoded LCM.
- **Concurrent by default.** `ExtractAll` runs all 16 extractors in goroutines. Individual extractors are also safe to call concurrently.
- **Single SDK pin.** All extraction logic uses one version of `go-stellar-sdk`. When the SDK is upgraded, every consumer gets the fix.
