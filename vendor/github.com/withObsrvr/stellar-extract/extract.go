// Package extract provides a shared library for extracting typed rows from
// Stellar ledger data. It is the single source of truth for bronze-layer
// extraction logic, used by the history loader, streaming ingester, nebu
// processors, and flowctl-sdk processors.
//
// All extractors take a [LedgerInput] and return typed row slices. Callers
// are responsible for serialization (Parquet, PG inserts, protobuf, JSON).
package extract

import (
	"fmt"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// LedgerInput is the minimal context needed to run any extractor.
type LedgerInput struct {
	LCM               xdr.LedgerCloseMeta
	NetworkPassphrase string
	Sequence          uint32
	ClosedAt          time.Time
	LedgerRange       uint32  // partition key, typically floor(sequence / 10000)
	EraID             *string // optional DuckLake era identifier
}

// NewLedgerInput creates a LedgerInput from an already-decoded LedgerCloseMeta.
func NewLedgerInput(lcm xdr.LedgerCloseMeta, networkPassphrase string) *LedgerInput {
	seq := uint32(lcm.LedgerSequence())
	closedAt := time.Unix(int64(lcm.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0).UTC()
	return &LedgerInput{
		LCM:               lcm,
		NetworkPassphrase: networkPassphrase,
		Sequence:          seq,
		ClosedAt:          closedAt,
		LedgerRange:       (seq / 10000) * 10000,
	}
}

// NewLedgerInputFromXDR creates a LedgerInput from raw XDR bytes.
// This is the adapter for callers that receive bytes over gRPC or protobuf.
func NewLedgerInputFromXDR(xdrBytes []byte, networkPassphrase string) (*LedgerInput, error) {
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(xdrBytes); err != nil {
		return nil, fmt.Errorf("unmarshal LedgerCloseMeta: %w", err)
	}
	return NewLedgerInput(lcm, networkPassphrase), nil
}

// LedgerData holds all extracted rows from a single ledger.
type LedgerData struct {
	Ledgers           []LedgerRowData
	Transactions      []TransactionData
	Operations        []OperationData
	Effects           []EffectData
	Trades            []TradeData
	Accounts          []AccountData
	Offers            []OfferData
	Trustlines        []TrustlineData
	AccountSigners    []AccountSignerData
	ClaimableBalances []ClaimableBalanceData
	LiquidityPools    []LiquidityPoolData
	ConfigSettings    []ConfigSettingData
	TTLEntries        []TTLData
	NativeBalances    []NativeBalanceData
	ContractEvents    []ContractEventData
	ContractData      []ContractDataData
	ContractCode      []ContractCodeData
	ContractCreations []ContractCreationData
	TokenTransfers    []TokenTransferData
	EvictedKeys       []EvictedKeyData
	RestoredKeys      []RestoredKeyData
}

// ExtractAll runs all extractors concurrently and returns combined results.
// Errors from individual extractors are collected; partial results are returned.
func ExtractAll(input *LedgerInput) (*LedgerData, []error) {
	data := &LedgerData{}
	var mu sync.Mutex
	var errs []error
	var wg sync.WaitGroup

	run := func(name string, fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("panic in %s: %v", name, r))
					mu.Unlock()
				}
			}()
			fn()
		}()
	}

	collect := func(name string, fn func(*LedgerInput) error) {
		run(name, func() {
			if err := fn(input); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("%s: %w", name, err))
				mu.Unlock()
			}
		})
	}

	collect("ledgers", func(in *LedgerInput) error {
		rows, err := ExtractLedgers(in)
		if err == nil {
			mu.Lock()
			data.Ledgers = rows
			mu.Unlock()
		}
		return err
	})
	collect("transactions", func(in *LedgerInput) error {
		rows, err := ExtractTransactions(in)
		if err == nil {
			mu.Lock()
			data.Transactions = rows
			mu.Unlock()
		}
		return err
	})
	collect("operations", func(in *LedgerInput) error {
		rows, err := ExtractOperations(in)
		if err == nil {
			mu.Lock()
			data.Operations = rows
			mu.Unlock()
		}
		return err
	})
	collect("effects", func(in *LedgerInput) error {
		rows, err := ExtractEffects(in)
		if err == nil {
			mu.Lock()
			data.Effects = rows
			mu.Unlock()
		}
		return err
	})
	collect("trades", func(in *LedgerInput) error {
		rows, err := ExtractTrades(in)
		if err == nil {
			mu.Lock()
			data.Trades = rows
			mu.Unlock()
		}
		return err
	})
	collect("accounts", func(in *LedgerInput) error {
		rows, err := ExtractAccounts(in)
		if err == nil {
			mu.Lock()
			data.Accounts = rows
			mu.Unlock()
		}
		return err
	})
	collect("trustlines", func(in *LedgerInput) error {
		rows, err := ExtractTrustlines(in)
		if err == nil {
			mu.Lock()
			data.Trustlines = rows
			mu.Unlock()
		}
		return err
	})
	collect("account_signers", func(in *LedgerInput) error {
		rows, err := ExtractAccountSigners(in)
		if err == nil {
			mu.Lock()
			data.AccountSigners = rows
			mu.Unlock()
		}
		return err
	})
	collect("offers", func(in *LedgerInput) error {
		rows, err := ExtractOffers(in)
		if err == nil {
			mu.Lock()
			data.Offers = rows
			mu.Unlock()
		}
		return err
	})
	collect("claimable_balances", func(in *LedgerInput) error {
		rows, err := ExtractClaimableBalances(in)
		if err == nil {
			mu.Lock()
			data.ClaimableBalances = rows
			mu.Unlock()
		}
		return err
	})
	collect("liquidity_pools", func(in *LedgerInput) error {
		rows, err := ExtractLiquidityPools(in)
		if err == nil {
			mu.Lock()
			data.LiquidityPools = rows
			mu.Unlock()
		}
		return err
	})
	collect("config_settings", func(in *LedgerInput) error {
		rows, err := ExtractConfigSettings(in)
		if err == nil {
			mu.Lock()
			data.ConfigSettings = rows
			mu.Unlock()
		}
		return err
	})
	collect("ttl", func(in *LedgerInput) error {
		rows, err := ExtractTTL(in)
		if err == nil {
			mu.Lock()
			data.TTLEntries = rows
			mu.Unlock()
		}
		return err
	})
	collect("native_balances", func(in *LedgerInput) error {
		rows, err := ExtractNativeBalances(in)
		if err == nil {
			mu.Lock()
			data.NativeBalances = rows
			mu.Unlock()
		}
		return err
	})
	collect("contract_events", func(in *LedgerInput) error {
		rows, err := ExtractContractEvents(in)
		if err == nil {
			mu.Lock()
			data.ContractEvents = rows
			mu.Unlock()
		}
		return err
	})
	collect("contract_data", func(in *LedgerInput) error {
		rows, err := ExtractContractData(in)
		if err == nil {
			mu.Lock()
			data.ContractData = rows
			mu.Unlock()
		}
		return err
	})
	collect("contract_code", func(in *LedgerInput) error {
		rows, err := ExtractContractCode(in)
		if err == nil {
			mu.Lock()
			data.ContractCode = rows
			mu.Unlock()
		}
		return err
	})
	collect("contract_creations", func(in *LedgerInput) error {
		rows, err := ExtractContractCreations(in)
		if err == nil {
			mu.Lock()
			data.ContractCreations = rows
			mu.Unlock()
		}
		return err
	})
	collect("token_transfers", func(in *LedgerInput) error {
		rows, err := ExtractTokenTransfers(in)
		if err == nil {
			mu.Lock()
			data.TokenTransfers = rows
			mu.Unlock()
		}
		return err
	})
	collect("evicted_keys", func(in *LedgerInput) error {
		rows, err := ExtractEvictedKeys(in)
		if err == nil {
			mu.Lock()
			data.EvictedKeys = rows
			mu.Unlock()
		}
		return err
	})
	collect("restored_keys", func(in *LedgerInput) error {
		rows, err := ExtractRestoredKeys(in)
		if err == nil {
			mu.Lock()
			data.RestoredKeys = rows
			mu.Unlock()
		}
		return err
	})

	wg.Wait()
	return data, errs
}
