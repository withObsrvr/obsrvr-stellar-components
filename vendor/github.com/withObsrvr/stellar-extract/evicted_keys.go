package extract

import (
	"crypto/sha256"
	"encoding/hex"
	"log"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// ExtractEvictedKeys extracts evicted storage keys from a ledger.
// Protocol 20+ Soroban archival tracking (V2-only).
func ExtractEvictedKeys(input *LedgerInput) ([]EvictedKeyData, error) {
	var evictedKeysList []EvictedKeyData

	// Only LedgerCloseMetaV2 has evicted keys tracking
	if input.LCM.V != 2 {
		return evictedKeysList, nil
	}

	v2Meta := input.LCM.MustV2()

	// Track unique evicted keys by hash (deduplicate within ledger)
	evictedKeysMap := make(map[string]*EvictedKeyData)

	// Process evicted keys from V2 meta
	for _, evictedKey := range v2Meta.EvictedKeys {
		// Determine durability based on key type
		durability := "unknown"

		data := extractEvictedKeyData(evictedKey, input.Sequence, durability, input.ClosedAt, input.LedgerRange, input.EraID)
		if data != nil {
			evictedKeysMap[data.KeyHash] = data
		}
	}

	// Convert map to slice
	for _, data := range evictedKeysMap {
		evictedKeysList = append(evictedKeysList, *data)
	}

	return evictedKeysList, nil
}

// extractEvictedKeyData extracts data from a single evicted ledger key
func extractEvictedKeyData(ledgerKey xdr.LedgerKey, ledgerSeq uint32, durability string, closedAt time.Time, ledgerRange uint32, eraID *string) *EvictedKeyData {
	// Generate SHA256 hash of the ledger key
	keyHashBytes, err := ledgerKey.MarshalBinary()
	if err != nil {
		log.Printf("Failed to marshal evicted ledger key: %v", err)
		return nil
	}

	hash := sha256.Sum256(keyHashBytes)
	keyHash := hex.EncodeToString(hash[:])

	// Extract contract ID and key type based on ledger entry type
	var contractID string
	var keyType string

	switch ledgerKey.Type {
	case xdr.LedgerEntryTypeContractData:
		// Explicit nil checks to prevent panics
		if ledgerKey.ContractData == nil {
			log.Printf("Warning: ContractData is nil for ledger %d, key type %v", ledgerSeq, ledgerKey.Type)
			keyType = "ContractData"
		} else {
			// Safely extract contract ID
			contractBytes, err := ledgerKey.ContractData.Contract.MarshalBinary()
			if err == nil && len(contractBytes) > 0 {
				// Use hash of the full contract address as ID
				contractHash := sha256.Sum256(contractBytes)
				contractID = hex.EncodeToString(contractHash[:])
			}

			// Safely extract key type from ScVal
			keyType = "ContractData"
			if keyBytes, err := ledgerKey.ContractData.Key.MarshalBinary(); err == nil && len(keyBytes) > 0 {
				keyType = ledgerKey.ContractData.Key.Type.String()
			}
		}

	case xdr.LedgerEntryTypeContractCode:
		// Explicit nil checks to prevent panics
		if ledgerKey.ContractCode == nil {
			log.Printf("Warning: ContractCode is nil for ledger %d, key type %v", ledgerSeq, ledgerKey.Type)
			keyType = "ContractCode"
		} else {
			// Safely extract contract code hash
			codeHashBytes, err := ledgerKey.ContractCode.Hash.MarshalBinary()
			if err == nil && len(codeHashBytes) > 0 {
				contractID = hex.EncodeToString(codeHashBytes)
			}
			keyType = "ContractCode"
		}

	default:
		// Other ledger entry types (unlikely to be evicted)
		keyType = ledgerKey.Type.String()
	}

	// Create evicted key entry
	now := time.Now().UTC()

	data := EvictedKeyData{
		KeyHash:        keyHash,
		LedgerSequence: ledgerSeq,
		ContractID:     contractID,
		KeyType:        keyType,
		Durability:     durability,
		ClosedAt:       closedAt,
		LedgerRange:    ledgerRange,
		CreatedAt:      now,
		EraID:          eraID,
	}

	return &data
}
