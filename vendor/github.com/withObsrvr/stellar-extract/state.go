package extract

import (
	"encoding/base64"
	"encoding/hex"
	"io"
	"log"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ExtractConfigSettings extracts network configuration settings from a ledger.
// Protocol 20+ Soroban configuration parameters.
//
// Config setting changes can be emitted as ledger-level changes during protocol
// upgrades and may not appear in per-transaction change streams. For that
// reason this extractor intentionally uses the ledger change reader rather than
// the transaction reader.
func ExtractConfigSettings(input *LedgerInput) ([]ConfigSettingData, error) {
	var configSettingsList []ConfigSettingData

	changeReader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(input.NetworkPassphrase, input.LCM)
	if err != nil {
		log.Printf("Failed to create ledger change reader for config settings: %v", err)
		return configSettingsList, nil
	}
	defer changeReader.Close()

	configSettingsMap := make(map[int32]*ConfigSettingData)

	for {
		change, err := changeReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading ledger change for config settings: %v", err)
			continue
		}
		if !isConfigSettingChange(change) {
			continue
		}

		var configEntry *xdr.ConfigSettingEntry
		var deleted bool
		var lastModifiedLedger uint32

		if change.Post != nil {
			entry, _ := change.Post.Data.GetConfigSetting()
			configEntry = &entry
			lastModifiedLedger = uint32(change.Post.LastModifiedLedgerSeq)
			deleted = false
		} else if change.Pre != nil {
			entry, _ := change.Pre.Data.GetConfigSetting()
			configEntry = &entry
			lastModifiedLedger = uint32(change.Pre.LastModifiedLedgerSeq)
			deleted = true
		}

		if configEntry == nil {
			continue
		}

		configSettingID := int32(configEntry.ConfigSettingId)
		now := time.Now().UTC()

		data := ConfigSettingData{
			ConfigSettingID: configSettingID,
			LedgerSequence:  input.Sequence,

			LastModifiedLedger: int32(lastModifiedLedger),
			Deleted:            deleted,
			ClosedAt:           input.ClosedAt,

			ConfigSettingXDR: encodeConfigSettingXDR(configEntry),

			CreatedAt:   now,
			LedgerRange: input.LedgerRange,
			EraID:       input.EraID,
		}

		parseConfigSettingFields(configEntry, &data)
		configSettingsMap[configSettingID] = &data
	}

	for _, data := range configSettingsMap {
		configSettingsList = append(configSettingsList, *data)
	}

	return configSettingsList, nil
}

// isConfigSettingChange checks if a change involves config settings.
func isConfigSettingChange(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeConfigSetting {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeConfigSetting {
		return true
	}
	return false
}

// parseConfigSettingFields extracts specific fields from config setting entry.
// For MVP, we store the full XDR which provides complete fidelity.
func parseConfigSettingFields(entry *xdr.ConfigSettingEntry, data *ConfigSettingData) {
	// Full config data is available in ConfigSettingXDR field
	// Individual fields can be parsed from XDR as needed
}

// encodeConfigSettingXDR encodes config setting entry to base64 XDR.
func encodeConfigSettingXDR(entry *xdr.ConfigSettingEntry) string {
	if entry == nil {
		return ""
	}

	xdrBytes, err := entry.MarshalBinary()
	if err != nil {
		log.Printf("Failed to encode config setting XDR: %v", err)
		return ""
	}

	return base64.StdEncoding.EncodeToString(xdrBytes)
}

// ExtractTTL extracts time-to-live (TTL) entries from a ledger.
// Protocol 20+ Soroban storage expiration tracking.
func ExtractTTL(input *LedgerInput) ([]TTLData, error) {
	var ttlList []TTLData

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(input.NetworkPassphrase, input.LCM)
	if err != nil {
		log.Printf("Failed to create transaction reader for TTL: %v", err)
		return ttlList, nil
	}
	defer txReader.Close()

	ttlMap := make(map[string]*TTLData)

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for TTL: %v", err)
			continue
		}

		changes, err := tx.GetChanges()
		if err != nil {
			log.Printf("Failed to get transaction changes: %v", err)
			continue
		}

		for _, change := range changes {
			if !isTTLChange(change) {
				continue
			}

			var ttlEntry *xdr.TtlEntry
			var deleted bool
			var lastModifiedLedger uint32

			if change.Post != nil {
				entry, _ := change.Post.Data.GetTtl()
				ttlEntry = &entry
				lastModifiedLedger = uint32(change.Post.LastModifiedLedgerSeq)
				deleted = false
			} else if change.Pre != nil {
				entry, _ := change.Pre.Data.GetTtl()
				ttlEntry = &entry
				lastModifiedLedger = uint32(change.Pre.LastModifiedLedgerSeq)
				deleted = true
			}

			if ttlEntry == nil {
				continue
			}

			keyHashBytes, err := ttlEntry.KeyHash.MarshalBinary()
			if err != nil {
				log.Printf("Failed to marshal key hash: %v", err)
				continue
			}
			keyHash := hex.EncodeToString(keyHashBytes)

			liveUntilLedgerSeq := uint32(ttlEntry.LiveUntilLedgerSeq)
			ttlRemaining := int64(liveUntilLedgerSeq) - int64(input.Sequence)
			expired := ttlRemaining <= 0

			now := time.Now().UTC()

			data := TTLData{
				KeyHash:        keyHash,
				LedgerSequence: input.Sequence,

				LiveUntilLedgerSeq: liveUntilLedgerSeq,
				TTLRemaining:       ttlRemaining,
				Expired:            expired,

				LastModifiedLedger: int32(lastModifiedLedger),
				Deleted:            deleted,
				ClosedAt:           input.ClosedAt,

				CreatedAt:   now,
				LedgerRange: input.LedgerRange,
				EraID:       input.EraID,
			}

			ttlMap[keyHash] = &data
		}
	}

	for _, data := range ttlMap {
		ttlList = append(ttlList, *data)
	}

	return ttlList, nil
}

// isTTLChange checks if a change involves TTL entries.
func isTTLChange(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeTtl {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeTtl {
		return true
	}
	return false
}
