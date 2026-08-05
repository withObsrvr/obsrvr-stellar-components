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

// parseConfigSettingFields promotes the settings that act as capacity
// denominators into their own columns.
//
// ConfigSettingXDR still carries the entry in full; these are the values a
// consumer needs to turn a usage number into a percentage, and reaching them
// otherwise means decoding the XDR at query time. Every setting arrives as its
// own entry, so a given call populates only the fields belonging to that one
// and leaves the rest nil.
//
// Protocol 23 renamed the read-side limits to "disk read" (CAP-0062 moved
// live state out of the disk-read budget). They keep their original column
// names here — the meaning consumers care about, the ledger-wide ceiling on
// entries a transaction may read, is unchanged.
func parseConfigSettingFields(entry *xdr.ConfigSettingEntry, data *ConfigSettingData) {
	if entry == nil {
		return
	}

	switch entry.ConfigSettingId {
	case xdr.ConfigSettingIdConfigSettingContractMaxSizeBytes:
		if v, ok := entry.GetContractMaxSizeBytes(); ok {
			data.ContractMaxSizeBytes = uint32Ptr(uint32(v))
		}

	case xdr.ConfigSettingIdConfigSettingContractComputeV0:
		if v, ok := entry.GetContractCompute(); ok {
			data.LedgerMaxInstructions = int64Ptr(int64(v.LedgerMaxInstructions))
			data.TxMaxInstructions = int64Ptr(int64(v.TxMaxInstructions))
			data.FeeRatePerInstructionsIncrement = int64Ptr(int64(v.FeeRatePerInstructionsIncrement))
			data.TxMemoryLimit = uint32Ptr(uint32(v.TxMemoryLimit))
		}

	case xdr.ConfigSettingIdConfigSettingContractLedgerCostV0:
		if v, ok := entry.GetContractLedgerCost(); ok {
			data.LedgerMaxReadLedgerEntries = uint32Ptr(uint32(v.LedgerMaxDiskReadEntries))
			data.LedgerMaxReadBytes = uint32Ptr(uint32(v.LedgerMaxDiskReadBytes))
			data.LedgerMaxWriteLedgerEntries = uint32Ptr(uint32(v.LedgerMaxWriteLedgerEntries))
			data.LedgerMaxWriteBytes = uint32Ptr(uint32(v.LedgerMaxWriteBytes))
			data.TxMaxReadLedgerEntries = uint32Ptr(uint32(v.TxMaxDiskReadEntries))
			data.TxMaxReadBytes = uint32Ptr(uint32(v.TxMaxDiskReadBytes))
			data.TxMaxWriteLedgerEntries = uint32Ptr(uint32(v.TxMaxWriteLedgerEntries))
			data.TxMaxWriteBytes = uint32Ptr(uint32(v.TxMaxWriteBytes))
		}

	case xdr.ConfigSettingIdConfigSettingContractLedgerCostExtV0:
		// CAP-0062 caps the total footprint entries a single transaction may
		// declare. It is the denominator for a per-transaction footprint
		// meter, which the ledger-wide limits above cannot express.
		if v, ok := entry.GetContractLedgerCostExt(); ok {
			data.TxMaxFootprintEntries = uint32Ptr(uint32(v.TxMaxFootprintEntries))
		}
	}
}

func uint32Ptr(v uint32) *uint32 { return &v }

func int64Ptr(v int64) *int64 { return &v }

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
