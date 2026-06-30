package extract

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ===========================================================================
// Contract Events
// ===========================================================================

// ExtractContractEvents extracts Soroban contract events from a ledger.
// Captures three event types: CONTRACT, SYSTEM, DIAGNOSTIC (includes invocation tree).
// Includes V4/CAP-67 TransactionEvents support.
func ExtractContractEvents(input *LedgerInput) ([]ContractEventData, error) {
	var events []ContractEventData

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(input.NetworkPassphrase, input.LCM)
	if err != nil {
		log.Printf("Failed to create transaction reader for contract events: %v", err)
		return events, nil
	}
	defer reader.Close()

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for contract events: %v", err)
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])
		txSuccessful := tx.Result.Successful()

		txEvents, err := tx.GetTransactionEvents()
		if err != nil {
			continue
		}

		// Extract diagnostic events (contains invocation tree)
		for diagIdx, diagEvent := range txEvents.DiagnosticEvents {
			eventData := extractDiagnosticEvent(diagEvent, txHash, input.Sequence, input.ClosedAt, input.LedgerRange, uint32(diagIdx), txSuccessful)
			eventData.EraID = input.EraID
			events = append(events, eventData)
		}

		// Extract operation-level contract events
		for opIdx, opEvents := range txEvents.OperationEvents {
			for eventIdx, contractEvent := range opEvents {
				eventData := extractContractEvent(contractEvent, txHash, input.Sequence, input.ClosedAt, input.LedgerRange, uint32(opIdx), uint32(eventIdx), false, txSuccessful)
				eventData.EraID = input.EraID
				events = append(events, eventData)
			}
		}

		// Extract transaction-level events (V4 / CAP-67 unified events).
		for txEvtIdx, txEvt := range txEvents.TransactionEvents {
			eventData := extractContractEvent(txEvt.Event, txHash, input.Sequence, input.ClosedAt, input.LedgerRange, 0, uint32(txEvtIdx), true, txSuccessful)
			eventData.EraID = input.EraID
			events = append(events, eventData)
		}
	}

	return events, nil
}

// extractDiagnosticEvent extracts data from a diagnostic event.
func extractDiagnosticEvent(diagEvent xdr.DiagnosticEvent, txHash string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32, diagIdx uint32, txSuccessful bool) ContractEventData {
	eventData := extractContractEvent(
		diagEvent.Event,
		txHash,
		ledgerSeq,
		closedAt,
		ledgerRange,
		diagIdx,
		0,
		diagEvent.InSuccessfulContractCall,
		txSuccessful,
	)
	eventData.EventType = "diagnostic"
	return eventData
}

// extractContractEvent extracts data from a ContractEvent.
func extractContractEvent(
	event xdr.ContractEvent,
	txHash string,
	ledgerSeq uint32,
	closedAt time.Time,
	ledgerRange uint32,
	opIndex uint32,
	eventIndex uint32,
	inSuccessfulCall bool,
	txSuccessful bool,
) ContractEventData {
	eventID := fmt.Sprintf("%s:%d:%d", txHash, opIndex, eventIndex)

	// Encode contract ID as C-address (matching block explorer format)
	var contractID *string
	if event.ContractId != nil {
		if cAddr, err := strkey.Encode(strkey.VersionByteContract, (*event.ContractId)[:]); err == nil {
			contractID = &cAddr
		}
	}

	// Base64-encode the full ContractEvent XDR for reprocessing
	var contractEventXDR string
	if xdrBytes, err := event.MarshalBinary(); err == nil {
		contractEventXDR = base64.StdEncoding.EncodeToString(xdrBytes)
	}

	eventType := eventTypeString(event.Type)
	topicsJSON, topicsDecoded, topicCount, dataXDR, dataDecoded, positionalTopics := extractEventBody(event.Body)

	now := time.Now().UTC()

	return ContractEventData{
		EventID:         eventID,
		ContractID:      contractID,
		LedgerSequence:  ledgerSeq,
		TransactionHash: txHash,
		ClosedAt:        closedAt,

		EventType:                eventType,
		InSuccessfulContractCall: inSuccessfulCall,
		Successful:               txSuccessful,
		ContractEventXDR:         contractEventXDR,

		TopicsJSON:    topicsJSON,
		TopicsDecoded: topicsDecoded,
		DataXDR:       dataXDR,
		DataDecoded:   dataDecoded,
		TopicCount:    topicCount,

		Topic0Decoded: positionalTopics[0],
		Topic1Decoded: positionalTopics[1],
		Topic2Decoded: positionalTopics[2],
		Topic3Decoded: positionalTopics[3],

		OperationIndex: opIndex,
		EventIndex:     eventIndex,

		CreatedAt:   now,
		LedgerRange: ledgerRange,
	}
}

// extractEventBody extracts topics and data from ContractEventBody.
// Returns: (topicsJSON, topicsDecoded, topicCount, dataXDR, dataDecoded, positionalTopics)
func extractEventBody(body xdr.ContractEventBody) (string, string, int32, string, string, [4]*string) {
	var positionalTopics [4]*string

	if body.V != 0 {
		log.Printf("Unknown ContractEventBody version: %d", body.V)
		return "[]", "[]", 0, "", "{}", positionalTopics
	}

	v0 := body.MustV0()

	topicsXDR := []string{}
	topicsDecodedArray := []interface{}{}
	for i, topic := range v0.Topics {
		topicBytes, err := topic.MarshalBinary()
		if err != nil {
			log.Printf("Failed to marshal topic: %v", err)
			continue
		}
		topicsXDR = append(topicsXDR, base64.StdEncoding.EncodeToString(topicBytes))

		decodedTopic, err := ConvertScValToJSON(topic)
		if err != nil {
			log.Printf("Failed to decode topic: %v", err)
			topicsDecodedArray = append(topicsDecodedArray, map[string]interface{}{
				"error": err.Error(),
				"type":  topic.Type.String(),
			})
		} else {
			topicsDecodedArray = append(topicsDecodedArray, decodedTopic)
			if i < 4 {
				positionalTopics[i] = flattenTopicValue(decodedTopic)
			}
		}
	}

	topicsJSON, err := json.Marshal(topicsXDR)
	if err != nil {
		log.Printf("Failed to marshal topics JSON: %v", err)
		return "[]", "[]", 0, "", "{}", positionalTopics
	}

	topicsDecodedJSON, err := json.Marshal(topicsDecodedArray)
	if err != nil {
		log.Printf("Failed to marshal decoded topics: %v", err)
		topicsDecodedJSON = []byte("[]")
	}

	topicCount := int32(len(v0.Topics))

	dataBytes, err := v0.Data.MarshalBinary()
	if err != nil {
		log.Printf("Failed to marshal data: %v", err)
		return string(topicsJSON), string(topicsDecodedJSON), topicCount, "", "{}", positionalTopics
	}

	dataXDR := base64.StdEncoding.EncodeToString(dataBytes)

	decodedData, err := ConvertScValToJSON(v0.Data)
	if err != nil {
		log.Printf("Failed to decode data: %v", err)
		decodedData = map[string]interface{}{
			"error": err.Error(),
			"type":  v0.Data.Type.String(),
		}
	}

	dataDecodedJSON, err := json.Marshal(decodedData)
	if err != nil {
		log.Printf("Failed to marshal decoded data: %v", err)
		dataDecodedJSON = []byte("{}")
	}

	return string(topicsJSON), string(topicsDecodedJSON), topicCount, dataXDR, string(dataDecodedJSON), positionalTopics
}

// eventTypeString converts ContractEventType to string.
func eventTypeString(t xdr.ContractEventType) string {
	switch t {
	case xdr.ContractEventTypeSystem:
		return "system"
	case xdr.ContractEventTypeContract:
		return "contract"
	case xdr.ContractEventTypeDiagnostic:
		return "diagnostic"
	default:
		return "unknown"
	}
}

// flattenTopicValue extracts a query-friendly string from a decoded topic value.
func flattenTopicValue(decoded interface{}) *string {
	switch v := decoded.(type) {
	case string:
		return &v
	case map[string]interface{}:
		if addr, ok := v["address"].(string); ok {
			return &addr
		}
		if val, ok := v["value"].(string); ok {
			return &val
		}
		b, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		s := string(b)
		return &s
	default:
		s := fmt.Sprintf("%v", v)
		return &s
	}
}

// ===========================================================================
// Contract Data (with SAC detection and token metadata)
// ===========================================================================

// ExtractContractData extracts contract data state from a ledger.
// Uses LedgerChangeReader directly instead of per-transaction reader.
func ExtractContractData(input *LedgerInput) ([]ContractDataData, error) {
	var contractDataList []ContractDataData

	changeReader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(input.NetworkPassphrase, input.LCM)
	if err != nil {
		log.Printf("Failed to create change reader for contract data: %v", err)
		return contractDataList, nil
	}
	defer changeReader.Close()

	// Track unique contract data by key (deduplicate within ledger)
	contractMap := make(map[string]*ContractDataData)

	for {
		change, err := changeReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for contract data: %v", err)
			continue
		}

		if !isContractDataChange(change) {
			continue
		}

		// Determine the active entry (post for creates/updates, pre for deletes)
		var entry *xdr.LedgerEntry
		var deleted bool

		if change.Post != nil {
			entry = change.Post
			deleted = false
		} else if change.Pre != nil {
			entry = change.Pre
			deleted = true
		} else {
			continue
		}

		contractData, ok := entry.Data.GetContractData()
		if !ok {
			continue
		}

		// Skip nonce keys
		if contractData.Key.Type == xdr.ScValTypeScvLedgerKeyNonce {
			continue
		}

		// Extract contract ID from ScAddress
		contractIDHash, ok := contractData.Contract.GetContractId()
		if !ok {
			continue
		}
		contractIDBytes, err := contractIDHash.MarshalBinary()
		if err != nil {
			continue
		}
		contractIDStr, err := strkey.Encode(strkey.VersionByteContract, contractIDBytes)
		if err != nil {
			continue
		}

		// Generate ledger key hash
		var ledgerKeyHash string
		ledgerKey, err := entry.LedgerKey()
		if err == nil {
			keyBytes, err := ledgerKey.MarshalBinary()
			if err == nil {
				hash := sha256.Sum256(keyBytes)
				ledgerKeyHash = hex.EncodeToString(hash[:])
			}
		}

		// Key type and durability
		keyType := contractData.Key.Type.String()
		durability := contractData.Durability.String()

		// Encode contract data as base64 XDR
		var contractDataXDR string
		if xdrBytes, err := contractData.MarshalBinary(); err == nil {
			contractDataXDR = base64.StdEncoding.EncodeToString(xdrBytes)
		}

		now := time.Now().UTC()

		// SAC detection: check if this is a Stellar Asset Contract entry
		var assetCode, assetIssuer, assetType, balanceHolder, balance *string
		if asset, ok := sac.AssetFromContractData(*entry, input.NetworkPassphrase); ok {
			if asset.IsNative() {
				at := "native"
				assetType = &at
			} else {
				code := asset.GetCode()
				issuer := asset.GetIssuer()
				assetCode = &code
				assetIssuer = &issuer
				if len(code) <= 4 {
					at := "credit_alphanum4"
					assetType = &at
				} else {
					at := "credit_alphanum12"
					assetType = &at
				}
			}
		}

		// SAC balance detection
		if holder, amt, ok := sac.ContractBalanceFromContractData(*entry, input.NetworkPassphrase); ok {
			holderStr, err := strkey.Encode(strkey.VersionByteContract, holder[:])
			if err == nil {
				balanceHolder = &holderStr
			}
			amtStr := amt.String()
			balance = &amtStr
		}

		// Token metadata extraction from instance storage
		var tokenName, tokenSymbol *string
		var tokenDecimals *int32
		if contractData.Key.Type == xdr.ScValTypeScvLedgerKeyContractInstance {
			tokenName, tokenSymbol, tokenDecimals = extractTokenMetadata(contractData)
		}

		// Deduplicate key
		dedupeKey := fmt.Sprintf("%s_%s_%s", contractIDStr, keyType, ledgerKeyHash)

		data := ContractDataData{
			ContractId:     contractIDStr,
			LedgerSequence: input.Sequence,
			LedgerKeyHash:  ledgerKeyHash,

			ContractKeyType:    keyType,
			ContractDurability: durability,

			AssetCode:   assetCode,
			AssetIssuer: assetIssuer,
			AssetType:   assetType,

			BalanceHolder: balanceHolder,
			Balance:       balance,

			LastModifiedLedger: int32(entry.LastModifiedLedgerSeq),
			LedgerEntryChange:  int32(change.Type),
			Deleted:            deleted,
			ClosedAt:           input.ClosedAt,

			ContractDataXDR: contractDataXDR,

			TokenName:     tokenName,
			TokenSymbol:   tokenSymbol,
			TokenDecimals: tokenDecimals,

			CreatedAt:   now,
			LedgerRange: input.LedgerRange,
			EraID:       input.EraID,
		}

		contractMap[dedupeKey] = &data
	}

	for _, data := range contractMap {
		contractDataList = append(contractDataList, *data)
	}

	return contractDataList, nil
}

// isContractDataChange checks if a change involves contract data.
func isContractDataChange(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeContractData {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeContractData {
		return true
	}
	return false
}

// nullableString converts empty strings to nil pointers.
func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	str := s
	return &str
}

// extractTokenMetadata extracts token name, symbol, and decimals from
// a contract instance storage entry's metadata map.
func extractTokenMetadata(contractData xdr.ContractDataEntry) (*string, *string, *int32) {
	// Instance storage has the contract's metadata in the Val field
	if contractData.Val.Type != xdr.ScValTypeScvContractInstance {
		return nil, nil, nil
	}

	instance, ok := contractData.Val.GetInstance()
	if !ok || instance.Storage == nil {
		return nil, nil, nil
	}

	var name, symbol *string
	var decimals *int32

	for _, entry := range *instance.Storage {
		// Look for symbol keys like "METADATA"
		if entry.Key.Type == xdr.ScValTypeScvSymbol && entry.Key.Sym != nil {
			sym := string(*entry.Key.Sym)
			switch sym {
			case "METADATA":
				// METADATA is typically a map with name, symbol, decimals
				if entry.Val.Type == xdr.ScValTypeScvMap && entry.Val.Map != nil {
					for _, item := range **entry.Val.Map {
						if item.Key.Type == xdr.ScValTypeScvSymbol && item.Key.Sym != nil {
							key := string(*item.Key.Sym)
							switch key {
							case "name":
								if item.Val.Type == xdr.ScValTypeScvString && item.Val.Str != nil {
									s := string(*item.Val.Str)
									name = &s
								}
							case "symbol":
								if item.Val.Type == xdr.ScValTypeScvString && item.Val.Str != nil {
									s := string(*item.Val.Str)
									symbol = &s
								}
							case "decimal", "decimals":
								if item.Val.Type == xdr.ScValTypeScvU32 && item.Val.U32 != nil {
									d := int32(*item.Val.U32)
									decimals = &d
								}
							}
						}
					}
				}
			}
		}
	}

	return name, symbol, decimals
}

// Ensure math/big is used (for SAC balance detection)
var _ = (*big.Int)(nil)

// ===========================================================================
// Contract Code (WASM)
// ===========================================================================

// ExtractContractCode extracts contract code (WASM) from a ledger.
func ExtractContractCode(input *LedgerInput) ([]ContractCodeData, error) {
	var contractCodeList []ContractCodeData

	changeReader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(input.NetworkPassphrase, input.LCM)
	if err != nil {
		log.Printf("Failed to create change reader for contract code: %v", err)
		return contractCodeList, nil
	}
	defer changeReader.Close()

	contractCodeMap := make(map[string]*ContractCodeData)

	for {
		change, err := changeReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for contract code: %v", err)
			continue
		}

		if !isContractCodeChange(change) {
			continue
		}

		var contractCode *xdr.ContractCodeEntry
		var deleted bool
		var lastModifiedLedger uint32
		var ledgerKeyHash string

		if change.Post != nil {
			codeEntry, _ := change.Post.Data.GetContractCode()
			contractCode = &codeEntry
			lastModifiedLedger = uint32(change.Post.LastModifiedLedgerSeq)
			deleted = false

			if key, err := change.Post.LedgerKey(); err == nil {
				if keyBytes, err := key.MarshalBinary(); err == nil {
					ledgerKeyHash = hex.EncodeToString(keyBytes)
				}
			}
		} else if change.Pre != nil {
			codeEntry, _ := change.Pre.Data.GetContractCode()
			contractCode = &codeEntry
			lastModifiedLedger = uint32(change.Pre.LastModifiedLedgerSeq)
			deleted = true

			if key, err := change.Pre.LedgerKey(); err == nil {
				if keyBytes, err := key.MarshalBinary(); err == nil {
					ledgerKeyHash = hex.EncodeToString(keyBytes)
				}
			}
		}

		if contractCode == nil {
			continue
		}

		now := time.Now().UTC()
		codeHashHex := hex.EncodeToString(contractCode.Hash[:])

		var extV int32
		if contractCode.Ext.V == 1 && contractCode.Ext.V1 != nil {
			extV = 1
		}

		wasmMetadata := parseWASMMetadata(contractCode.Code)

		data := ContractCodeData{
			ContractCodeHash: codeHashHex,
			LedgerKeyHash:    ledgerKeyHash,

			ContractCodeExtV: extV,

			LastModifiedLedger: int32(lastModifiedLedger),
			LedgerEntryChange:  int32(change.Type),
			Deleted:            deleted,
			ClosedAt:           input.ClosedAt,

			LedgerSequence: input.Sequence,

			NInstructions:     wasmMetadata.NInstructions,
			NFunctions:        wasmMetadata.NFunctions,
			NGlobals:          wasmMetadata.NGlobals,
			NTableEntries:     wasmMetadata.NTableEntries,
			NTypes:            wasmMetadata.NTypes,
			NDataSegments:     wasmMetadata.NDataSegments,
			NElemSegments:     wasmMetadata.NElemSegments,
			NImports:          wasmMetadata.NImports,
			NExports:          wasmMetadata.NExports,
			NDataSegmentBytes: wasmMetadata.NDataSegmentBytes,

			CreatedAt:   now,
			LedgerRange: input.LedgerRange,
			EraID:       input.EraID,
		}

		contractCodeMap[codeHashHex] = &data
	}

	for _, data := range contractCodeMap {
		contractCodeList = append(contractCodeList, *data)
	}

	return contractCodeList, nil
}

// isContractCodeChange checks if a change involves contract code.
func isContractCodeChange(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeContractCode {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeContractCode {
		return true
	}
	return false
}

// parseWASMMetadata parses WASM bytecode to extract metadata.
func parseWASMMetadata(wasmCode []byte) WASMMetadata {
	metadata := WASMMetadata{}

	if len(wasmCode) < 8 {
		return metadata
	}

	// Check magic number
	if wasmCode[0] != 0x00 || wasmCode[1] != 0x61 || wasmCode[2] != 0x73 || wasmCode[3] != 0x6D {
		return metadata
	}

	offset := 8 // Skip magic + version

	for offset < len(wasmCode) {
		if offset+1 >= len(wasmCode) {
			break
		}

		sectionType := wasmCode[offset]
		offset++

		sectionSize, bytesRead := decodeLEB128(wasmCode[offset:])
		offset += bytesRead

		if sectionSize < 0 || offset+int(sectionSize) > len(wasmCode) {
			break
		}

		sectionData := wasmCode[offset : offset+int(sectionSize)]
		offset += int(sectionSize)

		switch sectionType {
		case 1: // Type section
			count := countWASMElements(sectionData)
			metadata.NTypes = &count
		case 2: // Import section
			count := countWASMElements(sectionData)
			metadata.NImports = &count
		case 3: // Function section
			count := countWASMElements(sectionData)
			metadata.NFunctions = &count
		case 4: // Table section
			count := countWASMElements(sectionData)
			metadata.NTableEntries = &count
		case 6: // Global section
			count := countWASMElements(sectionData)
			metadata.NGlobals = &count
		case 7: // Export section
			count := countWASMElements(sectionData)
			metadata.NExports = &count
		case 9: // Element section
			count := countWASMElements(sectionData)
			metadata.NElemSegments = &count
		case 10: // Code section (contains instructions)
			count := countWASMElements(sectionData)
			if count > 0 {
				instrCount := int64(len(sectionData))
				metadata.NInstructions = &instrCount
			}
		case 11: // Data section
			count := countWASMElements(sectionData)
			metadata.NDataSegments = &count
			dataSize := int64(len(sectionData))
			metadata.NDataSegmentBytes = &dataSize
		}
	}

	return metadata
}

// countWASMElements counts the number of elements in a WASM section.
func countWASMElements(data []byte) int64 {
	if len(data) == 0 {
		return 0
	}
	count, _ := decodeLEB128(data)
	return count
}

// decodeLEB128 decodes a signed LEB128 encoded integer.
func decodeLEB128(data []byte) (int64, int) {
	var result int64
	var shift uint
	var bytesRead int

	for bytesRead = 0; bytesRead < len(data) && bytesRead < 10; bytesRead++ {
		b := data[bytesRead]
		result |= int64(b&0x7F) << shift

		if b&0x80 == 0 {
			if shift < 64 && (b&0x40) != 0 {
				result |= -(1 << (shift + 7))
			}
			return result, bytesRead + 1
		}

		shift += 7
	}

	return 0, bytesRead
}

// ===========================================================================
// Restored Keys
// ===========================================================================

// ExtractRestoredKeys extracts restored storage keys from a ledger.
func ExtractRestoredKeys(input *LedgerInput) ([]RestoredKeyData, error) {
	var restoredKeysList []RestoredKeyData

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(input.NetworkPassphrase, input.LCM)
	if err != nil {
		log.Printf("Failed to create transaction reader for restored keys: %v", err)
		return restoredKeysList, nil
	}
	defer reader.Close()

	restoredKeysMap := make(map[string]*RestoredKeyData)

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for restored keys: %v", err)
			continue
		}

		if !tx.Result.Successful() {
			continue
		}

		envelope := tx.Envelope
		var operations []xdr.Operation

		switch envelope.Type {
		case xdr.EnvelopeTypeEnvelopeTypeTx:
			operations = envelope.V1.Tx.Operations
		case xdr.EnvelopeTypeEnvelopeTypeTxV0:
			operations = envelope.V0.Tx.Operations
		case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
			innerTx := envelope.FeeBump.Tx.InnerTx
			if innerTx.Type == xdr.EnvelopeTypeEnvelopeTypeTx {
				operations = innerTx.V1.Tx.Operations
			}
		default:
			continue
		}

		for _, op := range operations {
			if op.Body.Type != xdr.OperationTypeRestoreFootprint {
				continue
			}

			var footprint *xdr.LedgerFootprint

			switch envelope.Type {
			case xdr.EnvelopeTypeEnvelopeTypeTx:
				if envelope.V1.Tx.Ext.V == 1 && envelope.V1.Tx.Ext.SorobanData != nil {
					footprint = &envelope.V1.Tx.Ext.SorobanData.Resources.Footprint
				}
			}

			if footprint == nil {
				continue
			}

			restoredFromLedger := uint32(0)

			for _, key := range footprint.ReadWrite {
				durability := "unknown"
				data := extractRestoredKeyData(key, input.Sequence, input.LedgerRange, durability, restoredFromLedger, input.ClosedAt)
				if data != nil {
					data.EraID = input.EraID
					restoredKeysMap[data.KeyHash] = data
				}
			}
		}
	}

	for _, data := range restoredKeysMap {
		restoredKeysList = append(restoredKeysList, *data)
	}

	return restoredKeysList, nil
}

// extractRestoredKeyData extracts data from a single restored ledger key.
func extractRestoredKeyData(ledgerKey xdr.LedgerKey, ledgerSeq uint32, ledgerRange uint32, durability string, restoredFromLedger uint32, closedAt time.Time) *RestoredKeyData {
	keyHashBytes, err := ledgerKey.MarshalBinary()
	if err != nil {
		log.Printf("Failed to marshal restored ledger key: %v", err)
		return nil
	}

	hash := sha256.Sum256(keyHashBytes)
	keyHash := hex.EncodeToString(hash[:])

	var contractID string
	var keyType string

	switch ledgerKey.Type {
	case xdr.LedgerEntryTypeContractData:
		if ledgerKey.ContractData == nil {
			log.Printf("Warning: ContractData is nil for ledger %d, key type %v", ledgerSeq, ledgerKey.Type)
			keyType = "ContractData"
		} else {
			if ledgerKey.ContractData.Contract.ContractId != nil {
				contractIDBytes, err := ledgerKey.ContractData.Contract.ContractId.MarshalBinary()
				if err == nil {
					contractID = hex.EncodeToString(contractIDBytes)
				}
			}
			keyType = ledgerKey.ContractData.Key.Type.String()
		}

	case xdr.LedgerEntryTypeContractCode:
		if ledgerKey.ContractCode == nil {
			log.Printf("Warning: ContractCode is nil for ledger %d, key type %v", ledgerSeq, ledgerKey.Type)
			keyType = "ContractCode"
		} else {
			codeHashBytes, err := ledgerKey.ContractCode.Hash.MarshalBinary()
			if err == nil {
				contractID = hex.EncodeToString(codeHashBytes)
			}
			keyType = "ContractCode"
		}

	default:
		keyType = ledgerKey.Type.String()
	}

	now := time.Now().UTC()

	data := RestoredKeyData{
		KeyHash:        keyHash,
		LedgerSequence: ledgerSeq,

		ContractID:         contractID,
		KeyType:            keyType,
		Durability:         durability,
		RestoredFromLedger: restoredFromLedger,

		ClosedAt:    closedAt,
		LedgerRange: ledgerRange,
		CreatedAt:   now,
	}

	return &data
}

// ===========================================================================
// Contract Creations
// ===========================================================================

// ExtractContractCreations extracts contract creation events from a ledger.
// Looks for InvokeHostFunction ops with HOST_FUNCTION_TYPE_CREATE_CONTRACT or CREATE_CONTRACT_V2.
func ExtractContractCreations(input *LedgerInput) ([]ContractCreationData, error) {
	var creations []ContractCreationData

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(input.NetworkPassphrase, input.LCM)
	if err != nil {
		return creations, nil
	}
	defer reader.Close()

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		if !tx.Result.Successful() {
			continue
		}

		txSourceAccount := tx.Envelope.SourceAccount().ToAccountId().Address()

		for opIdx, op := range tx.Envelope.Operations() {
			invokeHostFn, ok := op.Body.GetInvokeHostFunctionOp()
			if !ok {
				continue
			}

			fnType := invokeHostFn.HostFunction.Type
			if fnType != xdr.HostFunctionTypeHostFunctionTypeCreateContract &&
				fnType != xdr.HostFunctionTypeHostFunctionTypeCreateContractV2 {
				continue
			}

			creatorAddress := txSourceAccount
			if op.SourceAccount != nil {
				creatorAddress = op.SourceAccount.ToAccountId().Address()
			}

			// Get the created contract ID from the transaction meta
			var changes xdr.LedgerEntryChanges
			switch tx.UnsafeMeta.V {
			case 3:
				v3 := tx.UnsafeMeta.MustV3()
				if v3.SorobanMeta != nil && opIdx < len(v3.Operations) {
					changes = v3.Operations[opIdx].Changes
				}
			case 4:
				v4 := tx.UnsafeMeta.MustV4()
				if v4.SorobanMeta != nil && opIdx < len(v4.Operations) {
					changes = v4.Operations[opIdx].Changes
				}
			}

			if len(changes) > 0 {
				for _, change := range changes {
					created, ok := change.GetCreated()
					if !ok {
						continue
					}
					contractData, ok := created.Data.GetContractData()
					if !ok {
						continue
					}

					if contractData.Durability == xdr.ContractDataDurabilityPersistent {
						if scAddr, ok := contractData.Contract.GetContractId(); ok {
							contractIDStr, err := strkey.Encode(strkey.VersionByteContract, scAddr[:])
							if err != nil {
								continue
							}

							creation := ContractCreationData{
								ContractID:     contractIDStr,
								CreatorAddress: creatorAddress,
								CreatedLedger:  input.Sequence,
								CreatedAt:      input.ClosedAt,
								LedgerRange:    input.LedgerRange,
								EraID:          input.EraID,
							}

							// Try to extract WASM hash
							if fnType == xdr.HostFunctionTypeHostFunctionTypeCreateContract {
								if args, ok := invokeHostFn.HostFunction.GetCreateContract(); ok {
									if wasmHash, ok := args.Executable.GetWasmHash(); ok {
										wasmHashStr := hex.EncodeToString(wasmHash[:])
										creation.WasmHash = &wasmHashStr
									}
								}
							} else if fnType == xdr.HostFunctionTypeHostFunctionTypeCreateContractV2 {
								if args, ok := invokeHostFn.HostFunction.GetCreateContractV2(); ok {
									if wasmHash, ok := args.Executable.GetWasmHash(); ok {
										wasmHashStr := hex.EncodeToString(wasmHash[:])
										creation.WasmHash = &wasmHashStr
									}
								}
							}

							creations = append(creations, creation)
							break // One contract per op
						}
					}
				}
			}
		}
	}

	return creations, nil
}
