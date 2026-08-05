package extract

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// contractErrorFacts reports the first ScError found in a transaction's
// diagnostic events.
//
// The first is used rather than the last because a contract that errors
// unwinds, and the events emitted during unwinding describe the unwinding
// rather than the cause. The first error is the one that actually stopped
// execution.
//
// Returns present=false when the transaction emitted no ScError at all, which
// covers every successful transaction and every classic failure.
func contractErrorFacts(tx ingest.LedgerTransaction) (errorType string, errorCode *int32, present bool) {
	meta := tx.UnsafeMeta
	events, err := meta.GetDiagnosticEvents()
	if err != nil {
		// Meta versions that carry no diagnostic events are not an error
		// condition; they simply have nothing to report.
		return "", nil, false
	}

	for _, event := range events {
		body, ok := event.Event.Body.GetV0()
		if !ok {
			continue
		}
		for _, topic := range body.Topics {
			if scErr, ok := topic.GetError(); ok {
				return scErrorFacts(scErr)
			}
		}
		if scErr, ok := body.Data.GetError(); ok {
			return scErrorFacts(scErr)
		}
	}
	return "", nil, false
}

func scErrorFacts(scErr xdr.ScError) (string, *int32, bool) {
	errorType := canonicalScErrorType(scErr.Type)

	// Only contract-defined errors carry a number. A host error reports its
	// type with no code rather than a misleading zero.
	if scErr.Type == xdr.ScErrorTypeSceContract && scErr.ContractCode != nil {
		code := int32(*scErr.ContractCode)
		return errorType, &code, true
	}
	return errorType, nil, true
}

// canonicalScErrorType renders the XDR enum as a lowercase snake_case string.
// Unknown future types keep their numeric identity rather than being folded
// into an existing bucket — a mislabelled failure cause is worse than an
// obviously unrecognised one.
func canonicalScErrorType(errorType xdr.ScErrorType) string {
	switch errorType {
	case xdr.ScErrorTypeSceContract:
		return "contract"
	case xdr.ScErrorTypeSceWasmVm:
		return "wasm_vm"
	case xdr.ScErrorTypeSceContext:
		return "context"
	case xdr.ScErrorTypeSceStorage:
		return "storage"
	case xdr.ScErrorTypeSceObject:
		return "object"
	case xdr.ScErrorTypeSceCrypto:
		return "crypto"
	case xdr.ScErrorTypeSceEvents:
		return "events"
	case xdr.ScErrorTypeSceBudget:
		return "budget"
	case xdr.ScErrorTypeSceValue:
		return "value"
	case xdr.ScErrorTypeSceAuth:
		return "auth"
	default:
		return fmt.Sprintf("unknown_%d", int32(errorType))
	}
}
