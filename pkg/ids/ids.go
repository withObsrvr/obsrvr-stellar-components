package ids

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

func Ledger(network string, sequence uint32) string {
	return stable("ledger", network, fmt.Sprintf("%d", sequence))
}

func Transaction(network string, sequence uint32, index uint32, hash string) string {
	return stable("tx", network, fmt.Sprintf("%d", sequence), fmt.Sprintf("%d", index), hash)
}

func Operation(network string, sequence uint32, txIndex uint32, opIndex uint32) string {
	return stable("op", network, fmt.Sprintf("%d", sequence), fmt.Sprintf("%d", txIndex), fmt.Sprintf("%d", opIndex))
}

func ContractEvent(network string, sequence uint32, txIndex uint32, eventIndex uint32) string {
	return stable("contract-event", network, fmt.Sprintf("%d", sequence), fmt.Sprintf("%d", txIndex), fmt.Sprintf("%d", eventIndex))
}

func ContractInvocation(network string, sequence uint32, txIndex uint32, invocationIndex uint32) string {
	return stable("contract-invocation", network, fmt.Sprintf("%d", sequence), fmt.Sprintf("%d", txIndex), fmt.Sprintf("%d", invocationIndex))
}

func TokenTransfer(network string, sequence uint32, txIndex uint32, opIndex uint32) string {
	return stable("token-transfer", network, fmt.Sprintf("%d", sequence), fmt.Sprintf("%d", txIndex), fmt.Sprintf("%d", opIndex))
}

func AccountEffect(network string, sequence uint32, txIndex uint32, opIndex uint32, account string) string {
	return stable("account-effect", network, fmt.Sprintf("%d", sequence), fmt.Sprintf("%d", txIndex), fmt.Sprintf("%d", opIndex), account)
}

func stable(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, ":")))
	return strings.Join(parts[:1], "") + "_" + hex.EncodeToString(h[:])[:24]
}
