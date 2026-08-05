package ingestbatch

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"google.golang.org/protobuf/proto"
)

type Descriptor struct {
	ID            string
	LedgerStart   uint32
	LedgerEnd     uint32
	LedgerCount   uint32
	EncodedBytes  uint64
	BronzeRows    uint64
	PayloadSHA256 string
}

// MeasureLedger returns the protobuf payload bytes and Bronze row count used
// by both client assembly and server resource enforcement. Length-delimiter
// bytes are excluded because each LedgerBatch remains an individual gRPC
// message.
func MeasureLedger(batch *componentsv1.LedgerBatch) (encodedBytes, bronzeRows uint64, err error) {
	if batch == nil {
		return 0, 0, fmt.Errorf("micro-batch ledger is nil")
	}
	return uint64(proto.Size(batch)), uint64(len(batch.BronzeRows)), nil
}

func Describe(batches []*componentsv1.LedgerBatch) (Descriptor, error) {
	if len(batches) == 0 {
		return Descriptor{}, fmt.Errorf("micro-batch is empty")
	}

	hasher := sha256.New()
	network := ""
	var descriptor Descriptor
	for index, batch := range batches {
		if batch == nil {
			return Descriptor{}, fmt.Errorf("micro-batch ledger %d is nil", index)
		}
		if batch.NetworkPassphrase == "" {
			return Descriptor{}, fmt.Errorf("micro-batch ledger %d has empty network passphrase", batch.LedgerSequence)
		}
		if index == 0 {
			descriptor.LedgerStart = batch.LedgerSequence
			network = batch.NetworkPassphrase
		} else {
			expected := batches[index-1].LedgerSequence + 1
			if batch.LedgerSequence != expected {
				return Descriptor{}, fmt.Errorf("micro-batch ledger %d follows %d, want %d", batch.LedgerSequence, batches[index-1].LedgerSequence, expected)
			}
			if batch.NetworkPassphrase != network {
				return Descriptor{}, fmt.Errorf("micro-batch ledger %d changes network passphrase", batch.LedgerSequence)
			}
		}

		encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(batch)
		if err != nil {
			return Descriptor{}, fmt.Errorf("marshal micro-batch ledger %d: %w", batch.LedgerSequence, err)
		}
		writeDelimited(hasher, encoded)
		encodedBytes, bronzeRows, err := MeasureLedger(batch)
		if err != nil {
			return Descriptor{}, err
		}
		descriptor.EncodedBytes += encodedBytes
		descriptor.BronzeRows += bronzeRows
	}

	descriptor.LedgerEnd = batches[len(batches)-1].LedgerSequence
	descriptor.LedgerCount = uint32(len(batches))
	descriptor.PayloadSHA256 = hex.EncodeToString(hasher.Sum(nil))
	descriptor.ID = descriptor.PayloadSHA256
	return descriptor, nil
}

func writeDelimited(writer hash.Hash, encoded []byte) {
	var length [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(length[:], uint64(len(encoded)))
	_, _ = writer.Write(length[:n])
	_, _ = writer.Write(encoded)
}
