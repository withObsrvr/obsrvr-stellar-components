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

// Accumulator computes a Descriptor one ledger at a time so bounded workers do
// not need to retain a complete range in memory.
type Accumulator struct {
	descriptor Descriptor
	network    string
	hasher     hash.Hash
}

func NewAccumulator() *Accumulator {
	return &Accumulator{hasher: sha256.New()}
}

func (a *Accumulator) Add(batch *componentsv1.LedgerBatch) error {
	if batch == nil {
		return fmt.Errorf("micro-batch ledger %d is nil", a.descriptor.LedgerCount)
	}
	if batch.NetworkPassphrase == "" {
		return fmt.Errorf("micro-batch ledger %d has empty network passphrase", batch.LedgerSequence)
	}
	if a.descriptor.LedgerCount == 0 {
		a.descriptor.LedgerStart = batch.LedgerSequence
		a.network = batch.NetworkPassphrase
	} else {
		expected := a.descriptor.LedgerEnd + 1
		if batch.LedgerSequence != expected {
			return fmt.Errorf("micro-batch ledger %d follows %d, want %d", batch.LedgerSequence, a.descriptor.LedgerEnd, expected)
		}
		if batch.NetworkPassphrase != a.network {
			return fmt.Errorf("micro-batch ledger %d changes network passphrase", batch.LedgerSequence)
		}
	}

	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal micro-batch ledger %d: %w", batch.LedgerSequence, err)
	}
	writeDelimited(a.hasher, encoded)
	a.descriptor.LedgerEnd = batch.LedgerSequence
	a.descriptor.LedgerCount++
	a.descriptor.EncodedBytes += uint64(len(encoded))
	a.descriptor.BronzeRows += uint64(len(batch.BronzeRows))
	return nil
}

func (a *Accumulator) Descriptor() (Descriptor, error) {
	if a.descriptor.LedgerCount == 0 {
		return Descriptor{}, fmt.Errorf("micro-batch is empty")
	}
	descriptor := a.descriptor
	descriptor.PayloadSHA256 = hex.EncodeToString(a.hasher.Sum(nil))
	descriptor.ID = descriptor.PayloadSHA256
	return descriptor, nil
}

// Totals returns the accumulated range and resource counts without computing
// the payload digest. Hot paths can use this after every Add and finalize the
// digest once after the stream ends.
func (a *Accumulator) Totals() Descriptor {
	return a.descriptor
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
	accumulator := NewAccumulator()
	for _, batch := range batches {
		if err := accumulator.Add(batch); err != nil {
			return Descriptor{}, err
		}
	}
	return accumulator.Descriptor()
}

func writeDelimited(writer hash.Hash, encoded []byte) {
	var length [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(length[:], uint64(len(encoded)))
	_, _ = writer.Write(length[:n])
	_, _ = writer.Write(encoded)
}
