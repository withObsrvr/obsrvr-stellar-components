package bronze

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
)

func TestDecodeTypedRowsBatchesPreservesOrderAndWorkerBound(t *testing.T) {
	const (
		batchCount   = 4
		rowsPerBatch = 300
		workerLimit  = 3
	)
	batches := make([]*componentsv1.LedgerBatch, 0, batchCount)
	for batchIndex := 0; batchIndex < batchCount; batchIndex++ {
		batch := &componentsv1.LedgerBatch{LedgerSequence: uint32(100 + batchIndex)}
		for rowIndex := 0; rowIndex < rowsPerBatch; rowIndex++ {
			batch.BronzeRows = append(batch.BronzeRows, &componentsv1.BronzeRow{
				Id: fmt.Sprintf("%d-%d", batchIndex, rowIndex),
			})
		}
		batches = append(batches, batch)
	}

	var active atomic.Int32
	var maximum atomic.Int32
	decode := func(row *componentsv1.BronzeRow, _ TypedRowEnrichments) (TypedTableSpec, []any, bool, error) {
		current := active.Add(1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		time.Sleep(50 * time.Microsecond)
		active.Add(-1)
		return TypedTableSpec{}, []any{row.Id}, true, nil
	}

	decoded := decodeTypedRowsBatches(batches, workerLimit, decode)
	if got, want := len(decoded), batchCount*rowsPerBatch; got != want {
		t.Fatalf("decoded rows = %d, want %d", got, want)
	}
	if got := maximum.Load(); got < 2 || got > workerLimit {
		t.Fatalf("maximum concurrent decoders = %d, want 2..%d", got, workerLimit)
	}
	for batchIndex := 0; batchIndex < batchCount; batchIndex++ {
		for rowIndex := 0; rowIndex < rowsPerBatch; rowIndex++ {
			index := batchIndex*rowsPerBatch + rowIndex
			want := fmt.Sprintf("%d-%d", batchIndex, rowIndex)
			if got := decoded[index].Values[0]; got != want {
				t.Fatalf("decoded row %d = %v, want %s", index, got, want)
			}
		}
	}
}
