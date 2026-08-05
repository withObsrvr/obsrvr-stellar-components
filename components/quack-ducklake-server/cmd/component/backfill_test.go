package main

import (
	"strings"
	"testing"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/ingestbatch"
)

func TestValidateMicroBatchBeginEnforcesRangeAndResourceLimits(t *testing.T) {
	server := &ingestServer{
		backfillMaxLedgers:      2,
		backfillMaxEncodedBytes: 1024,
		backfillMaxBronzeRows:   10,
	}
	valid := &componentsv1.IngestMicroBatchBegin{
		MicroBatchId:  "id",
		PayloadSha256: "sha",
		LedgerStart:   100,
		LedgerEnd:     101,
		LedgerCount:   2,
		EncodedBytes:  512,
		BronzeRows:    10,
	}
	if err := server.validateMicroBatchBegin(valid); err != nil {
		t.Fatalf("valid begin: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*componentsv1.IngestMicroBatchBegin)
		want   string
	}{
		{name: "missing digest", mutate: func(begin *componentsv1.IngestMicroBatchBegin) { begin.PayloadSha256 = "" }, want: "required"},
		{name: "range", mutate: func(begin *componentsv1.IngestMicroBatchBegin) { begin.LedgerEnd = 99 }, want: "precedes"},
		{name: "declared count", mutate: func(begin *componentsv1.IngestMicroBatchBegin) { begin.LedgerCount = 1 }, want: "does not cover"},
		{name: "ledger limit", mutate: func(begin *componentsv1.IngestMicroBatchBegin) { begin.LedgerEnd = 102; begin.LedgerCount = 3 }, want: "exceeds limit"},
		{name: "byte limit", mutate: func(begin *componentsv1.IngestMicroBatchBegin) { begin.EncodedBytes = 1025 }, want: "exceeds limit"},
		{name: "row limit", mutate: func(begin *componentsv1.IngestMicroBatchBegin) { begin.BronzeRows = 11 }, want: "exceeds limit"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			begin := *valid
			test.mutate(&begin)
			err := server.validateMicroBatchBegin(&begin)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestValidateMicroBatchDescriptorAndReceipt(t *testing.T) {
	batches := []*componentsv1.LedgerBatch{
		{NetworkPassphrase: "test network", LedgerSequence: 100},
		{NetworkPassphrase: "test network", LedgerSequence: 101},
	}
	descriptor, err := ingestbatch.Describe(batches)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	begin := &componentsv1.IngestMicroBatchBegin{
		MicroBatchId:  descriptor.ID,
		LedgerStart:   descriptor.LedgerStart,
		LedgerEnd:     descriptor.LedgerEnd,
		LedgerCount:   descriptor.LedgerCount,
		EncodedBytes:  descriptor.EncodedBytes,
		BronzeRows:    descriptor.BronzeRows,
		PayloadSha256: descriptor.PayloadSHA256,
	}
	if err := validateMicroBatchDescriptor(begin, descriptor); err != nil {
		t.Fatalf("validate descriptor: %v", err)
	}
	begin.PayloadSha256 = "different"
	if err := validateMicroBatchDescriptor(begin, descriptor); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("digest mismatch error = %v", err)
	}

	receipt := microBatchReceipt{
		network:       "test network",
		microBatchID:  descriptor.ID,
		ledgerStart:   descriptor.LedgerStart,
		ledgerEnd:     descriptor.LedgerEnd,
		ledgerCount:   descriptor.LedgerCount,
		payloadSHA256: descriptor.PayloadSHA256,
	}
	if err := receipt.matches("test network", descriptor); err != nil {
		t.Fatalf("matching receipt: %v", err)
	}
	receipt.ledgerEnd++
	if err := receipt.matches("test network", descriptor); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("conflicting receipt error = %v", err)
	}
}
