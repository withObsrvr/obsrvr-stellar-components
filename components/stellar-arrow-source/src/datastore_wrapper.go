package main

import (
	"context"
)

// DatastoreReaderWrapper wraps StellarDatastoreReader to implement DataLakeReader interface
type DatastoreReaderWrapper struct {
	*StellarDatastoreReader
}

// Ensure DatastoreReaderWrapper implements DataLakeReader interface
var _ DataLakeReader = (*DatastoreReaderWrapper)(nil)

// ListLedgers implements DataLakeReader interface
func (w *DatastoreReaderWrapper) ListLedgers(ctx context.Context, startSeq, endSeq uint32) ([]uint32, error) {
	return w.StellarDatastoreReader.ListLedgers(ctx, startSeq, endSeq)
}

// GetLedger implements DataLakeReader interface
func (w *DatastoreReaderWrapper) GetLedger(ctx context.Context, sequence uint32) ([]byte, error) {
	return w.StellarDatastoreReader.GetLedger(ctx, sequence)
}

// GetSourceURL implements DataLakeReader interface
func (w *DatastoreReaderWrapper) GetSourceURL(sequence uint32) string {
	return w.StellarDatastoreReader.GetSourceURL(sequence)
}

// Close implements DataLakeReader interface
func (w *DatastoreReaderWrapper) Close() {
	w.StellarDatastoreReader.Close()
}