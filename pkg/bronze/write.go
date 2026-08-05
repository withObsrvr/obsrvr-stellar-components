package bronze

import (
	"database/sql"
	"fmt"
	"runtime"
	"sort"
	"sync"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
)

// DecodedRow is one bronze row resolved to its table spec and ordered column
// values, ready for a parameterized insert.
type DecodedRow struct {
	Spec   TypedTableSpec
	Values []any
	OK     bool
	Err    error
}

// DecodeTypedRows resolves every bronze row in the batch concurrently. Row
// decoding (JSON unmarshal + reflection) is CPU-bound and per-row
// independent; enrichments is read-only during the fan-out. Order is
// preserved.
func DecodeTypedRows(batch *componentsv1.LedgerBatch) []DecodedRow {
	workers := runtime.NumCPU()
	if workers > 8 {
		workers = 8
	}
	if workers < 1 {
		workers = 1
	}
	return DecodeTypedRowsBatches([]*componentsv1.LedgerBatch{batch}, workers)
}

// DecodeTypedRowsBatches resolves a contiguous range through one shared worker
// budget. The returned rows retain batch order and row order. This avoids
// multiplying the per-ledger worker count when a bounded backfill range is
// prepared as one transaction.
func DecodeTypedRowsBatches(batches []*componentsv1.LedgerBatch, workers int) []DecodedRow {
	return decodeTypedRowsBatches(batches, workers, TypedRowInsertValues)
}

type decodeRowFunc func(*componentsv1.BronzeRow, TypedRowEnrichments) (TypedTableSpec, []any, bool, error)

func decodeTypedRowsBatches(batches []*componentsv1.LedgerBatch, workers int, decodeRow decodeRowFunc) []DecodedRow {
	totalRows := 0
	for _, batch := range batches {
		if batch != nil {
			totalRows += len(batch.BronzeRows)
		}
	}
	decoded := make([]DecodedRow, totalRows)
	if totalRows == 0 {
		return decoded
	}
	if workers < 1 {
		workers = 1
	}
	if workers > totalRows {
		workers = totalRows
	}

	type decodeJob struct {
		rows        []*componentsv1.BronzeRow
		enrichments TypedRowEnrichments
		offset      int
	}
	jobs := make(chan decodeJob, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				for index, row := range job.rows {
					spec, values, ok, err := decodeRow(row, job.enrichments)
					decoded[job.offset+index] = DecodedRow{Spec: spec, Values: values, OK: ok, Err: err}
				}
			}
		}()
	}

	const rowsPerDecodeJob = 256
	offset := 0
	for _, batch := range batches {
		if batch == nil {
			continue
		}
		enrichments := BuildTypedRowEnrichments(batch)
		for start := 0; start < len(batch.BronzeRows); start += rowsPerDecodeJob {
			end := start + rowsPerDecodeJob
			if end > len(batch.BronzeRows) {
				end = len(batch.BronzeRows)
			}
			jobs <- decodeJob{rows: batch.BronzeRows[start:end], enrichments: enrichments, offset: offset + start}
		}
		offset += len(batch.BronzeRows)
	}
	close(jobs)
	wg.Wait()
	return decoded
}

// InsertChunkRows is the multi-row chunk size for parameterized inserts.
// Per-row Execs cost ~0.4ms each through database/sql, so chunking is what
// keeps a ~9k-row mainnet ledger under a second.
const InsertChunkRows = 128

// InsertDecodedRowsChunkedTx inserts every decoded row inside tx using one
// prepared multi-row statement per (table, chunk shape). Table-relative names
// (bronze.<table>) resolve against the transaction session's current catalog.
func InsertDecodedRowsChunkedTx(tx *sql.Tx, decoded []DecodedRow) error {
	return InsertDecodedRowsChunkedTxIn(tx, decoded, "")
}

// InsertDecodedRowsChunkedTxIn is InsertDecodedRowsChunkedTx targeting
// catalog.bronze.<table> when catalog is non-empty.
func InsertDecodedRowsChunkedTxIn(tx *sql.Tx, decoded []DecodedRow, catalog string) error {
	grouped := map[string][][]any{}
	groupSpecs := map[string]TypedTableSpec{}
	var groupOrder []string
	for i, dr := range decoded {
		if dr.Err != nil {
			return fmt.Errorf("typed bronze row %d: %w", i, dr.Err)
		}
		if !dr.OK {
			continue
		}
		if _, seen := grouped[dr.Spec.TableName]; !seen {
			groupOrder = append(groupOrder, dr.Spec.TableName)
			groupSpecs[dr.Spec.TableName] = dr.Spec
		}
		grouped[dr.Spec.TableName] = append(grouped[dr.Spec.TableName], dr.Values)
	}

	for _, tableName := range groupOrder {
		spec := groupSpecs[tableName]
		tableRows := grouped[tableName]
		var fullChunkStmt *sql.Stmt
		for start := 0; start < len(tableRows); start += InsertChunkRows {
			end := start + InsertChunkRows
			if end > len(tableRows) {
				end = len(tableRows)
			}
			chunkRows := tableRows[start:end]
			stmt := fullChunkStmt
			if len(chunkRows) == InsertChunkRows && fullChunkStmt == nil {
				var err error
				fullChunkStmt, err = tx.Prepare(MultiRowInsertSQLIn(catalog, spec, InsertChunkRows))
				if err != nil {
					return fmt.Errorf("prepare insert for %s: %w", tableName, err)
				}
				stmt = fullChunkStmt
			} else if len(chunkRows) != InsertChunkRows {
				var err error
				stmt, err = tx.Prepare(MultiRowInsertSQLIn(catalog, spec, len(chunkRows)))
				if err != nil {
					return fmt.Errorf("prepare insert for %s: %w", tableName, err)
				}
			}
			flat := make([]any, 0, len(chunkRows)*len(spec.Columns))
			for _, values := range chunkRows {
				flat = append(flat, values...)
			}
			_, err := stmt.Exec(flat...)
			if stmt != fullChunkStmt {
				stmt.Close()
			}
			if err != nil {
				if fullChunkStmt != nil {
					fullChunkStmt.Close()
				}
				return fmt.Errorf("insert rows %d-%d for %s: %w", start, end-1, tableName, err)
			}
		}
		if fullChunkStmt != nil {
			fullChunkStmt.Close()
		}
	}
	return nil
}

// DeleteLedgerRowsTx clears every row a previous write of this ledger could
// have produced: envelope tables (bronze_rows/ledger_batches still cleared
// for catalogs written by older sinks), the watermark, and all typed tables.
func DeleteLedgerRowsTx(tx *sql.Tx, networkPassphrase string, ledgerSequence uint32) error {
	for _, stmt := range []struct {
		sql  string
		desc string
	}{
		{"DELETE FROM bronze_rows WHERE network_passphrase = ? AND ledger_sequence = ?", "bronze rows"},
		{"DELETE FROM ledger_batches WHERE network_passphrase = ? AND ledger_sequence = ?", "ledger batch"},
		{"DELETE FROM ingest_watermarks WHERE network_passphrase = ? AND ledger_sequence = ?", "ingest watermark"},
	} {
		if _, err := tx.Exec(stmt.sql, networkPassphrase, ledgerSequence); err != nil {
			return fmt.Errorf("delete existing %s: %w", stmt.desc, err)
		}
	}
	return DeleteTypedRows(tx, ledgerSequence)
}

// DeleteLedgerRangeRowsTx clears a contiguous inclusive ledger range and any
// overlapping micro-batch receipts. It is the replay primitive for an
// uncertain bounded backfill commit.
func DeleteLedgerRangeRowsTx(tx *sql.Tx, networkPassphrase string, ledgerStart, ledgerEnd uint32) error {
	if ledgerStart > ledgerEnd {
		return fmt.Errorf("delete ledger range start %d exceeds end %d", ledgerStart, ledgerEnd)
	}
	for _, stmt := range []struct {
		sql  string
		desc string
	}{
		{"DELETE FROM bronze_rows WHERE network_passphrase = ? AND ledger_sequence BETWEEN ? AND ?", "bronze rows"},
		{"DELETE FROM ledger_batches WHERE network_passphrase = ? AND ledger_sequence BETWEEN ? AND ?", "ledger batches"},
		{"DELETE FROM ingest_watermarks WHERE network_passphrase = ? AND ledger_sequence BETWEEN ? AND ?", "ingest watermarks"},
	} {
		if _, err := tx.Exec(stmt.sql, networkPassphrase, ledgerStart, ledgerEnd); err != nil {
			return fmt.Errorf("delete existing %s: %w", stmt.desc, err)
		}
	}
	if _, err := tx.Exec(
		"DELETE FROM bronze.ingest_microbatch_commits WHERE network_passphrase = ? AND ledger_start <= ? AND ledger_end >= ?",
		networkPassphrase,
		ledgerEnd,
		ledgerStart,
	); err != nil {
		return fmt.Errorf("delete existing micro-batch receipts: %w", err)
	}

	tableNames := make([]string, 0, len(TypedTableSpecs))
	for tableName := range TypedTableSpecs {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)
	for _, tableName := range tableNames {
		spec := TypedTableSpecs[tableName]
		if spec.LedgerColumn == "" {
			continue
		}
		if _, err := tx.Exec(
			fmt.Sprintf("DELETE FROM bronze.%s WHERE %s BETWEEN ? AND ?", spec.TableName, QuoteIdentifier(spec.LedgerColumn)),
			ledgerStart,
			ledgerEnd,
		); err != nil {
			return fmt.Errorf("delete typed range from %s: %w", spec.TableName, err)
		}
	}
	return nil
}

func InsertMicroBatchReceiptTx(tx *sql.Tx, networkPassphrase, microBatchID string, ledgerStart, ledgerEnd, ledgerCount uint32, payloadSHA256 string) error {
	if _, err := tx.Exec(
		`INSERT INTO bronze.ingest_microbatch_commits (
			network_passphrase,
			micro_batch_id,
			ledger_start,
			ledger_end,
			ledger_count,
			payload_sha256,
			committed_at
		) VALUES (?, ?, ?, ?, ?, ?, current_timestamp)`,
		networkPassphrase,
		microBatchID,
		ledgerStart,
		ledgerEnd,
		ledgerCount,
		payloadSHA256,
	); err != nil {
		return fmt.Errorf("insert micro-batch receipt: %w", err)
	}
	return nil
}

// InsertLedgerBatchRowTx records the per-ledger metadata row. payload_json
// is always NULL: the raw ledger payload is not persisted, the upstream
// archive is the replay source.
func InsertLedgerBatchRowTx(tx *sql.Tx, batch *componentsv1.LedgerBatch) error {
	if _, err := tx.Exec(
		`INSERT INTO ledger_batches (
			network_passphrase,
			ledger_sequence,
			closed_at_unix,
			schema_version,
			extraction_version,
			transaction_count,
			operation_count,
			bronze_row_count,
			payload_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		batch.NetworkPassphrase,
		batch.LedgerSequence,
		batch.ClosedAtUnix,
		batch.SchemaVersion,
		batch.ExtractionVersion,
		len(batch.Transactions),
		len(batch.Operations),
		len(batch.BronzeRows),
	); err != nil {
		return fmt.Errorf("insert ledger batch: %w", err)
	}
	return nil
}

// InsertWatermarkTx records the committed-ledger watermark inside the same
// transaction as the batch's rows.
func InsertWatermarkTx(tx *sql.Tx, batch *componentsv1.LedgerBatch) error {
	if _, err := tx.Exec(
		`INSERT INTO ingest_watermarks (
			network_passphrase,
			ledger_sequence,
			written_at
		) VALUES (?, ?, current_timestamp)`,
		batch.NetworkPassphrase,
		batch.LedgerSequence,
	); err != nil {
		return fmt.Errorf("insert ingest watermark: %w", err)
	}
	return nil
}
