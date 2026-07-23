package bronze

import (
	"database/sql"
	"fmt"
	"runtime"
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
	enrichments := BuildTypedRowEnrichments(batch)
	rows := batch.BronzeRows
	decoded := make([]DecodedRow, len(rows))
	workers := runtime.NumCPU()
	if workers > 8 {
		workers = 8
	}
	if workers < 1 {
		workers = 1
	}
	var wg sync.WaitGroup
	chunk := (len(rows) + workers - 1) / workers
	if chunk == 0 {
		return decoded
	}
	for start := 0; start < len(rows); start += chunk {
		end := start + chunk
		if end > len(rows) {
			end = len(rows)
		}
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			for i := start; i < end; i++ {
				spec, values, ok, err := TypedRowInsertValues(rows[i], enrichments)
				decoded[i] = DecodedRow{Spec: spec, Values: values, OK: ok, Err: err}
			}
		}(start, end)
	}
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
