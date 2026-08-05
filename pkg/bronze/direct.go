package bronze

import (
	"fmt"
	"reflect"
	"sync"

	extract "github.com/withObsrvr/stellar-extract"
)

// TransactionOverrides contains raw-envelope values that are intentionally
// not retained by stellar-extract's typed TransactionData rows. The key is a
// transaction hash within one ledger.
type TransactionOverrides map[string]map[string]any

type ledgerDataTable struct {
	name string
	rows any
}

// ProjectLedgerData maps stellar-extract's typed rows directly to the ordered
// values consumed by DuckDB Appenders. It avoids the live transport's
// LedgerBatch protobuf, per-row JSON encoding, and JSON decoding round trip.
func ProjectLedgerData(data *extract.LedgerData, transactionOverrides TransactionOverrides) []DecodedRow {
	return ProjectLedgerDataWithWorkers(data, transactionOverrides, 1)
}

// ProjectLedgerDataWithWorkers projects independent rows through one bounded
// worker pool while retaining exact table and source-row order.
func ProjectLedgerDataWithWorkers(data *extract.LedgerData, transactionOverrides TransactionOverrides, workers int) []DecodedRow {
	return projectLedgerDataWithWorkers(data, transactionOverrides, workers, nil)
}

// ProjectLedgerDataExceptWithWorkers leaves selected tables in their typed
// extraction slices so a generated columnar builder can consume them without
// first materializing reflected []any rows.
func ProjectLedgerDataExceptWithWorkers(data *extract.LedgerData, transactionOverrides TransactionOverrides, workers int, excludedTables ...string) []DecodedRow {
	excluded := make(map[string]struct{}, len(excludedTables))
	for _, tableName := range excludedTables {
		excluded[tableName] = struct{}{}
	}
	return projectLedgerDataWithWorkers(data, transactionOverrides, workers, excluded)
}

// LedgerDataRowCount counts typed extraction rows without projecting them.
func LedgerDataRowCount(data *extract.LedgerData) int {
	count := 0
	for _, table := range ledgerDataTables(data) {
		count += reflect.ValueOf(table.rows).Len()
	}
	return count
}

func projectLedgerDataWithWorkers(data *extract.LedgerData, transactionOverrides TransactionOverrides, workers int, excluded map[string]struct{}) []DecodedRow {
	if data == nil {
		return nil
	}
	tables := ledgerDataTables(data)
	if len(excluded) > 0 {
		included := tables[:0]
		for _, table := range tables {
			if _, skip := excluded[table.name]; !skip {
				included = append(included, table)
			}
		}
		tables = included
	}

	rowCount := 0
	for _, table := range tables {
		rowCount += reflect.ValueOf(table.rows).Len()
	}
	projected := make([]DecodedRow, 0, rowCount)
	projected = projected[:rowCount]
	if rowCount == 0 {
		return projected
	}
	if workers < 1 {
		workers = 1
	}
	if workers > rowCount {
		workers = rowCount
	}
	type projectionJob struct {
		table  ledgerDataTable
		start  int
		end    int
		offset int
	}
	jobs := make(chan projectionJob, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for job := range jobs {
				rows := reflect.ValueOf(job.table.rows)
				for index := job.start; index < job.end; index++ {
					row := rows.Index(index).Interface()
					var overrides map[string]any
					if transaction, ok := row.(extract.TransactionData); ok {
						overrides = transactionOverrides[transaction.TransactionHash]
					}
					projected[job.offset+index-job.start] = projectTypedStruct(job.table.name, row, overrides)
				}
			}
		}()
	}
	const rowsPerJob = 256
	offset := 0
	for _, table := range tables {
		rows := reflect.ValueOf(table.rows)
		for start := 0; start < rows.Len(); start += rowsPerJob {
			end := start + rowsPerJob
			if end > rows.Len() {
				end = rows.Len()
			}
			jobs <- projectionJob{table: table, start: start, end: end, offset: offset + start}
		}
		offset += rows.Len()
	}
	close(jobs)
	wait.Wait()
	return projected
}

func ledgerDataTables(data *extract.LedgerData) []ledgerDataTable {
	if data == nil {
		return nil
	}
	return []ledgerDataTable{
		{name: "ledgers_row_v2", rows: data.Ledgers},
		{name: "transactions_row_v2", rows: data.Transactions},
		{name: "operations_row_v2", rows: data.Operations},
		{name: "effects_row_v1", rows: data.Effects},
		{name: "trades_row_v1", rows: data.Trades},
		{name: "accounts_snapshot_v1", rows: data.Accounts},
		{name: "offers_snapshot_v1", rows: data.Offers},
		{name: "trustlines_snapshot_v1", rows: data.Trustlines},
		{name: "account_signers_snapshot_v1", rows: data.AccountSigners},
		{name: "claimable_balances_snapshot_v1", rows: data.ClaimableBalances},
		{name: "liquidity_pools_snapshot_v1", rows: data.LiquidityPools},
		{name: "config_settings_snapshot_v1", rows: data.ConfigSettings},
		{name: "ttl_snapshot_v1", rows: data.TTLEntries},
		{name: "native_balances_snapshot_v1", rows: data.NativeBalances},
		{name: "contract_events_stream_v1", rows: data.ContractEvents},
		{name: "contract_data_snapshot_v1", rows: data.ContractData},
		{name: "contract_code_snapshot_v1", rows: data.ContractCode},
		{name: "contract_creations_v1", rows: data.ContractCreations},
		{name: "token_transfers_stream_v1", rows: data.TokenTransfers},
		{name: "evicted_keys_state_v1", rows: data.EvictedKeys},
		{name: "restored_keys_state_v1", rows: data.RestoredKeys},
	}
}

func projectTypedStruct(tableName string, row any, overrides map[string]any) DecodedRow {
	spec, ok := TypedTableSpecs[tableName]
	if !ok {
		return DecodedRow{Err: fmt.Errorf("unsupported typed table %q", tableName)}
	}
	value := reflect.ValueOf(row)
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return DecodedRow{Spec: spec, Err: fmt.Errorf("typed table %s received a nil row", tableName)}
		}
		value = value.Elem()
	}
	if value.Type() != spec.RowType {
		return DecodedRow{Spec: spec, Err: fmt.Errorf("typed table %s received %s, want %s", tableName, value.Type(), spec.RowType)}
	}
	values, err := directTypedValues(spec, value, overrides)
	return DecodedRow{Spec: spec, Values: values, OK: err == nil, Err: err}
}

func directTypedValues(spec TypedTableSpec, value reflect.Value, overrides map[string]any) ([]any, error) {
	values := make([]any, 0, len(spec.Columns))
	for _, column := range spec.Columns {
		if defaultValue, ok := spec.ColumnDefaults[column]; ok {
			values = append(values, defaultValue)
			continue
		}
		if override, ok := overrides[column]; ok {
			values = append(values, override)
			continue
		}
		fieldName := columnFieldName(spec, column)
		field := value.FieldByName(fieldName)
		if field.IsValid() {
			sqlValue, err := sqlValue(field)
			if err != nil {
				return nil, fmt.Errorf("column %s.%s: %w", spec.TableName, column, err)
			}
			values = append(values, sqlValue)
			continue
		}
		if _, ok := spec.ColumnJSONFallbacks[column]; ok {
			// JSON fallback columns not represented by the typed extraction row
			// were absent in the old JSON bridge as well. Raw-XDR enrichments are
			// supplied explicitly through overrides above.
			values = append(values, nil)
			continue
		}
		return nil, fmt.Errorf("column %s.%s has no struct field %s, default, fallback, or override", spec.TableName, column, fieldName)
	}
	return values, nil
}
