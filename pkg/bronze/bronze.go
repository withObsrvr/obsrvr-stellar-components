// Package bronze holds the typed bronze-table machinery shared by
// ducklake-sink (embedded and staged-parquet writes) and
// quack-ducklake-server (in-process bulk-ingest writes): table specs, row
// decoding, insert SQL builders, catalog network pinning, and the ordered
// schema migrations.
package bronze

import (
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	extract "github.com/withObsrvr/stellar-extract"
)

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

//go:embed bronze_schema.sql
var SchemaSQL string

const addContractExecutableColumnsSQL = `
ALTER TABLE bronze.contract_creations_v1 ADD COLUMN IF NOT EXISTS executable_type TEXT;
ALTER TABLE bronze.contract_creations_v1 ADD COLUMN IF NOT EXISTS external_ref_owner TEXT;
ALTER TABLE bronze.contract_creations_v1 ADD COLUMN IF NOT EXISTS external_ref_tag TEXT;
`

const addIngestMicrobatchCommitsSQL = `
CREATE TABLE IF NOT EXISTS bronze.ingest_microbatch_commits (
	network_passphrase VARCHAR NOT NULL,
	micro_batch_id VARCHAR NOT NULL,
	ledger_start UBIGINT NOT NULL,
	ledger_end UBIGINT NOT NULL,
	ledger_count UINTEGER NOT NULL,
	payload_sha256 VARCHAR NOT NULL,
	committed_at TIMESTAMP NOT NULL
);
`

type Migration struct {
	Version int
	Name    string
	SQL     string
}

var Migrations = []Migration{
	{Version: 1, Name: "bronze_schema", SQL: SchemaSQL},
	{Version: 2, Name: "contract_executable_columns", SQL: addContractExecutableColumnsSQL},
	{Version: 3, Name: "ingest_microbatch_commits", SQL: addIngestMicrobatchCommitsSQL},
}

func RecordMigrationTx(tx *sql.Tx, migration Migration) error {
	if _, err := tx.Exec(
		`INSERT INTO schema_migrations (version, name, applied_at)
SELECT ?, ?, current_timestamp
WHERE NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = ?)`,
		migration.Version,
		migration.Name,
		migration.Version,
	); err != nil {
		return fmt.Errorf("record DuckLake migration %03d %s: %w", migration.Version, migration.Name, err)
	}
	return nil
}

func EnsureMigrationRecordedTx(tx *sql.Tx, migration Migration) error {
	var count int
	if err := tx.QueryRow("SELECT count(*) FROM schema_migrations WHERE version = ?", migration.Version).Scan(&count); err != nil {
		return fmt.Errorf("verify DuckLake migration %03d %s: %w", migration.Version, migration.Name, err)
	}
	if count == 0 {
		return fmt.Errorf("DuckLake migration %03d %s was not recorded", migration.Version, migration.Name)
	}
	if count > 1 {
		return fmt.Errorf("DuckLake migration %03d %s has duplicate records", migration.Version, migration.Name)
	}
	return nil
}

const CreateCatalogMetadataSQL = `
CREATE TABLE IF NOT EXISTS catalog_metadata (
	key VARCHAR NOT NULL,
	value VARCHAR NOT NULL,
	updated_at TIMESTAMP NOT NULL
);
`

const CreateIngestWatermarksSQL = `
CREATE TABLE IF NOT EXISTS ingest_watermarks (
	network_passphrase VARCHAR,
	ledger_sequence UBIGINT,
	written_at TIMESTAMP
);
`

const CreateSchemaMigrationsSQL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
	version INTEGER NOT NULL,
	name VARCHAR NOT NULL,
	applied_at TIMESTAMP NOT NULL
);
`

const CreateLedgerBatchesSQL = `
CREATE TABLE IF NOT EXISTS ledger_batches (
	network_passphrase VARCHAR,
	ledger_sequence UBIGINT,
	closed_at_unix BIGINT,
	schema_version VARCHAR,
	extraction_version VARCHAR,
	transaction_count INTEGER,
	operation_count INTEGER,
	bronze_row_count INTEGER,
	payload_json VARCHAR
);
`

const CreateBronzeRowsSQL = `
CREATE TABLE IF NOT EXISTS bronze_rows (
	network_passphrase VARCHAR,
	ledger_sequence UBIGINT,
	ledger_range UBIGINT,
	row_ordinal INTEGER,
	bronze_row_id VARCHAR,
	table_name VARCHAR,
	row_json VARCHAR
);
`

type TypedTableSpec struct {
	TableName           string
	Columns             []string
	RowType             reflect.Type
	LedgerColumn        string
	ColumnOverrides     map[string]string
	ColumnJSONFallbacks map[string]string
	ColumnDefaults      map[string]any
}

func EnsureCatalogNetworkTx(tx *sql.Tx, networkPassphrase string) error {
	networkPassphrase = strings.TrimSpace(networkPassphrase)
	if networkPassphrase == "" {
		return fmt.Errorf("ledger batch network_passphrase is required")
	}
	existingCount, existingMin, existingMax, err := readCatalogNetworkMetadataTx(tx)
	if err != nil {
		return fmt.Errorf("read catalog network passphrase: %w", err)
	}
	if existingCount == 0 {
		if _, err := tx.Exec(
			"INSERT INTO catalog_metadata (key, value, updated_at) VALUES ('network_passphrase', ?, current_timestamp)",
			networkPassphrase,
		); err != nil {
			return fmt.Errorf("record catalog network passphrase: %w", err)
		}
		existingCount, existingMin, existingMax, err = readCatalogNetworkMetadataTx(tx)
		if err != nil {
			return fmt.Errorf("read catalog network passphrase after insert: %w", err)
		}
	}
	if existingCount > 1 {
		return fmt.Errorf("catalog network metadata has duplicate network_passphrase keys")
	}
	if !existingMin.Valid || !existingMax.Valid || existingMin.String != existingMax.String {
		return fmt.Errorf("catalog network metadata is invalid for network_passphrase")
	}
	if existingMin.String != networkPassphrase {
		return fmt.Errorf("catalog network mismatch: existing %q, batch %q", existingMin.String, networkPassphrase)
	}
	if _, err := tx.Exec(
		"UPDATE catalog_metadata SET updated_at = current_timestamp WHERE key = 'network_passphrase'",
	); err != nil {
		return fmt.Errorf("refresh catalog network metadata: %w", err)
	}
	return nil
}

func readCatalogNetworkMetadataTx(tx *sql.Tx) (int, sql.NullString, sql.NullString, error) {
	var count int
	var minValue, maxValue sql.NullString
	err := tx.QueryRow(
		"SELECT count(*), min(value), max(value) FROM catalog_metadata WHERE key = 'network_passphrase'",
	).Scan(&count, &minValue, &maxValue)
	return count, minValue, maxValue, err
}

func EnsureCatalogNetworkSQL(catalog, networkPassphrase string) string {
	return fmt.Sprintf(`INSERT INTO %s.catalog_metadata (key, value, updated_at)
SELECT 'network_passphrase', %s, current_timestamp
WHERE NOT EXISTS (
	SELECT 1 FROM %s.catalog_metadata WHERE key = 'network_passphrase'
);
SELECT CASE
	WHEN (SELECT count(*) FROM %s.catalog_metadata WHERE key = 'network_passphrase') > 1
		THEN error('catalog network metadata duplicate key')
	WHEN EXISTS (
		SELECT 1 FROM %s.catalog_metadata
		WHERE key = 'network_passphrase' AND value <> %s
	) THEN error('catalog network mismatch')
	ELSE 1
END;
UPDATE %s.catalog_metadata
SET updated_at = current_timestamp
WHERE key = 'network_passphrase'`,
		catalog,
		SQLLiteral(strings.TrimSpace(networkPassphrase)),
		catalog,
		catalog,
		catalog,
		SQLLiteral(strings.TrimSpace(networkPassphrase)),
		catalog,
	)
}

func DeleteTypedRows(tx *sql.Tx, ledgerSequence uint32) error {
	for _, spec := range TypedTableSpecs {
		if spec.LedgerColumn == "" {
			continue
		}
		if _, err := tx.Exec(
			fmt.Sprintf("DELETE FROM bronze.%s WHERE %s = ?", spec.TableName, QuoteIdentifier(spec.LedgerColumn)),
			ledgerSequence,
		); err != nil {
			return fmt.Errorf("delete typed rows from %s: %w", spec.TableName, err)
		}
	}
	return nil
}

type TypedRowEnrichment map[string]any
type TypedRowEnrichments map[string]TypedRowEnrichment

func BuildTypedRowEnrichments(batch *componentsv1.LedgerBatch) TypedRowEnrichments {
	enrichments := TypedRowEnrichments{}
	if batch == nil {
		return enrichments
	}
	for _, tx := range batch.Transactions {
		key := transactionEnrichmentKey(tx.LedgerSequence, tx.TransactionHash)
		enrichments[key] = TypedRowEnrichment{
			"tx_envelope": tx.EnvelopeXdr,
			"tx_result":   tx.ResultXdr,
			"tx_meta":     tx.MetaXdr,
		}
	}
	return enrichments
}

func transactionEnrichmentKey(ledgerSequence uint32, transactionHash string) string {
	return fmt.Sprintf("transactions_row_v2:%d:%s", ledgerSequence, transactionHash)
}

// TypedRowInsertValues resolves one bronze row into its table spec and the
// ordered column values for an INSERT. ok is false for rows without a typed
// table.
func TypedRowInsertValues(row *componentsv1.BronzeRow, enrichments TypedRowEnrichments) (TypedTableSpec, []any, bool, error) {
	spec, ok := TypedTableSpecs[row.TableName]
	if !ok {
		return TypedTableSpec{}, nil, false, nil
	}
	value := reflect.New(spec.RowType)
	if err := json.Unmarshal([]byte(row.RowJson), value.Interface()); err != nil {
		return TypedTableSpec{}, nil, false, fmt.Errorf("unmarshal typed row: %w", err)
	}
	values, err := typedValues(spec, value.Elem(), row, enrichments)
	if err != nil {
		return TypedTableSpec{}, nil, false, err
	}
	return spec, values, true, nil
}

func TypedInsertSQL(spec TypedTableSpec) string {
	placeholders := make([]string, len(spec.Columns))
	columns := make([]string, len(spec.Columns))
	for i, col := range spec.Columns {
		placeholders[i] = "?"
		columns[i] = QuoteIdentifier(col)
	}
	return fmt.Sprintf(
		"INSERT INTO bronze.%s (%s) VALUES (%s)",
		spec.TableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)
}

func MultiRowInsertSQL(spec TypedTableSpec, rowCount int) string {
	return MultiRowInsertSQLIn("", spec, rowCount)
}

// MultiRowInsertSQLIn renders the insert against catalog.bronze.<table> when
// catalog is non-empty, and the session-relative bronze.<table> otherwise.
func MultiRowInsertSQLIn(catalog string, spec TypedTableSpec, rowCount int) string {
	columns := make([]string, len(spec.Columns))
	placeholders := make([]string, len(spec.Columns))
	for i, col := range spec.Columns {
		columns[i] = QuoteIdentifier(col)
		placeholders[i] = "?"
	}
	tuple := "(" + strings.Join(placeholders, ", ") + ")"
	tuples := make([]string, rowCount)
	for i := range tuples {
		tuples[i] = tuple
	}
	target := "bronze." + spec.TableName
	if catalog != "" {
		target = catalog + "." + target
	}
	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		target,
		strings.Join(columns, ", "),
		strings.Join(tuples, ", "),
	)
}

func InsertTypedBronzeRow(tx *sql.Tx, row *componentsv1.BronzeRow, enrichments TypedRowEnrichments) error {
	spec, values, ok, err := TypedRowInsertValues(row, enrichments)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if _, err := tx.Exec(TypedInsertSQL(spec), values...); err != nil {
		return fmt.Errorf("insert %s: %w", spec.TableName, err)
	}
	return nil
}

func typedValues(spec TypedTableSpec, value reflect.Value, bronzeRow *componentsv1.BronzeRow, enrichments TypedRowEnrichments) ([]any, error) {
	values := make([]any, 0, len(spec.Columns))
	// RowJson is parsed a second time (beyond the struct unmarshal) only for
	// tables that actually need the raw map: the transactions enrichment
	// lookup and explicit JSON-fallback columns. Skipping it elsewhere
	// roughly halves per-row decode cost on the biggest tables.
	var jsonValues map[string]any
	loadJSON := func() (map[string]any, error) {
		if jsonValues == nil {
			var err error
			jsonValues, err = decodeTypedRowJSON(bronzeRow.RowJson)
			if err != nil {
				return nil, err
			}
		}
		return jsonValues, nil
	}
	var enrichment TypedRowEnrichment
	if spec.TableName == "transactions_row_v2" {
		jv, err := loadJSON()
		if err != nil {
			return nil, err
		}
		enrichment = typedRowEnrichmentFor(spec, bronzeRow, jv, enrichments)
	}
	for _, col := range spec.Columns {
		if defaultValue, ok := spec.ColumnDefaults[col]; ok {
			values = append(values, defaultValue)
			continue
		}
		if enrichedValue, ok := enrichment[col]; ok {
			values = append(values, enrichedValue)
			continue
		}
		fieldName := columnFieldName(spec, col)
		field := value.FieldByName(fieldName)
		if field.IsValid() {
			sqlValue, err := sqlValue(field)
			if err != nil {
				return nil, fmt.Errorf("column %s.%s: %w", spec.TableName, col, err)
			}
			values = append(values, sqlValue)
			continue
		}
		jsonKey, ok := spec.ColumnJSONFallbacks[col]
		if !ok {
			return nil, fmt.Errorf("column %s.%s has no struct field %s or explicit JSON fallback", spec.TableName, col, fieldName)
		}
		jv, err := loadJSON()
		if err != nil {
			return nil, err
		}
		value, ok := jv[jsonKey]
		if !ok {
			values = append(values, nil)
			continue
		}
		values = append(values, value)
	}
	return values, nil
}

func typedRowEnrichmentFor(spec TypedTableSpec, bronzeRow *componentsv1.BronzeRow, jsonValues map[string]any, enrichments TypedRowEnrichments) TypedRowEnrichment {
	if spec.TableName != "transactions_row_v2" {
		return nil
	}
	transactionHash, _ := jsonValues["TransactionHash"].(string)
	if transactionHash == "" {
		transactionHash, _ = jsonValues["transaction_hash"].(string)
	}
	return enrichments[transactionEnrichmentKey(bronzeRow.LedgerSequence, transactionHash)]
}

func decodeTypedRowJSON(rowJSON string) (map[string]any, error) {
	raw := map[string]json.RawMessage{}
	if err := json.Unmarshal([]byte(rowJSON), &raw); err != nil {
		return nil, fmt.Errorf("unmarshal typed row JSON map: %w", err)
	}
	values := make(map[string]any, len(raw))
	for key, data := range raw {
		if string(data) == "null" {
			values[key] = nil
			continue
		}
		var text string
		if err := json.Unmarshal(data, &text); err == nil {
			values[key] = text
			continue
		}
		var boolean bool
		if err := json.Unmarshal(data, &boolean); err == nil {
			values[key] = boolean
			continue
		}
		var number json.Number
		if err := json.Unmarshal(data, &number); err == nil {
			if i, intErr := number.Int64(); intErr == nil {
				values[key] = i
				continue
			}
			if f, floatErr := number.Float64(); floatErr == nil {
				values[key] = f
				continue
			}
		}
		values[key] = string(data)
	}
	return values, nil
}

func columnFieldName(spec TypedTableSpec, column string) string {
	if override, ok := spec.ColumnOverrides[column]; ok {
		return override
	}
	return snakeToExported(column)
}

func sqlValue(value reflect.Value) (any, error) {
	if !value.IsValid() {
		return nil, nil
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, nil
		}
		return sqlValue(value.Elem())
	}
	if value.Type() == reflect.TypeOf(time.Time{}) {
		return value.Interface(), nil
	}
	if value.Kind() == reflect.Slice || value.Kind() == reflect.Map || value.Kind() == reflect.Struct {
		data, err := json.Marshal(value.Interface())
		if err != nil {
			return nil, err
		}
		return string(data), nil
	}
	return value.Interface(), nil
}

func SQLLiteral(value any) string {
	if value == nil {
		return "NULL"
	}
	switch v := value.(type) {
	case string:
		return "'" + escapeSQLString(v) + "'"
	case []byte:
		return "'" + escapeSQLString(string(v)) + "'"
	case bool:
		if v {
			return "true"
		}
		return "false"
	case time.Time:
		return "TIMESTAMP '" + escapeSQLString(v.UTC().Format("2006-01-02 15:04:05.999999")) + "'"
	case fmt.Stringer:
		return "'" + escapeSQLString(v.String()) + "'"
	default:
		return fmt.Sprint(v)
	}
}

func QualifyMigrationSQL(sqlText, catalog, schema string) string {
	sqlText = strings.TrimSpace(sqlText)
	if schema != "" {
		for _, prefix := range []string{"CREATE TABLE IF NOT EXISTS ", "ALTER TABLE "} {
			unqualified := prefix + schema + "."
			if strings.HasPrefix(sqlText, unqualified) {
				return strings.Replace(sqlText, unqualified, prefix+catalog+"."+schema+".", 1)
			}
		}
		return sqlText
	}
	return strings.Replace(sqlText, "CREATE TABLE IF NOT EXISTS ", "CREATE TABLE IF NOT EXISTS "+catalog+".", 1)
}

func snakeToExported(value string) string {
	parts := strings.Split(strings.Trim(value, `"`), "_")
	var b strings.Builder
	for _, part := range parts {
		switch strings.ToLower(part) {
		case "":
			continue
		case "id":
			b.WriteString("ID")
		case "xdr":
			b.WriteString("XDR")
		case "ttl":
			b.WriteString("TTL")
		case "tx":
			b.WriteString("Tx")
		case "json":
			b.WriteString("JSON")
		case "wasm":
			b.WriteString("Wasm")
		default:
			b.WriteString(strings.ToUpper(part[:1]))
			if len(part) > 1 {
				b.WriteString(part[1:])
			}
		}
	}
	return b.String()
}

func QuoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(strings.Trim(value, `"`), `"`, `""`) + `"`
}

func SplitSQLStatements(sqlText string) []string {
	var statements []string
	for _, stmt := range strings.Split(sqlText, ";") {
		var cleaned []string
		for _, line := range strings.Split(stmt, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "--") {
				continue
			}
			cleaned = append(cleaned, line)
		}
		stmt = strings.TrimSpace(strings.Join(cleaned, "\n"))
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}
	return statements
}

func ValidateTypedTableSpecs() error {
	var missing []string
	for _, spec := range TypedTableSpecs {
		for _, col := range spec.Columns {
			if _, ok := spec.ColumnDefaults[col]; ok {
				continue
			}
			fieldName := columnFieldName(spec, col)
			if reflect.New(spec.RowType).Elem().FieldByName(fieldName).IsValid() {
				continue
			}
			if _, ok := spec.ColumnJSONFallbacks[col]; ok {
				continue
			}
			missing = append(missing, fmt.Sprintf("%s.%s -> %s", spec.TableName, col, fieldName))
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("typed DuckLake column mappings are incomplete: %s", strings.Join(missing, "; "))
	}
	return nil
}

func tableSpec(table string, row any, ledgerColumn string, columns []string, overrides map[string]string, jsonFallbacks ...map[string]string) TypedTableSpec {
	var fallback map[string]string
	if len(jsonFallbacks) > 0 {
		fallback = jsonFallbacks[0]
	}
	return TypedTableSpec{
		TableName:           table,
		Columns:             columns,
		RowType:             reflect.TypeOf(row),
		LedgerColumn:        ledgerColumn,
		ColumnOverrides:     overrides,
		ColumnJSONFallbacks: fallback,
		ColumnDefaults:      map[string]any{"version_label": contracts.ExtractionVersion},
	}
}

func jsonFallbacks(columns ...string) map[string]string {
	fallbacks := make(map[string]string, len(columns))
	for _, col := range columns {
		fallbacks[col] = snakeToExported(col)
	}
	return fallbacks
}

var TypedTableSpecs = map[string]TypedTableSpec{
	"ledgers_row_v2": tableSpec("ledgers_row_v2", extract.LedgerRowData{}, "sequence", []string{
		"sequence", "ledger_hash", "previous_ledger_hash", "closed_at", "protocol_version", "total_coins", "fee_pool", "base_fee", "base_reserve", "max_tx_set_size", "successful_tx_count", "failed_tx_count", "ingestion_timestamp", "ledger_range", "transaction_count", "operation_count", "tx_set_operation_count", "soroban_fee_write1kb", "node_id", "signature", "ledger_header", "bucket_list_size", "live_soroban_state_size", "evicted_keys_count", "soroban_op_count", "total_fee_charged", "contract_events_count", "era_id", "version_label",
	}, nil),
	"transactions_row_v2": tableSpec("transactions_row_v2", extract.TransactionData{}, "ledger_sequence", []string{
		"ledger_sequence", "transaction_hash", "source_account", "fee_charged", "max_fee", "successful", "transaction_result_code", "operation_count", "memo_type", "memo", "created_at", "account_sequence", "ledger_range", "source_account_muxed", "fee_account_muxed", "inner_transaction_hash", "fee_bump_fee", "max_fee_bid", "inner_source_account", "timebounds_min_time", "timebounds_max_time", "ledgerbounds_min", "ledgerbounds_max", "min_sequence_number", "min_sequence_age", "soroban_resources_instructions", "soroban_resources_read_bytes", "soroban_resources_write_bytes", "soroban_data_size_bytes", "soroban_data_resources", "soroban_fee_base", "soroban_fee_resources", "soroban_fee_refund", "soroban_fee_charged", "soroban_fee_wasted", "soroban_host_function_type", "soroban_contract_id", "soroban_contract_events_count", "signatures_count", "new_account", "rent_fee_charged", "tx_envelope", "tx_result", "tx_meta", "tx_fee_meta", "tx_signers", "extra_signers", "era_id", "version_label", "transaction_id",
	}, nil, jsonFallbacks(
		"fee_account_muxed",
		"inner_transaction_hash",
		"fee_bump_fee",
		"max_fee_bid",
		"inner_source_account",
		"ledgerbounds_min",
		"ledgerbounds_max",
		"min_sequence_number",
		"min_sequence_age",
		"soroban_data_size_bytes",
		"soroban_data_resources",
		"soroban_fee_base",
		"soroban_fee_resources",
		"soroban_fee_refund",
		"soroban_fee_charged",
		"soroban_fee_wasted",
		"soroban_contract_events_count",
		"tx_envelope",
		"tx_result",
		"tx_meta",
		"tx_fee_meta",
		"tx_signers",
		"extra_signers",
	)),
	"operations_row_v2": tableSpec("operations_row_v2", extract.OperationData{}, "ledger_sequence", []string{
		"transaction_hash", "operation_index", "ledger_sequence", "source_account", "type", "type_string", "created_at", "transaction_successful", "operation_result_code", "operation_trace_code", "ledger_range", "source_account_muxed", "asset", "asset_type", "asset_code", "asset_issuer", "source_asset", "source_asset_type", "source_asset_code", "source_asset_issuer", "amount", "source_amount", "destination_min", "starting_balance", "destination", "trustline_limit", "trustor", "authorize", "authorize_to_maintain_liabilities", "trust_line_flags", "balance_id", "claimants_count", "sponsored_id", "offer_id", "price", "price_r", "buying_asset", "buying_asset_type", "buying_asset_code", "buying_asset_issuer", "selling_asset", "selling_asset_type", "selling_asset_code", "selling_asset_issuer", "soroban_operation", "soroban_function", "soroban_contract_id", "soroban_auth_required", "bump_to", "set_flags", "clear_flags", "home_domain", "master_weight", "low_threshold", "medium_threshold", "high_threshold", "data_name", "data_value", "era_id", "version_label", "transaction_index", "soroban_arguments_json", "contract_calls_json", "contracts_involved", "max_call_depth", "transaction_id", "operation_id", "soroban_auth_credentials_types", "soroban_auth_addresses",
	}, map[string]string{"type": "OpType"}, jsonFallbacks(
		"operation_trace_code",
		"trustor",
		"authorize",
		"authorize_to_maintain_liabilities",
		"trust_line_flags",
		"claimants_count",
		"soroban_auth_credentials_types",
		"soroban_auth_addresses",
	)),
	"effects_row_v1": tableSpec("effects_row_v1", extract.EffectData{}, "ledger_sequence", []string{
		"ledger_sequence", "transaction_hash", "operation_index", "effect_index", "effect_type", "effect_type_string", "account_id", "amount", "asset_code", "asset_issuer", "asset_type", "trustline_limit", "authorize_flag", "clawback_flag", "signer_account", "signer_weight", "offer_id", "seller_account", "created_at", "ledger_range", "era_id", "version_label", "details_json", "operation_id",
	}, nil),
	"trades_row_v1": tableSpec("trades_row_v1", extract.TradeData{}, "ledger_sequence", []string{
		"ledger_sequence", "transaction_hash", "operation_index", "trade_index", "trade_type", "trade_timestamp", "seller_account", "selling_asset_code", "selling_asset_issuer", "selling_amount", "buyer_account", "buying_asset_code", "buying_asset_issuer", "buying_amount", "price", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"accounts_snapshot_v1": tableSpec("accounts_snapshot_v1", extract.AccountData{}, "ledger_sequence", []string{
		"account_id", "ledger_sequence", "closed_at", "balance", "sequence_number", "num_subentries", "num_sponsoring", "num_sponsored", "home_domain", "master_weight", "low_threshold", "med_threshold", "high_threshold", "flags", "auth_required", "auth_revocable", "auth_immutable", "auth_clawback_enabled", "signers", "sponsor_account", "created_at", "updated_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"trustlines_snapshot_v1": tableSpec("trustlines_snapshot_v1", extract.TrustlineData{}, "ledger_sequence", []string{
		"account_id", "asset_code", "asset_issuer", "asset_type", "balance", "trust_limit", "buying_liabilities", "selling_liabilities", "authorized", "authorized_to_maintain_liabilities", "clawback_enabled", "ledger_sequence", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"account_signers_snapshot_v1": tableSpec("account_signers_snapshot_v1", extract.AccountSignerData{}, "ledger_sequence", []string{
		"account_id", "signer", "ledger_sequence", "weight", "sponsor", "deleted", "closed_at", "ledger_range", "created_at", "era_id", "version_label",
	}, nil),
	"native_balances_snapshot_v1": tableSpec("native_balances_snapshot_v1", extract.NativeBalanceData{}, "ledger_sequence", []string{
		"account_id", "balance", "buying_liabilities", "selling_liabilities", "num_subentries", "num_sponsoring", "num_sponsored", "sequence_number", "last_modified_ledger", "ledger_sequence", "ledger_range", "era_id", "version_label",
	}, nil),
	"offers_snapshot_v1": tableSpec("offers_snapshot_v1", extract.OfferData{}, "ledger_sequence", []string{
		"offer_id", "seller_account", "ledger_sequence", "closed_at", "selling_asset_type", "selling_asset_code", "selling_asset_issuer", "buying_asset_type", "buying_asset_code", "buying_asset_issuer", "amount", "price", "flags", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"liquidity_pools_snapshot_v1": tableSpec("liquidity_pools_snapshot_v1", extract.LiquidityPoolData{}, "ledger_sequence", []string{
		"liquidity_pool_id", "ledger_sequence", "closed_at", "pool_type", "fee", "trustline_count", "total_pool_shares", "asset_a_type", "asset_a_code", "asset_a_issuer", "asset_a_amount", "asset_b_type", "asset_b_code", "asset_b_issuer", "asset_b_amount", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"claimable_balances_snapshot_v1": tableSpec("claimable_balances_snapshot_v1", extract.ClaimableBalanceData{}, "ledger_sequence", []string{
		"balance_id", "sponsor", "ledger_sequence", "closed_at", "asset_type", "asset_code", "asset_issuer", "amount", "claimants_count", "flags", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"contract_events_stream_v1": tableSpec("contract_events_stream_v1", extract.ContractEventData{}, "ledger_sequence", []string{
		"event_id", "contract_id", "ledger_sequence", "transaction_hash", "closed_at", "event_type", "in_successful_contract_call", "successful", "contract_event_xdr", "topics_json", "topics_decoded", "data_xdr", "data_decoded", "topic_count", "operation_index", "event_index", "topic0_decoded", "topic1_decoded", "topic2_decoded", "topic3_decoded", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"contract_data_snapshot_v1": tableSpec("contract_data_snapshot_v1", extract.ContractDataData{}, "ledger_sequence", []string{
		"contract_id", "ledger_sequence", "ledger_key_hash", "contract_key_type", "contract_durability", "asset_code", "asset_issuer", "asset_type", "balance_holder", "balance", "last_modified_ledger", "ledger_entry_change", "deleted", "closed_at", "contract_data_xdr", "created_at", "ledger_range", "token_name", "token_symbol", "token_decimals", "era_id", "version_label",
	}, map[string]string{"contract_id": "ContractId"}),
	"contract_code_snapshot_v1": tableSpec("contract_code_snapshot_v1", extract.ContractCodeData{}, "ledger_sequence", []string{
		"contract_code_hash", "ledger_key_hash", "contract_code_ext_v", "last_modified_ledger", "ledger_entry_change", "deleted", "closed_at", "ledger_sequence", "n_instructions", "n_functions", "n_globals", "n_table_entries", "n_types", "n_data_segments", "n_elem_segments", "n_imports", "n_exports", "n_data_segment_bytes", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"config_settings_snapshot_v1": tableSpec("config_settings_snapshot_v1", extract.ConfigSettingData{}, "ledger_sequence", []string{
		"config_setting_id", "ledger_sequence", "last_modified_ledger", "deleted", "closed_at", "ledger_max_instructions", "tx_max_instructions", "fee_rate_per_instructions_increment", "tx_memory_limit", "ledger_max_read_ledger_entries", "ledger_max_read_bytes", "ledger_max_write_ledger_entries", "ledger_max_write_bytes", "tx_max_read_ledger_entries", "tx_max_read_bytes", "tx_max_write_ledger_entries", "tx_max_write_bytes", "contract_max_size_bytes", "config_setting_xdr", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"ttl_snapshot_v1": tableSpec("ttl_snapshot_v1", extract.TTLData{}, "ledger_sequence", []string{
		"key_hash", "ledger_sequence", "live_until_ledger_seq", "ttl_remaining", "expired", "last_modified_ledger", "deleted", "closed_at", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"evicted_keys_state_v1": tableSpec("evicted_keys_state_v1", extract.EvictedKeyData{}, "ledger_sequence", []string{
		"key_hash", "ledger_sequence", "contract_id", "key_type", "durability", "closed_at", "ledger_range", "created_at", "era_id", "version_label",
	}, nil),
	"restored_keys_state_v1": tableSpec("restored_keys_state_v1", extract.RestoredKeyData{}, "ledger_sequence", []string{
		"key_hash", "ledger_sequence", "contract_id", "key_type", "durability", "restored_from_ledger", "closed_at", "ledger_range", "created_at", "era_id", "version_label",
	}, nil),
	"contract_creations_v1": tableSpec("contract_creations_v1", extract.ContractCreationData{}, "created_ledger", []string{
		"contract_id", "creator_address", "wasm_hash", "executable_type", "external_ref_owner", "external_ref_tag", "created_ledger", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"token_transfers_stream_v1": tableSpec("token_transfers_stream_v1", extract.TokenTransferData{}, "ledger_sequence", []string{
		"ledger_sequence", "transaction_hash", "transaction_id", "operation_id", "operation_index", "event_type", "from", "to", "asset", "asset_type", "asset_code", "asset_issuer", "amount", "amount_raw", "contract_id", "closed_at", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
}
