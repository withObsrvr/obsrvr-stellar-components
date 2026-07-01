package main

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	"github.com/withObsrvr/flowctl-sdk/pkg/consumer"
	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	extract "github.com/withObsrvr/stellar-extract"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

//go:embed bronze_schema.sql
var bronzeSchemaSQL string

func main() {
	sink, err := NewDuckLakeSink(DuckLakeConfigFromEnv())
	if err != nil {
		panic(err)
	}
	defer sink.Close()

	consumer.Run(consumer.ConsumerConfig{
		ConsumerName: "Stellar Ledger DuckLake Sink",
		ComponentID:  getenv("COMPONENT_ID", "ducklake-sink"),
		InputTypes:   []string{contracts.LedgerBatchEventType},
		OnEvent: func(ctx context.Context, event *flowctlv1.Event) error {
			if event.Type != contracts.LedgerBatchEventType {
				return nil
			}
			var batch componentsv1.LedgerBatch
			if err := proto.Unmarshal(event.Payload, &batch); err != nil {
				return fmt.Errorf("unmarshal ledger batch: %w", err)
			}
			return sink.WriteBatch(&batch)
		},
	})
}

type DuckLakeConfig struct {
	Mode            string
	CatalogPath     string
	DataPath        string
	AttachName      string
	QuackURI        string
	QuackToken      string
	QuackRemoteDB   string
	QuackDisableSSL bool
}

func DuckLakeConfigFromEnv() DuckLakeConfig {
	return DuckLakeConfig{
		Mode:            strings.ToLower(getenv("DUCKLAKE_MODE", "embedded")),
		CatalogPath:     getenv("DUCKLAKE_CATALOG_PATH", "ducklake/stellar.ducklake"),
		DataPath:        getenv("DUCKLAKE_DATA_PATH", "ducklake/data"),
		AttachName:      getenv("DUCKLAKE_ATTACH_NAME", "stellar_lake"),
		QuackURI:        getenv("QUACK_URI", "quack:127.0.0.1:9494"),
		QuackToken:      getenv("QUACK_TOKEN", ""),
		QuackRemoteDB:   getenv("QUACK_REMOTE_DB", "remote_lake"),
		QuackDisableSSL: getenvBool("QUACK_DISABLE_SSL", true),
	}
}

type DuckLakeSink struct {
	db            *sql.DB
	attachName    string
	remoteDB      string
	remoteCatalog string
	remoteMode    bool
	mu            sync.Mutex
}

func NewDuckLakeSink(cfg DuckLakeConfig) (*DuckLakeSink, error) {
	if cfg.Mode == "" {
		cfg.Mode = "embedded"
	}
	if cfg.AttachName == "" {
		return nil, fmt.Errorf("DUCKLAKE_ATTACH_NAME is required")
	}
	if cfg.Mode == "quack" {
		return newQuackDuckLakeSink(cfg)
	}
	if cfg.Mode != "embedded" {
		return nil, fmt.Errorf("unsupported DUCKLAKE_MODE %q", cfg.Mode)
	}
	if cfg.CatalogPath == "" {
		return nil, fmt.Errorf("DUCKLAKE_CATALOG_PATH is required")
	}
	if cfg.DataPath == "" {
		return nil, fmt.Errorf("DUCKLAKE_DATA_PATH is required")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.CatalogPath), 0o755); err != nil && filepath.Dir(cfg.CatalogPath) != "." {
		return nil, fmt.Errorf("create DuckLake catalog directory: %w", err)
	}
	if err := os.MkdirAll(cfg.DataPath, 0o755); err != nil {
		return nil, fmt.Errorf("create DuckLake data directory: %w", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open embedded DuckDB: %w", err)
	}
	// Pin the pool to one connection so the loaded extensions/attachment and
	// every transaction (including a failure ROLLBACK) share a single DuckDB
	// session instead of being routed across pooled connections.
	db.SetMaxOpenConns(1)
	sink := &DuckLakeSink{db: db, attachName: sanitizeIdentifier(cfg.AttachName)}
	if err := sink.init(cfg); err != nil {
		db.Close()
		return nil, err
	}
	return sink, nil
}

func newQuackDuckLakeSink(cfg DuckLakeConfig) (*DuckLakeSink, error) {
	if cfg.QuackURI == "" {
		return nil, fmt.Errorf("QUACK_URI is required when DUCKLAKE_MODE=quack")
	}
	if cfg.QuackToken == "" {
		return nil, fmt.Errorf("QUACK_TOKEN is required when DUCKLAKE_MODE=quack")
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open DuckDB Quack client: %w", err)
	}
	// Pin the pool to one connection so the remote write script and its failure
	// ROLLBACK land on the same DuckDB/Quack session. This process is long-lived,
	// so a pooled connection left holding an aborted transaction could otherwise
	// be reused for a later batch.
	db.SetMaxOpenConns(1)
	sink := &DuckLakeSink{
		db:            db,
		attachName:    sanitizeIdentifier(cfg.AttachName),
		remoteDB:      sanitizeIdentifier(cfg.QuackRemoteDB),
		remoteCatalog: sanitizeIdentifier(cfg.AttachName),
		remoteMode:    true,
	}
	if err := sink.initQuack(cfg); err != nil {
		db.Close()
		return nil, err
	}
	return sink, nil
}

func (s *DuckLakeSink) init(cfg DuckLakeConfig) error {
	duckDBHome := filepath.Join(cfg.DataPath, ".duckdb")
	if err := os.MkdirAll(duckDBHome, 0o755); err != nil {
		return fmt.Errorf("create DuckDB home directory: %w", err)
	}
	stmts := []string{
		fmt.Sprintf("SET home_directory='%s'", escapeSQLString(duckDBHome)),
		"INSTALL ducklake",
		"LOAD ducklake",
		fmt.Sprintf(
			"ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')",
			escapeSQLString(cfg.CatalogPath),
			s.attachName,
			escapeSQLString(cfg.DataPath),
		),
		"USE " + s.attachName,
		createLedgerBatchesSQL,
		createBronzeRowsSQL,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("ducklake init %q: %w", stmt, err)
		}
	}
	if err := s.initBronzeSchema(); err != nil {
		return err
	}
	return nil
}

func (s *DuckLakeSink) initQuack(cfg DuckLakeConfig) error {
	stmts := []string{
		"INSTALL quack",
		"LOAD quack",
		fmt.Sprintf(
			"ATTACH '%s' AS %s (TOKEN '%s', DISABLE_SSL %t)",
			escapeSQLString(cfg.QuackURI),
			s.remoteDB,
			escapeSQLString(cfg.QuackToken),
			cfg.QuackDisableSSL,
		),
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("quack init %q: %w", stmt, err)
		}
	}
	if err := s.execRemoteScript(s.remoteInitSQL()); err != nil {
		return fmt.Errorf("quack remote schema init: %w", err)
	}
	return nil
}

func (s *DuckLakeSink) remoteInitSQL() string {
	stmts := []string{
		fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.bronze", s.remoteCatalog),
		qualifyCreateTableSQL(createLedgerBatchesSQL, s.remoteCatalog, ""),
		qualifyCreateTableSQL(createBronzeRowsSQL, s.remoteCatalog, ""),
	}
	for _, stmt := range splitSQLStatements(bronzeSchemaSQL) {
		stmts = append(stmts, qualifyCreateTableSQL(stmt, s.remoteCatalog, "bronze"))
	}
	return strings.Join(stmts, ";\n") + ";"
}

func (s *DuckLakeSink) initBronzeSchema() error {
	stmts := []string{"CREATE SCHEMA IF NOT EXISTS bronze"}
	stmts = append(stmts, splitSQLStatements(bronzeSchemaSQL)...)
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("ducklake bronze schema %q: %w", stmt, err)
		}
	}
	return nil
}

func (s *DuckLakeSink) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *DuckLakeSink) WriteBatch(batch *componentsv1.LedgerBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.remoteMode {
		return s.writeBatchRemote(batch)
	}

	payloadJSON, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(batch)
	if err != nil {
		return err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin DuckLake transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		"DELETE FROM bronze_rows WHERE network_passphrase = ? AND ledger_sequence = ?",
		batch.NetworkPassphrase,
		batch.LedgerSequence,
	); err != nil {
		return fmt.Errorf("delete existing bronze rows: %w", err)
	}
	if _, err := tx.Exec(
		"DELETE FROM ledger_batches WHERE network_passphrase = ? AND ledger_sequence = ?",
		batch.NetworkPassphrase,
		batch.LedgerSequence,
	); err != nil {
		return fmt.Errorf("delete existing ledger batch: %w", err)
	}
	if err := deleteTypedRows(tx, batch.LedgerSequence); err != nil {
		return err
	}
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
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		batch.NetworkPassphrase,
		batch.LedgerSequence,
		batch.ClosedAtUnix,
		batch.SchemaVersion,
		batch.ExtractionVersion,
		len(batch.Transactions),
		len(batch.Operations),
		len(batch.BronzeRows),
		string(payloadJSON),
	); err != nil {
		return fmt.Errorf("insert ledger batch: %w", err)
	}

	stmt, err := tx.Prepare(`INSERT INTO bronze_rows (
		network_passphrase,
		ledger_sequence,
		ledger_range,
		row_ordinal,
		bronze_row_id,
		table_name,
		row_json
	) VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare bronze insert: %w", err)
	}
	defer stmt.Close()

	for i, row := range batch.BronzeRows {
		if _, err := stmt.Exec(
			row.NetworkPassphrase,
			row.LedgerSequence,
			row.LedgerRange,
			i,
			row.Id,
			row.TableName,
			row.RowJson,
		); err != nil {
			return fmt.Errorf("insert bronze row %d: %w", i, err)
		}
		if err := insertTypedBronzeRow(tx, row); err != nil {
			return fmt.Errorf("insert typed bronze row %d table %s: %w", i, row.TableName, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit DuckLake transaction: %w", err)
	}
	return nil
}

func (s *DuckLakeSink) writeBatchRemote(batch *componentsv1.LedgerBatch) error {
	payloadJSON, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(batch)
	if err != nil {
		return err
	}

	var stmts []string
	stmts = append(stmts, "BEGIN TRANSACTION")
	stmts = append(stmts,
		fmt.Sprintf(
			"DELETE FROM %s.bronze_rows WHERE network_passphrase = %s AND ledger_sequence = %d",
			s.remoteCatalog,
			sqlLiteral(batch.NetworkPassphrase),
			batch.LedgerSequence,
		),
		fmt.Sprintf(
			"DELETE FROM %s.ledger_batches WHERE network_passphrase = %s AND ledger_sequence = %d",
			s.remoteCatalog,
			sqlLiteral(batch.NetworkPassphrase),
			batch.LedgerSequence,
		),
	)
	for _, spec := range typedTableSpecs {
		if spec.LedgerColumn == "" {
			continue
		}
		stmts = append(stmts, fmt.Sprintf(
			"DELETE FROM %s.bronze.%s WHERE %s = %d",
			s.remoteCatalog,
			spec.TableName,
			quoteIdentifier(spec.LedgerColumn),
			batch.LedgerSequence,
		))
	}
	stmts = append(stmts, fmt.Sprintf(
		`INSERT INTO %s.ledger_batches (
			network_passphrase,
			ledger_sequence,
			closed_at_unix,
			schema_version,
			extraction_version,
			transaction_count,
			operation_count,
			bronze_row_count,
			payload_json
		) VALUES (%s, %d, %d, %s, %s, %d, %d, %d, %s)`,
		s.remoteCatalog,
		sqlLiteral(batch.NetworkPassphrase),
		batch.LedgerSequence,
		batch.ClosedAtUnix,
		sqlLiteral(batch.SchemaVersion),
		sqlLiteral(batch.ExtractionVersion),
		len(batch.Transactions),
		len(batch.Operations),
		len(batch.BronzeRows),
		sqlLiteral(string(payloadJSON)),
	))

	if len(batch.BronzeRows) > 0 {
		values := make([]string, 0, len(batch.BronzeRows))
		for i, row := range batch.BronzeRows {
			values = append(values, fmt.Sprintf("(%s, %d, %d, %d, %s, %s, %s)",
				sqlLiteral(row.NetworkPassphrase),
				row.LedgerSequence,
				row.LedgerRange,
				i,
				sqlLiteral(row.Id),
				sqlLiteral(row.TableName),
				sqlLiteral(row.RowJson),
			))
		}
		stmts = append(stmts, fmt.Sprintf(
			`INSERT INTO %s.bronze_rows (
				network_passphrase,
				ledger_sequence,
				ledger_range,
				row_ordinal,
				bronze_row_id,
				table_name,
				row_json
			) VALUES %s`,
			s.remoteCatalog,
			strings.Join(values, ", "),
		))
	}

	typedRows := map[string][]string{}
	for _, row := range batch.BronzeRows {
		valuesSQL, err := typedBronzeValuesSQL(row)
		if err != nil {
			return fmt.Errorf("prepare typed bronze row table %s: %w", row.TableName, err)
		}
		if valuesSQL != "" {
			typedRows[row.TableName] = append(typedRows[row.TableName], valuesSQL)
		}
	}
	for tableName, rows := range typedRows {
		spec := typedTableSpecs[tableName]
		columns := make([]string, len(spec.Columns))
		for i, col := range spec.Columns {
			columns[i] = quoteIdentifier(col)
		}
		stmts = append(stmts, fmt.Sprintf(
			"INSERT INTO %s.bronze.%s (%s) VALUES %s",
			s.remoteCatalog,
			tableName,
			strings.Join(columns, ", "),
			strings.Join(rows, ", "),
		))
	}
	stmts = append(stmts, "COMMIT")

	if err := s.execRemoteScript(strings.Join(stmts, ";\n") + ";"); err != nil {
		// Best-effort ROLLBACK on the same pinned connection to clear any
		// lingering transaction before this connection is reused for the next
		// batch. Bound it so a degraded Quack link cannot stall the sink, and
		// log a failed rollback rather than swallowing an unclean state.
		rbCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if rbErr := s.execRemoteScriptContext(rbCtx, "ROLLBACK;"); rbErr != nil {
			log.Printf("remote DuckLake write batch ledger %d: rollback did not confirm clean state: %v", batch.LedgerSequence, rbErr)
		}
		return fmt.Errorf("remote DuckLake write batch ledger %d: %w", batch.LedgerSequence, err)
	}
	return nil
}

func (s *DuckLakeSink) execRemoteScript(sqlText string) error {
	return s.execRemoteScriptContext(context.Background(), sqlText)
}

func (s *DuckLakeSink) execRemoteScriptContext(ctx context.Context, sqlText string) error {
	query := fmt.Sprintf("SELECT * FROM %s.query(?)", s.remoteDB)
	if _, err := s.db.ExecContext(ctx, query, sqlText); err != nil {
		return err
	}
	return nil
}

func sanitizeIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "stellar_lake"
	}
	var b strings.Builder
	for i, r := range value {
		valid := r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || i > 0 && r >= '0' && r <= '9'
		if valid {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	return b.String()
}

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	// These flags gate security-relevant transport behavior (e.g. DISABLE_SSL),
	// so an unrecognized value is a misconfiguration we must not silently coerce
	// to false. Accept the common boolean spellings and fail fast on anything else.
	switch strings.ToLower(raw) {
	case "1", "t", "true", "y", "yes", "on":
		return true
	case "0", "f", "false", "n", "no", "off":
		return false
	default:
		log.Fatalf("%s must be a boolean (true/false/1/0/yes/no), got %q", key, raw)
		return fallback // unreachable; log.Fatalf exits
	}
}

const createLedgerBatchesSQL = `
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

const createBronzeRowsSQL = `
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

type typedTableSpec struct {
	TableName       string
	Columns         []string
	RowType         reflect.Type
	LedgerColumn    string
	ColumnOverrides map[string]string
	ColumnDefaults  map[string]any
}

func deleteTypedRows(tx *sql.Tx, ledgerSequence uint32) error {
	for _, spec := range typedTableSpecs {
		if spec.LedgerColumn == "" {
			continue
		}
		if _, err := tx.Exec(
			fmt.Sprintf("DELETE FROM bronze.%s WHERE %s = ?", spec.TableName, quoteIdentifier(spec.LedgerColumn)),
			ledgerSequence,
		); err != nil {
			return fmt.Errorf("delete typed rows from %s: %w", spec.TableName, err)
		}
	}
	return nil
}

func insertTypedBronzeRow(tx *sql.Tx, row *componentsv1.BronzeRow) error {
	spec, ok := typedTableSpecs[row.TableName]
	if !ok {
		return nil
	}
	value := reflect.New(spec.RowType)
	if err := json.Unmarshal([]byte(row.RowJson), value.Interface()); err != nil {
		return fmt.Errorf("unmarshal typed row: %w", err)
	}

	values, err := typedValues(spec, value.Elem(), row)
	if err != nil {
		return err
	}
	placeholders := make([]string, len(spec.Columns))
	columns := make([]string, len(spec.Columns))
	for i, col := range spec.Columns {
		placeholders[i] = "?"
		columns[i] = quoteIdentifier(col)
	}
	query := fmt.Sprintf(
		"INSERT INTO bronze.%s (%s) VALUES (%s)",
		spec.TableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)
	if _, err := tx.Exec(query, values...); err != nil {
		return fmt.Errorf("insert %s: %w", spec.TableName, err)
	}
	return nil
}

func typedValues(spec typedTableSpec, value reflect.Value, bronzeRow *componentsv1.BronzeRow) ([]any, error) {
	values := make([]any, 0, len(spec.Columns))
	for _, col := range spec.Columns {
		if defaultValue, ok := spec.ColumnDefaults[col]; ok {
			values = append(values, defaultValue)
			continue
		}
		fieldName := columnFieldName(spec, col)
		field := value.FieldByName(fieldName)
		if !field.IsValid() {
			values = append(values, nil)
			continue
		}
		values = append(values, sqlValue(field))
	}
	return values, nil
}

func columnFieldName(spec typedTableSpec, column string) string {
	if override, ok := spec.ColumnOverrides[column]; ok {
		return override
	}
	return snakeToExported(column)
}

func sqlValue(value reflect.Value) any {
	if !value.IsValid() {
		return nil
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
		return sqlValue(value.Elem())
	}
	if value.Type() == reflect.TypeOf(time.Time{}) {
		return value.Interface()
	}
	if value.Kind() == reflect.Slice || value.Kind() == reflect.Map || value.Kind() == reflect.Struct {
		data, err := json.Marshal(value.Interface())
		if err != nil {
			return nil
		}
		return string(data)
	}
	return value.Interface()
}

func typedBronzeValuesSQL(row *componentsv1.BronzeRow) (string, error) {
	spec, ok := typedTableSpecs[row.TableName]
	if !ok {
		return "", nil
	}
	value := reflect.New(spec.RowType)
	if err := json.Unmarshal([]byte(row.RowJson), value.Interface()); err != nil {
		return "", fmt.Errorf("unmarshal typed row: %w", err)
	}
	values, err := typedValues(spec, value.Elem(), row)
	if err != nil {
		return "", err
	}
	literals := make([]string, len(values))
	for i, value := range values {
		literals[i] = sqlLiteral(value)
	}
	return "(" + strings.Join(literals, ", ") + ")", nil
}

func sqlLiteral(value any) string {
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

func qualifyCreateTableSQL(sqlText, catalog, schema string) string {
	sqlText = strings.TrimSpace(sqlText)
	if schema != "" {
		return strings.Replace(sqlText, "CREATE TABLE IF NOT EXISTS "+schema+".", "CREATE TABLE IF NOT EXISTS "+catalog+"."+schema+".", 1)
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

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(strings.Trim(value, `"`), `"`, `""`) + `"`
}

func splitSQLStatements(sqlText string) []string {
	sqlText = strings.ReplaceAll(sqlText, "bronze.", "bronze.")
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

func tableSpec(table string, row any, ledgerColumn string, columns []string, overrides map[string]string) typedTableSpec {
	return typedTableSpec{
		TableName:       table,
		Columns:         columns,
		RowType:         reflect.TypeOf(row),
		LedgerColumn:    ledgerColumn,
		ColumnOverrides: overrides,
		ColumnDefaults:  map[string]any{"version_label": contracts.ExtractionVersion},
	}
}

var typedTableSpecs = map[string]typedTableSpec{
	"ledgers_row_v2": tableSpec("ledgers_row_v2", extract.LedgerRowData{}, "sequence", []string{
		"sequence", "ledger_hash", "previous_ledger_hash", "closed_at", "protocol_version", "total_coins", "fee_pool", "base_fee", "base_reserve", "max_tx_set_size", "successful_tx_count", "failed_tx_count", "ingestion_timestamp", "ledger_range", "transaction_count", "operation_count", "tx_set_operation_count", "soroban_fee_write1kb", "node_id", "signature", "ledger_header", "bucket_list_size", "live_soroban_state_size", "evicted_keys_count", "soroban_op_count", "total_fee_charged", "contract_events_count", "era_id", "version_label",
	}, nil),
	"transactions_row_v2": tableSpec("transactions_row_v2", extract.TransactionData{}, "ledger_sequence", []string{
		"ledger_sequence", "transaction_hash", "source_account", "fee_charged", "max_fee", "successful", "transaction_result_code", "operation_count", "memo_type", "memo", "created_at", "account_sequence", "ledger_range", "source_account_muxed", "fee_account_muxed", "inner_transaction_hash", "fee_bump_fee", "max_fee_bid", "inner_source_account", "timebounds_min_time", "timebounds_max_time", "ledgerbounds_min", "ledgerbounds_max", "min_sequence_number", "min_sequence_age", "soroban_resources_instructions", "soroban_resources_read_bytes", "soroban_resources_write_bytes", "soroban_data_size_bytes", "soroban_data_resources", "soroban_fee_base", "soroban_fee_resources", "soroban_fee_refund", "soroban_fee_charged", "soroban_fee_wasted", "soroban_host_function_type", "soroban_contract_id", "soroban_contract_events_count", "signatures_count", "new_account", "rent_fee_charged", "tx_envelope", "tx_result", "tx_meta", "tx_fee_meta", "tx_signers", "extra_signers", "era_id", "version_label", "transaction_id",
	}, nil),
	"operations_row_v2": tableSpec("operations_row_v2", extract.OperationData{}, "ledger_sequence", []string{
		"transaction_hash", "operation_index", "ledger_sequence", "source_account", "type", "type_string", "created_at", "transaction_successful", "operation_result_code", "operation_trace_code", "ledger_range", "source_account_muxed", "asset", "asset_type", "asset_code", "asset_issuer", "source_asset", "source_asset_type", "source_asset_code", "source_asset_issuer", "amount", "source_amount", "destination_min", "starting_balance", "destination", "trustline_limit", "trustor", "authorize", "authorize_to_maintain_liabilities", "trust_line_flags", "balance_id", "claimants_count", "sponsored_id", "offer_id", "price", "price_r", "buying_asset", "buying_asset_type", "buying_asset_code", "buying_asset_issuer", "selling_asset", "selling_asset_type", "selling_asset_code", "selling_asset_issuer", "soroban_operation", "soroban_function", "soroban_contract_id", "soroban_auth_required", "bump_to", "set_flags", "clear_flags", "home_domain", "master_weight", "low_threshold", "medium_threshold", "high_threshold", "data_name", "data_value", "era_id", "version_label", "transaction_index", "soroban_arguments_json", "contract_calls_json", "contracts_involved", "max_call_depth", "transaction_id", "operation_id", "soroban_auth_credentials_types", "soroban_auth_addresses",
	}, map[string]string{"type": "OpType"}),
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
		"contract_id", "creator_address", "wasm_hash", "created_ledger", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
	"token_transfers_stream_v1": tableSpec("token_transfers_stream_v1", extract.TokenTransferData{}, "ledger_sequence", []string{
		"ledger_sequence", "transaction_hash", "transaction_id", "operation_id", "operation_index", "event_type", "from", "to", "asset", "asset_type", "asset_code", "asset_issuer", "amount", "amount_raw", "contract_id", "closed_at", "created_at", "ledger_range", "era_id", "version_label",
	}, nil),
}
