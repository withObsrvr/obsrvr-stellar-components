package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	"github.com/withObsrvr/flowctl-sdk/pkg/consumer"
	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	"google.golang.org/protobuf/proto"
)

type postgresConfig struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

func main() {
	cfg, err := postgresConfigFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	db, err := openPostgres(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := ensureSchema(context.Background(), db); err != nil {
		log.Fatal(err)
	}

	consumer.Run(consumer.ConsumerConfig{
		ConsumerName: "Stellar Ledger Postgres Sink",
		ComponentID:  getenv("COMPONENT_ID", "postgres-sink"),
		InputTypes:   []string{contracts.LedgerBatchEventType},
		OnEvent: func(ctx context.Context, event *flowctlv1.Event) error {
			if event.Type != contracts.LedgerBatchEventType {
				return nil
			}
			var batch componentsv1.LedgerBatch
			if err := proto.Unmarshal(event.Payload, &batch); err != nil {
				return fmt.Errorf("unmarshal ledger batch: %w", err)
			}
			return writeBatch(ctx, db, &batch)
		},
	})
}

func postgresConfigFromEnv() (postgresConfig, error) {
	dsn := strings.TrimSpace(os.Getenv("POSTGRES_DSN"))
	if dsn == "" {
		return postgresConfig{}, fmt.Errorf("POSTGRES_DSN is required")
	}
	maxOpen, err := getenvInt("POSTGRES_MAX_OPEN_CONNS", 8)
	if err != nil {
		return postgresConfig{}, err
	}
	maxIdle, err := getenvInt("POSTGRES_MAX_IDLE_CONNS", maxOpen)
	if err != nil {
		return postgresConfig{}, err
	}
	lifetime, err := getenvDuration("POSTGRES_CONN_MAX_LIFETIME", 30*time.Minute)
	if err != nil {
		return postgresConfig{}, err
	}
	return postgresConfig{
		DSN:             dsn,
		MaxOpenConns:    maxOpen,
		MaxIdleConns:    maxIdle,
		ConnMaxLifetime: lifetime,
	}, nil
}

func openPostgres(ctx context.Context, cfg postgresConfig) (*sql.DB, error) {
	db, err := sql.Open("postgres", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open Postgres: %w", err)
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping Postgres: %w", err)
	}
	return db, nil
}

func ensureSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, schemaSQL)
	return err
}

func writeBatch(ctx context.Context, db *sql.DB, batch *componentsv1.LedgerBatch) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, row := range batch.Ledgers {
		if _, err := tx.ExecContext(ctx, upsertLedgerSQL,
			row.Id, row.NetworkPassphrase, row.LedgerSequence, row.ClosedAtUnix, row.LedgerHash,
			row.PreviousLedgerHash, row.ProtocolVersion, row.TransactionCount, row.SchemaVersion, row.ExtractionVersion); err != nil {
			return err
		}
	}

	for _, row := range batch.Transactions {
		if _, err := tx.ExecContext(ctx, upsertTransactionSQL,
			row.Id, row.NetworkPassphrase, row.LedgerSequence, row.TransactionIndex, row.TransactionHash,
			row.Successful, row.EnvelopeXdr, row.ResultXdr, row.MetaXdr); err != nil {
			return err
		}
	}

	for _, row := range batch.Operations {
		if _, err := tx.ExecContext(ctx, upsertOperationSQL,
			row.Id, row.TransactionId, row.NetworkPassphrase, row.LedgerSequence, row.TransactionIndex,
			row.OperationIndex, row.OperationType, row.OperationXdr); err != nil {
			return err
		}
	}

	if err := deleteBronzeRowsForBatch(ctx, tx, batch); err != nil {
		return err
	}
	for _, row := range batch.BronzeRows {
		if _, err := tx.ExecContext(ctx, insertBronzeRowSQL,
			row.Id, row.TableName, row.NetworkPassphrase, row.LedgerSequence, row.LedgerRange, cleanJSONB(row.RowJson)); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func deleteBronzeRowsForBatch(ctx context.Context, tx *sql.Tx, batch *componentsv1.LedgerBatch) error {
	type key struct {
		network string
		ledger  uint32
		table   string
	}
	seen := map[key]struct{}{}
	for _, row := range batch.BronzeRows {
		k := key{network: row.NetworkPassphrase, ledger: row.LedgerSequence, table: row.TableName}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		if _, err := tx.ExecContext(ctx, deleteBronzeRowsSQL, k.network, k.ledger, k.table); err != nil {
			return fmt.Errorf("delete bronze rows for %s ledger %d table %s: %w", k.network, k.ledger, k.table, err)
		}
	}
	return nil
}

func cleanJSONB(value string) string {
	return strings.ReplaceAll(value, "\x00", "")
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvInt(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be positive", key)
	}
	return value, nil
}

func getenvDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err == nil {
		return value, nil
	}
	seconds, secondsErr := time.ParseDuration(raw + "s")
	if secondsErr != nil {
		return 0, fmt.Errorf("%s must be a duration like 30m or a number of seconds: %w", key, err)
	}
	return seconds, nil
}

const upsertLedgerSQL = `
insert into stellar_ledgers (
  id, network_passphrase, ledger_sequence, closed_at_unix, ledger_hash,
  previous_ledger_hash, protocol_version, transaction_count, schema_version, extraction_version
) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
on conflict (network_passphrase, ledger_sequence) do update set
  id = excluded.id,
  closed_at_unix = excluded.closed_at_unix,
  ledger_hash = excluded.ledger_hash,
  previous_ledger_hash = excluded.previous_ledger_hash,
  protocol_version = excluded.protocol_version,
  transaction_count = excluded.transaction_count,
  schema_version = excluded.schema_version,
  extraction_version = excluded.extraction_version`

const upsertTransactionSQL = `
insert into stellar_transactions (
  id, network_passphrase, ledger_sequence, transaction_index, transaction_hash,
  successful, envelope_xdr, result_xdr, meta_xdr
) values ($1,$2,$3,$4,$5,$6,$7,$8,$9)
on conflict (network_passphrase, ledger_sequence, transaction_index) do update set
  id = excluded.id,
  transaction_hash = excluded.transaction_hash,
  successful = excluded.successful,
  envelope_xdr = excluded.envelope_xdr,
  result_xdr = excluded.result_xdr,
  meta_xdr = excluded.meta_xdr`

const upsertOperationSQL = `
insert into stellar_operations (
  id, transaction_id, network_passphrase, ledger_sequence, transaction_index,
  operation_index, operation_type, operation_xdr
) values ($1,$2,$3,$4,$5,$6,$7,$8)
on conflict (network_passphrase, ledger_sequence, transaction_index, operation_index) do update set
  id = excluded.id,
  transaction_id = excluded.transaction_id,
  operation_type = excluded.operation_type,
  operation_xdr = excluded.operation_xdr`

const deleteBronzeRowsSQL = `
delete from stellar_bronze_rows
where network_passphrase = $1
  and ledger_sequence = $2
  and table_name = $3`

const insertBronzeRowSQL = `
insert into stellar_bronze_rows (
  id, table_name, network_passphrase, ledger_sequence, ledger_range, row_json
) values ($1,$2,$3,$4,$5,$6::jsonb)`

const schemaSQL = `
create table if not exists stellar_ledgers (
  id text not null,
  network_passphrase text not null,
  ledger_sequence integer not null,
  closed_at_unix bigint not null,
  ledger_hash text not null,
  previous_ledger_hash text not null,
  protocol_version integer not null,
  transaction_count integer not null,
  schema_version text not null,
  extraction_version text not null,
  primary key (network_passphrase, ledger_sequence)
);

create table if not exists stellar_transactions (
  id text not null,
  network_passphrase text not null,
  ledger_sequence integer not null,
  transaction_index integer not null,
  transaction_hash text not null,
  successful boolean not null,
  envelope_xdr text not null,
  result_xdr text not null,
  meta_xdr text not null,
  primary key (network_passphrase, ledger_sequence, transaction_index)
);

create table if not exists stellar_operations (
  id text not null,
  transaction_id text not null,
  network_passphrase text not null,
  ledger_sequence integer not null,
  transaction_index integer not null,
  operation_index integer not null,
  operation_type text not null,
  operation_xdr text not null,
  primary key (network_passphrase, ledger_sequence, transaction_index, operation_index)
);

create table if not exists stellar_bronze_rows (
  id text primary key,
  table_name text not null,
  network_passphrase text not null,
  ledger_sequence integer not null,
  ledger_range integer not null,
  row_json jsonb not null
);

create index if not exists stellar_bronze_rows_table_ledger_idx
  on stellar_bronze_rows(table_name, ledger_sequence);

create index if not exists stellar_bronze_rows_range_idx
  on stellar_bronze_rows(network_passphrase, ledger_range);
`
