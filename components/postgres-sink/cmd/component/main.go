package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq"
	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	"github.com/withObsrvr/flowctl-sdk/pkg/consumer"
	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	"google.golang.org/protobuf/proto"
)

func main() {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	if err := ensureSchema(context.Background(), db); err != nil {
		panic(err)
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
		if _, err := tx.ExecContext(ctx, `
insert into stellar_ledgers (
  id, network_passphrase, ledger_sequence, closed_at_unix, ledger_hash,
  previous_ledger_hash, protocol_version, transaction_count, schema_version, extraction_version
) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
on conflict (id) do update set
  transaction_count = excluded.transaction_count,
  schema_version = excluded.schema_version,
  extraction_version = excluded.extraction_version`,
			row.Id, row.NetworkPassphrase, row.LedgerSequence, row.ClosedAtUnix, row.LedgerHash,
			row.PreviousLedgerHash, row.ProtocolVersion, row.TransactionCount, row.SchemaVersion, row.ExtractionVersion); err != nil {
			return err
		}
	}

	for _, row := range batch.Transactions {
		if _, err := tx.ExecContext(ctx, `
insert into stellar_transactions (
  id, network_passphrase, ledger_sequence, transaction_index, transaction_hash,
  successful, envelope_xdr, result_xdr, meta_xdr
) values ($1,$2,$3,$4,$5,$6,$7,$8,$9)
on conflict (id) do update set
  successful = excluded.successful,
  envelope_xdr = excluded.envelope_xdr,
  result_xdr = excluded.result_xdr,
  meta_xdr = excluded.meta_xdr`,
			row.Id, row.NetworkPassphrase, row.LedgerSequence, row.TransactionIndex, row.TransactionHash,
			row.Successful, row.EnvelopeXdr, row.ResultXdr, row.MetaXdr); err != nil {
			return err
		}
	}

	for _, row := range batch.Operations {
		if _, err := tx.ExecContext(ctx, `
insert into stellar_operations (
  id, transaction_id, network_passphrase, ledger_sequence, transaction_index,
  operation_index, operation_type, operation_xdr
) values ($1,$2,$3,$4,$5,$6,$7,$8)
on conflict (id) do update set
  operation_type = excluded.operation_type,
  operation_xdr = excluded.operation_xdr`,
			row.Id, row.TransactionId, row.NetworkPassphrase, row.LedgerSequence, row.TransactionIndex,
			row.OperationIndex, row.OperationType, row.OperationXdr); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

const schemaSQL = `
create table if not exists stellar_ledgers (
  id text primary key,
  network_passphrase text not null,
  ledger_sequence integer not null,
  closed_at_unix bigint not null,
  ledger_hash text not null,
  previous_ledger_hash text not null,
  protocol_version integer not null,
  transaction_count integer not null,
  schema_version text not null,
  extraction_version text not null,
  unique (network_passphrase, ledger_sequence)
);

create table if not exists stellar_transactions (
  id text primary key,
  network_passphrase text not null,
  ledger_sequence integer not null,
  transaction_index integer not null,
  transaction_hash text not null,
  successful boolean not null,
  envelope_xdr text not null,
  result_xdr text not null,
  meta_xdr text not null,
  unique (network_passphrase, ledger_sequence, transaction_index)
);

create table if not exists stellar_operations (
  id text primary key,
  transaction_id text not null references stellar_transactions(id) on delete cascade,
  network_passphrase text not null,
  ledger_sequence integer not null,
  transaction_index integer not null,
  operation_index integer not null,
  operation_type text not null,
  operation_xdr text not null,
  unique (network_passphrase, ledger_sequence, transaction_index, operation_index)
);
`
