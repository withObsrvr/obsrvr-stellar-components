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
