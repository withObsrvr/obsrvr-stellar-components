# Stellar dependency upgrade — 2026-08-04

## Pins

- `github.com/stellar/go-stellar-sdk v0.7.1`
- `github.com/withObsrvr/stellar-extract v0.1.4`
- `github.com/withObsrvr/flowctl-sdk v0.1.3`

`vendor/` is regenerated from these pins.

## Compatibility work

`go-stellar-sdk v0.7.1` includes the Protocol 28 XDR surface. The corresponding
`stellar-extract v0.1.4` release handles the new `SCV_EXECUTABLE_TAG`,
`CONTRACT_EXECUTABLE_EXTERNAL_REF`, and `STELLAR_VALUE_EMPTY_TX_SET` union arms.
It also corrects `rent_fee_charged` extraction from TransactionMeta V4, which
has been used since Protocol 23.

DuckLake now materializes the additive `contract_creations_v1` columns:

- `executable_type`
- `external_ref_owner`
- `external_ref_tag`

Migration `002_contract_executable_columns` adds them idempotently to existing
catalogs. Remote Quack migration qualification was extended to handle `ALTER
TABLE`, and the regression test requires the fully qualified remote statements.
The Quack chaos gate also checks all three columns and rejects NULL
`rent_fee_charged` on observed Soroban transactions.

`flowctl-sdk v0.1.3` only aligns dependencies/examples with
`go-stellar-sdk v0.7.1`; its exported runtime packages are unchanged from
`v0.1.2`. It does not close the open delivery-backpressure, registration-retry,
or health-reporting work.

## Validation

- `go test ./...`
- `go vet ./...`
- race tests for the processor, DuckLake sink/server, bronze, and normalization
- upstream `stellar-extract v0.1.4` protocol-coverage tests and vet
- all pipeline validations
- all component builds
- Quack kill/replay/maintenance chaos gate over ledgers `62080000–62080002`
- exact chaos/baseline table parity
- zero watermark gaps
- typed XDR/Soroban, TransactionMeta V4 rent-fee, and Protocol 28 schema gates

Raw chaos evidence is retained at
`/tmp/obsrvr-quack-chaos-dependency-upgrade-20260803`.

## Open data operation

Previously materialized Protocol 23-and-newer `transactions_row_v2` rows retain
the old NULL `rent_fee_charged` values until replayed. Plan and review that
backfill separately; this dependency upgrade does not mutate historical rows.
