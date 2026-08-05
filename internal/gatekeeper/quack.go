package gatekeeper

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
)

type QuackConfig struct {
	URI        string
	Token      string
	RemoteDB   string
	DisableSSL bool
}

type QuackRemote struct {
	db       *sql.DB
	querySQL string
}

func OpenQuackRemote(ctx context.Context, cfg QuackConfig) (*QuackRemote, error) {
	if cfg.Token == "" {
		return nil, fmt.Errorf("QUACK_TOKEN is required")
	}
	if err := validateName("QUACK_REMOTE_DB", cfg.RemoteDB); err != nil {
		return nil, err
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open DuckDB Quack client: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, statement := range []string{
		"INSTALL quack",
		"LOAD quack",
		fmt.Sprintf("ATTACH '%s' AS %s (TOKEN '%s', DISABLE_SSL %t)", escapeSQLString(cfg.URI), quoteIdentifier(cfg.RemoteDB), escapeSQLString(cfg.Token), cfg.DisableSSL),
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("initialize Quack client: %w", err)
		}
	}
	return &QuackRemote{db: db, querySQL: fmt.Sprintf("SELECT * FROM %s.query(?)", quoteIdentifier(cfg.RemoteDB))}, nil
}

func (q *QuackRemote) Close() error {
	return q.db.Close()
}

func (q *QuackRemote) Execute(ctx context.Context, statement string) error {
	rows, err := q.db.QueryContext(ctx, q.querySQL, statement)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
	}
	return rows.Err()
}

func (q *QuackRemote) QueryBool(ctx context.Context, statement string) (bool, error) {
	rows, err := q.db.QueryContext(ctx, q.querySQL, statement)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return false, fmt.Errorf("query returned no rows")
	}
	var value bool
	if err := rows.Scan(&value); err != nil {
		return false, err
	}
	if rows.Next() {
		return false, fmt.Errorf("query returned more than one row")
	}
	return value, rows.Err()
}

func (q *QuackRemote) QueryUint64(ctx context.Context, statement string) (uint64, error) {
	rows, err := q.db.QueryContext(ctx, q.querySQL, statement)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, fmt.Errorf("query returned no rows")
	}
	var value uint64
	if err := rows.Scan(&value); err != nil {
		return 0, err
	}
	if rows.Next() {
		return 0, fmt.Errorf("query returned more than one row")
	}
	return value, rows.Err()
}
