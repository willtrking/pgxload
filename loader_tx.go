package pgxload

import (
	"github.com/jackc/pgx/v4"
	"github.com/jmoiron/sqlx/reflectx"
)

// A tx loader providing a pgx transaction interface, and ability to generate a scanner
type PgxTxLoader interface {
	pgx.Tx

	// The common loader funcs
	CommonLoader
}


func NewPgxTxLoader(existing PgxLoader, tx pgx.Tx) PgxTxLoader {

	return &pgxTxLoader{
		loader:existing,
		Tx: tx,
	}
}

type pgxTxLoader struct {
	pgx.Tx
	loader PgxLoader
}

// The reflectx mapper this loader uses
func (p *pgxTxLoader) Mapper() *reflectx.Mapper {
	return p.loader.Mapper()
}

// Create a new Scanner for the specified rows and the underlying reflectx mapper
func (p *pgxTxLoader) Scanner(rows pgx.Rows) Scanner {

	return NewScanner(rows, p.Mapper())
}