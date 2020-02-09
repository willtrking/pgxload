package pgxload

import (
	"errors"

	"github.com/jackc/pgx/v4"
	"github.com/jmoiron/sqlx/reflectx"
)

// Interface providing funcs that all loaders must provide
type CommonLoader interface {
	// The reflectx mapper this loader uses
	Mapper() *reflectx.Mapper

	// Create a new Scanner for the specified rows and the underlying reflectx mapper
	Scanner(rows pgx.Rows) Scanner
}

// A loader providing a pgx connection interface, and ability to generate a scanner
type PgxLoader interface {
	// The underlying PGX connection
	PGXConn

	// The common loader funcs
	CommonLoader
}


// Configuration options to pass to reflectx.NewMapperFunc
type Config struct {
	StructTag string
	Mapper    func(string) string
}

func (c *Config) generateMapper() *reflectx.Mapper {
	return reflectx.NewMapperFunc(c.StructTag, c.Mapper)
}

// Default config used by NewPGXLoader
// Will use `db` struct tag an our CamelToSnakeCase mapper
var DefaultConfig = &Config{
	StructTag: "db",
	Mapper:    CamelToSnakeCase,
}

// Create a new PgxLoader with our CamelToSnakeCase mapper and the `db` struct tag
// If config is nil, will use `db` struct tag an our CamelToSnakeCase mapper
func NewPgxLoader(conn PGXConn, config ...*Config) (PgxLoader, error) {

	if len(config) > 1 {
		return nil, errors.New("specify only 1 config option")
	}

	if len(config) == 1 && config[0] != nil {
		return &pgxLoader{
			mapper:  config[0].generateMapper(),
			PGXConn: conn,
		}, nil
	}

	return &pgxLoader{
		PGXConn: conn,
		mapper:  DefaultConfig.generateMapper(),
	}, nil

}

type pgxLoader struct {
	PGXConn
	mapper *reflectx.Mapper
}

// The reflectx mapper this loader uses
func (p *pgxLoader) Mapper() *reflectx.Mapper {
	return p.mapper
}

// Create a new Scanner for the specified rows and the underlying reflectx mapper
func (p *pgxLoader) Scanner(rows pgx.Rows) Scanner {

	return NewScanner(rows, p.Mapper())
}
