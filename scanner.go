package pgxload

import (
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v4"
	"github.com/jmoiron/sqlx/reflectx"
)


type Scanner interface {
	Scan(dest ...interface{}) error
}

func NewScanner(rows pgx.Rows, mapper *reflectx.Mapper) Scanner {
	return &scanner{
		rows:   rows,
		mapper: mapper,
	}
}

type scanner struct {
	rows            pgx.Rows
	mapper          *reflectx.Mapper
	values          []interface{}
	cols            []string
	colsInitialized bool
}

func (s *scanner) Scan(dest ...interface{}) error {
	defer s.rows.Close()

	if dest == nil || len(dest) == 0 {
		return nil
	}

	val, err := prepareInput(dest[0])
	if err != nil {
		return err
	}

	if val.Kind() == reflect.Slice {

		sliceOf := SliceElemType(val)

		for s.rows.Next() {
			sliceVal := reflect.New(sliceOf)

			err := s.scanStruct(sliceVal)
			if err != nil {
				return err
			}

			ReflectAppend(val, sliceVal)
		}

	} else if val.Kind() == reflect.Struct {
		for s.rows.Next() {

			err = s.scanStruct(val)
			if err != nil {
				return err
			}
		}
	}

	return s.rows.Err()
}

func (s *scanner) scanStruct(v reflect.Value) error {

	err := s.initColsIfNecessary()
	if err != nil {
		return err
	}

	fieldTraversals := s.mapper.TraversalsByName(v.Type(), s.cols)

	err = missingColumns(s.cols, fieldTraversals)
	if err != nil {
		return err
	}
	fmt.Println(s.cols)
	fmt.Println(fieldTraversals)

	err = fieldsByTraversal(v, fieldTraversals, s.values, true)
	if err != nil {
		return err
	}

	err = s.rows.Scan(s.values...)
	if err != nil {
		return err
	}

	return nil
}

func (s *scanner) initColsIfNecessary() error {

	if !s.colsInitialized {
		var err error
		s.cols, err = ColumnNames(s.rows)
		s.values = make([]interface{}, len(s.cols))
		if err != nil {
			return err
		}
	}

	return nil

}
