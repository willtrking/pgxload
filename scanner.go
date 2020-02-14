package pgxload

import (
	"errors"
	"reflect"

	"github.com/jackc/pgx/v4"
	"github.com/jmoiron/sqlx/reflectx"
)

type Scanner interface {
	Scan(dest ...interface{}) error

	ScanRow(dest interface{}) error
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

func (s *scanner) ScanRow(dest interface{}) error {

	defer s.rows.Close()

	gotRow := false
	if isDirectlyScannable(dest) {

		for s.rows.Next() {

			if gotRow {
				return errors.New("scan row found multiple rows, expected one")
			}

			gotRow = true

			err := s.rows.Scan(dest)
			if err != nil {
				return err
			}
		}

	} else {
		val, err := prepareInput(dest)
		if err != nil {
			return err
		}

		for s.rows.Next() {

			if gotRow {
				return errors.New("scan row found multiple rows, expected one")
			}

			gotRow = true
			err = s.scanStruct(val)
			if err != nil {
				return err
			}
		}
	}

	if !gotRow {
		return pgx.ErrNoRows
	}

	return nil

}

func (s *scanner) Scan(dest ...interface{}) error {
	defer s.rows.Close()

	if dest == nil || len(dest) == 0 {
		// Nothing specified to scan into. Immediately return
		return nil
	} else if len(dest) > 1 || (len(dest) == 1 && isDirectlyScannable(dest[0])) {
		// Variadic values specified. Scan into them, but if there are MULTIPLE ROWS returned then return an error
		gotRow := false
		for s.rows.Next() {

			if gotRow {
				return errors.New("variadic arguments specified to scan, but more then 1 row returned so specified arguments will be continually overwritten, use a slice instead")
			}

			gotRow = true
			err := s.rows.Scan(dest...)
			if err != nil {
				return err
			}
		}

	} else {

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

			// One element specified. Scan into it, but if there are MULTIPLE ROWS returned then return an error
			gotRow := false
			for s.rows.Next() {

				if gotRow {
					return errors.New("one argument specified to scan, but more then 1 row returned so specified arguments will be continually overwritten, use a slice instead")
				}

				gotRow = true
				err = s.scanStruct(val)
				if err != nil {
					return err
				}
			}
		}
	}

	return s.rows.Err()
}

// Scan an individual struct
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
