package pgxload

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"unicode"

	"github.com/jackc/pgproto3/v2"
	"github.com/jmoiron/sqlx/reflectx"
)

type FieldDescriptionProvider interface {
	FieldDescriptions() []pgproto3.FieldDescription
}

// Determine if a string is all upper case
func IsAllUpper(local string) bool {

	upper := true
	for _, f := range local {
		if !unicode.IsUpper(f) {
			upper = false
			return upper
		}
	}

	return upper
}

// Determine column names from pgx Rows
func ColumnNames(r FieldDescriptionProvider) ([]string, error) {
	fieldDescriptions := r.FieldDescriptions()
	names := make([]string, 0, len(fieldDescriptions))
	for _, fd := range fieldDescriptions {
		names = append(names, string(fd.Name))
	}
	return names, nil
}

// Returns the type of one of a slices elements
func SliceElemType(val reflect.Value) reflect.Type {
	elemType := val.Type().Elem()
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}

	return elemType
}

// ReflectAppend will append val to slice.
func ReflectAppend(slice, val reflect.Value) {
	if slice.Type().Elem().Kind() == reflect.Ptr {
		slice.Set(reflect.Append(slice, val))
	} else {
		slice.Set(reflect.Append(slice, reflect.Indirect(val)))
	}
}

// fieldsByName fills a values interface with fields from the passed value based
// on the traversals in int.  If ptrs is true, return addresses instead of values.
// We write this instead of using FieldsByName to save allocations and map lookups
// when iterating over many rows.  Empty traversals will get an interface pointer.
// Because of the necessity of requesting ptrs or values, it's considered a bit too
// specialized for inclusion in reflectx itself.
func fieldsByTraversal(v reflect.Value, traversals [][]int, values []interface{}, ptrs bool) error {
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Struct {
		return errors.New("argument not a struct")
	}

	for i, traversal := range traversals {
		if len(traversal) == 0 {
			values[i] = new(interface{})
			continue
		}
		f := reflectx.FieldByIndexes(v, traversal)
		if ptrs {
			values[i] = f.Addr().Interface()
		} else {
			values[i] = f.Interface()
		}
	}
	return nil
}

// Prepare and validate an input destination to be scanned
func prepareInput(dest interface{}) (reflect.Value, error) {

	val := reflect.ValueOf(dest)

	if val.Kind() != reflect.Ptr {
		return reflect.Value{}, errors.New("can only scan into a pointer")
	}

	if !val.Elem().CanSet() {
		return reflect.Value{}, errors.New("can only scan into a publicly set-able destination (is this a private field? has it been initialized?)")
	}

	return reflect.Indirect(val), nil
}

// Detect missing columns from struct traversals
func missingColumns(columnNames []string, traversals [][]int) error {

	var missingColNames []string

	for colIdx, t := range traversals {
		if len(t) == 0 {
			if len(columnNames) <= colIdx {
				missingColNames = append(missingColNames, "UNKNOWN")
			} else {
				missingColNames = append(missingColNames, columnNames[colIdx])
			}
		}
	}

	if len(columnNames) > len(traversals) {
		missingColNames = append(missingColNames, columnNames[len(traversals):]...)
	}

	if len(missingColNames) == 1 {
		return errors.New("missing destination name: "+missingColNames[0])
	} else if len(missingColNames) > 1 {
		return errors.New("missing destination names: "+strings.Join(missingColNames,", "))
	}

	return nil
}

func isDirectlyScannable(in interface{}) bool {

	if _, isScanner := in.(sql.Scanner); isScanner {
		return true
	}

	t := reflect.TypeOf(in)
	if t.PkgPath() != "" {
		return true
	}

	k := t.Kind()
	if  k == reflect.Array || k == reflect.Chan || k == reflect.Map ||
		k == reflect.Ptr || k == reflect.Slice {
		return isDirectlyScannable(t.Elem()) || k == reflect.Map && isDirectlyScannable(t.Key())
	} else if k == reflect.Struct {
		for i := t.NumField() - 1; i >= 0; i-- {
			if isDirectlyScannable(t.Field(i).Type) {
				return true
			}
		}
	}

	return false
}

// Run the specified function in the given transaction
// Will automatically rollback if the function returns an error, and commit if it does not
func RunInTransaction(ctx context.Context, loader PgxLoader, fn func(ctx context.Context, tx PgxTxLoader) error) error {

	tx, err := loader.Begin(ctx)
	if err != nil {
		return err
	}

	return internalRunInTransaction(ctx, NewPgxTxLoader(loader, tx), fn)
}

func internalRunInTransaction(ctx context.Context, tx PgxTxLoader, fn func(ctx context.Context, tx PgxTxLoader) error) error {
	defer func() {
		if err := recover(); err != nil {
			_ = tx.Rollback(ctx)
			panic(err)
		}
	}()

	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}

	return tx.Commit(ctx)
}