package pgxload

import (
	"errors"

	"github.com/jmoiron/sqlx/reflectx"
)

func NewStructInsert(tableName string, data ...interface{}) StructInsert {
	return StructInsert{
		tableName:    tableName,
		data:         data,
		returning:    "",
		conflictStmt: "",
	}
}

type StructInsert struct {
	tableName    string
	data         []interface{}
	returning    string
	conflictStmt string
}

func (ins StructInsert) WithConflict(str string) StructInsert {
	return StructInsert{
		tableName:    ins.tableName,
		data:         ins.data,
		returning:    ins.returning,
		conflictStmt: str,
	}
}

func (ins StructInsert) WithReturning(str string) StructInsert {
	return StructInsert{
		tableName:    ins.tableName,
		data:         ins.data,
		returning:    str,
		conflictStmt: ins.conflictStmt,
	}
}

func (ins StructInsert) WithReturningColumns(cols ...string) StructInsert {
	return ins.WithReturning(ToColumnList(cols...))
}

func (ins StructInsert) GenerateInsert(m *reflectx.Mapper) (string, []interface{}, error) {

	if len(ins.data) == 0 {
		return "", nil, errors.New("missing input to insert")
	}

	stmt := "INSERT INTO " + ins.tableName + " "
	var params []interface{}
	var paramOffset int

	extractor, err := NewStructColumnValueExtractor(m, ins.data[0])
	if err != nil {
		return "", nil, err
	}

	for idx, dat := range ins.data {
		extracted, err := extractor.Extract(dat)
		if err != nil {
			return "", nil, err
		}

		if idx == 0 {
			stmt += extracted.InsertColumnSyntax() + " VALUES "
		}

		var toAdd string
		var toAddParams []interface{}
		toAdd, toAddParams, paramOffset = extracted.InsertValueSyntax(paramOffset)

		paramOffset += 1

		stmt += toAdd
		params = append(params, toAddParams...)

		if idx < len(ins.data)-1 {
			stmt += ", "
		}

	}

	if len(ins.conflictStmt) > 0 {
		stmt += " ON CONFLICT " + ins.conflictStmt
	}

	if len(ins.returning) > 0 {
		stmt += " RETURNING " + ins.returning
	}

	return stmt, params, nil
}
