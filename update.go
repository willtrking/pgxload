package pgxload

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx/reflectx"
)

func NewStructUpdate(tableName string, data interface{}) StructUpdate {
	return StructUpdate{
		tableName: tableName,
		data:      data,
		returning: "",
	}
}

type StructUpdate struct {
	tableName string
	data      interface{}
	returning string
}

func (upd StructUpdate) WithReturning(str string) StructUpdate {
	return StructUpdate{
		tableName: upd.tableName,
		data:      upd.data,
		returning: str,
	}
}

func (upd StructUpdate) getFieldMap(m *reflectx.Mapper) (map[string]reflect.Value, error) {

	if upd.data == nil {
		return nil, errors.New("missing data to update")
	}

	val := reflect.ValueOf(upd.data)

	if val.Kind() == reflect.Ptr && val.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("update data is not a struct or pointer to a struct")
	}

	fieldMap := m.FieldMap(val)

	if len(fieldMap) <= 0 {
		return nil, errors.New("unable to extract fields from input struct. are your fields private?")
	}

	return fieldMap, nil

}

func (upd StructUpdate) WithReturningColumns(cols ...string) StructUpdate {
	return upd.WithReturning(ToColumnList(cols...))
}

func (upd StructUpdate) GenerateGenericUpdate(m *reflectx.Mapper, afterUpd string) (string, []interface{}, error) {

	afterUpd = strings.TrimSpace(afterUpd)

	if len(afterUpd) <= 0 {
		return "", nil, errors.New("missing after update statement")
	}

	fieldMap, err := upd.getFieldMap(m)
	if err != nil {
		return "", nil, err
	}

	stmt := "UPDATE " + upd.tableName + " SET "
	var params []interface{}

	addedParam := false
	for fieldName, fieldData := range fieldMap {
		iface := fieldData.Interface()

		if addedParam {
			stmt += ", "
		}

		stmt += QuotedColumn(fieldName) + " = "

		addedParam = true
		if iface == nil {
			stmt += "NULL"
		} else {
			params = append(params, iface)
			stmt += "?"
		}
	}

	return stmt + " " + afterUpd, params, nil
}

func (upd StructUpdate) GenerateExactUpdate(m *reflectx.Mapper, columnToMatch string) (string, []interface{}, error) {

	columnToMatch = strings.TrimSpace(columnToMatch)

	if len(columnToMatch) <= 0 {
		return "", nil, errors.New("missing column to match")
	}

	stmt := "UPDATE " + upd.tableName + " SET "

	extractor, err := NewStructColumnValueExtractor(m, upd.data)
	if err != nil {
		return "", nil, err
	}

	extractor = extractor.WithOmitColumns(columnToMatch)

	toMatchVal, err := extractor.ExtractRawColumnData(upd.data, columnToMatch)
	if err != nil {
		return "", nil, err
	}

	if toMatchVal == nil {
		return "", nil, errors.New("failed to locate column to match")
	}

	extracted, err := extractor.Extract(upd.data)
	if err != nil {
		return "", nil, err
	}

	updStmt, params, offset := extracted.UpdateSyntax(0)
	params = append(params, toMatchVal)

	stmt += fmt.Sprintf("%s WHERE %s = $%d", updStmt, columnToMatch, offset+1)

	if len(upd.returning) > 0 {
		stmt += " RETURNING " + upd.returning
	}

	return stmt, params, nil
}
