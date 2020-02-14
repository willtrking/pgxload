package pgxload

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/jmoiron/sqlx/reflectx"
)

func NewStructColumnValueExtractor(m *reflectx.Mapper, tmpl interface{}) (StructColumnValueExtractor, error) {
	tpe := reflect.TypeOf(tmpl)

	if tpe.Kind() == reflect.Ptr {
		tpe = tpe.Elem()
	}

	if tpe.Kind() != reflect.Struct {
		return StructColumnValueExtractor{}, fmt.Errorf("struct type to use as a template must be a non-nil struct")
	}

	typeMap := m.TypeMap(tpe)

	fieldOpts := make([]structTagOpts, len(typeMap.Index))

	for idx, field := range typeMap.Index {
		fieldOpts[idx] = parseStructTag(field.Field.Tag)
	}

	return StructColumnValueExtractor{
		mapper:    m,
		fields:    typeMap.Index,
		fieldOpts: fieldOpts,
	}, nil
}

type StructColumnValueExtractor struct {
	mapper *reflectx.Mapper

	fields    []*reflectx.FieldInfo
	fieldOpts []structTagOpts

	omitColumns map[string]struct{}
}

func (s StructColumnValueExtractor) tagOpts(num int) structTagOpts {

	return s.fieldOpts[num]
}

func (s StructColumnValueExtractor) omitField(num int, isZero bool) bool {
	if s.tagOpts(num).Omit || s.tagOpts(num).OmitZero && isZero {
		return true
	}

	_, omit := s.omitColumns[s.fields[num].Name]
	return omit
}

func (s StructColumnValueExtractor) WithOmitColumns(cols ...string) StructColumnValueExtractor {

	omitColumns := make(map[string]struct{})

	for _, c := range cols {
		omitColumns[c] = struct{}{}
	}

	return StructColumnValueExtractor{
		mapper:      s.mapper,
		fields:      s.fields,
		fieldOpts:   s.fieldOpts,
		omitColumns: omitColumns,
	}
}

func (s StructColumnValueExtractor) ExtractRawColumnData(data interface{}, column string) (interface{}, error) {
	val := reflect.ValueOf(data)

	for _, field := range s.fields {
		if field.Name == column {
			dataField := reflectx.FieldByIndexes(val, field.Index)

			return dataField.Interface(), nil

		}
	}

	return nil, errors.New("unknown column " + column)
}

func (s StructColumnValueExtractor) Extract(data interface{}) (ExtractedColumnValues, error) {
	var columns []string

	values := make(map[string]ColumnValue)

	val := reflect.ValueOf(data)

	for fieldNum, field := range s.fields {

		fmt.Println(field)
		dataField := reflectx.FieldByIndexesReadOnly(val, field.Index)
		fmt.Println(dataField)

		isZero, err := IsZero(dataField)
		if err != nil {
			return ExtractedColumnValues{}, err
		}

		if s.omitField(fieldNum, isZero) {
			continue
		}

		column := field.Name

		columns = append(columns, column)

		if isZero && s.tagOpts(fieldNum).NullZero {
			values[column] = ColumnValue{
				value:      nil,
				sqlDefault: false,
			}
		} else if isZero && s.tagOpts(fieldNum).DefaultZero {
			values[column] = ColumnValue{
				value:      nil,
				sqlDefault: true,
			}
		} else {
			values[column] = ColumnValue{
				value:      dataField.Interface(),
				sqlDefault: false,
			}
		}

	}

	return ExtractedColumnValues{
		columns:      columns,
		columnValues: values,
	}, nil
}

type ColumnValue struct {
	value      interface{}
	sqlDefault bool
}

func (c ColumnValue) UseNULL() bool {
	return c.value == nil && !c.UseDefault()
}

func (c ColumnValue) UseDefault() bool {
	return c.sqlDefault
}

func (c ColumnValue) Value() interface{} {
	return c.value
}

type ExtractedColumnValues struct {
	columns []string

	columnValues map[string]ColumnValue
}

func (e ExtractedColumnValues) UpdateSyntax(paramOffset int) (string, []interface{}, int) {

	stmt := ""
	var params []interface{}
	for idx, col := range e.columns {

		paramOffset += 1

		stmt += QuotedColumn(col) + " = "

		colValue := e.columnValues[col]

		if colValue.UseDefault() {
			stmt += "DEFAULT"
		} else if colValue.UseNULL() {
			stmt += "NULL"
		} else {
			stmt += fmt.Sprintf("$%d", paramOffset)

			params = append(params, colValue.value)
		}

		if idx < len(e.columns)-1 {
			stmt += ", "
		}
	}

	return stmt, params, paramOffset
}

func (e ExtractedColumnValues) InsertColumnSyntax() string {

	stmt := "("
	for idx, col := range e.columns {
		stmt += QuotedColumn(col)
		if idx < len(e.columns)-1 {
			stmt += ", "
		}
	}
	return stmt + ")"
}

func (e ExtractedColumnValues) InsertValueSyntax(paramOffset int) (string, []interface{}, int) {
	stmt := "("
	var params []interface{}
	for idx, col := range e.columns {

		paramOffset += 1

		colValue := e.columnValues[col]

		if colValue.UseDefault() {
			stmt += "DEFAULT"
		} else if colValue.UseNULL() {
			stmt += "NULL"
		} else {
			stmt += fmt.Sprintf("$%d", paramOffset)

			params = append(params, colValue.value)
		}

		if idx < len(e.columns)-1 {
			stmt += ", "
		}
	}

	return stmt + ")", params, paramOffset
}
