package pgxload

import (
	"reflect"
	"testing"

	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
)


func Test_IsAllUpper(t *testing.T) {

	assert.True(t, IsAllUpper("ID"))
	assert.True(t, IsAllUpper("HELLOTHÜ"))
	assert.False(t, IsAllUpper("HELLOTHü"))
	assert.False(t, IsAllUpper("HELLo"))
}

type MockFieldDescriptionProvider struct {
}

func (m MockFieldDescriptionProvider) FieldDescriptions() []pgproto3.FieldDescription {

	return []pgproto3.FieldDescription{
		pgproto3.FieldDescription{
			Name: []byte("col1"),
		},
		pgproto3.FieldDescription{
			Name: []byte("col2"),
		},
	}
}

func Test_SliceElemType(t *testing.T) {

	var sl []MockFieldDescriptionProvider
	var slPtr []*MockFieldDescriptionProvider

	tpe := SliceElemType(reflect.ValueOf(sl))
	assert.Equal(t, "MockFieldDescriptionProvider", tpe.Name())

	tpe = SliceElemType(reflect.ValueOf(slPtr))
	assert.Equal(t, "MockFieldDescriptionProvider", tpe.Name())

	tpe = SliceElemType(reflect.Indirect(reflect.ValueOf(&sl)))
	assert.Equal(t, "MockFieldDescriptionProvider", tpe.Name())

	tpe = SliceElemType(reflect.Indirect(reflect.ValueOf(&slPtr)))
	assert.Equal(t, "MockFieldDescriptionProvider", tpe.Name())

}

func Test_ColumnNames(t *testing.T) {

	names, err := ColumnNames(MockFieldDescriptionProvider{})
	if assert.NoError(t, err, "ColumnNames") {
		assert.Equal(t, 2, len(names))

		assert.Contains(t, names, "col1")
		assert.Contains(t, names, "col2")
	}
}


func Test_MissingColumns(t *testing.T) {

	err := missingColumns([]string{"hello", "ok", "missing"}, [][]int{
		[]int{
			0,
		},
		[]int{
			0,
		},
	})

	if assert.Error(t, err) {

		assert.Equal(t, "missing destination name: missing", err.Error())
	}

	err = missingColumns([]string{"hello", "ok", "missing"}, [][]int{
		[]int{
			0,
		},
		[]int{
			0,
		},
		[]int{},
	})

	if assert.Error(t, err) {
		assert.Equal(t, "missing destination name: missing", err.Error())
	}

	err = missingColumns([]string{"hello", "ok", "missing", "missing2"}, [][]int{
		[]int{
			0,
		},
		[]int{
			0,
		},
	})

	if assert.Error(t, err) {
		assert.Equal(t, "missing destination names: missing, missing2", err.Error())
	}

	err = missingColumns([]string{"hello", "ok", "missing", "missing2"}, [][]int{
		[]int{
			0,
		},
		[]int{
			0,
		},
		[]int{},
		[]int{},
	})

	if assert.Error(t, err) {
		assert.Equal(t, "missing destination names: missing, missing2", err.Error())
	}

	err = missingColumns([]string{"hello", "ok", "missing", "missing2"}, [][]int{
		[]int{
			0,
		},
		[]int{
			0,
		},
		[]int{
			0,
		},
		[]int{
			0,
		},
	})

	assert.NoError(t, err)
}


func Test_isDirectlyScannable(t *testing.T) {

}