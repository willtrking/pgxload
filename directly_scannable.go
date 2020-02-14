package pgxload

import (
	"database/sql"

	"github.com/jackc/pgtype"
)

func isDirectlyScannable(i interface{}) bool {

	if i == nil {
		return true
	}

	switch i.(type) {
	case sql.Scanner, pgtype.BinaryDecoder, pgtype.TextDecoder:
		return true
	case
		string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, complex64, complex128:
		return true
	case
		*string, *bool,
		*int, *int8, *int16, *int32, *int64,
		*uint, *uint8, *uint16, *uint32, *uint64,
		*float32, *float64, *complex64, *complex128:
		return true
	case
		[]string, []bool,
		[]int, []int8, []int16, []int32, []int64,
		[]uint, []uint8, []uint16, []uint32, []uint64,
		[]float32, []float64, []complex64, []complex128:
		return true
	case
		*[]string, *[]bool,
		*[]int, *[]int8, *[]int16, *[]int32, *[]int64,
		*[]uint, *[]uint8, *[]uint16, *[]uint32, *[]uint64,
		*[]float32, *[]float64, *[]complex64, *[]complex128:
		return true
	default:
		return false
	}
}
