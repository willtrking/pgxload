package pgxload

import (
	"database/sql/driver"
	"reflect"
)

type IsZeroer interface {
	IsZero() bool
}


func IsZero(val reflect.Value) (bool, error) {

	if val.IsZero() {
		return true, nil
	}

	iface := val.Interface()

	switch iface.(type) {
	case driver.Valuer:
		conv := iface.(driver.Valuer)
		v, err := conv.Value()

		return v == nil, err
	case IsZeroer:
		conv := iface.(IsZeroer)

		if conv.IsZero() {
			return true, nil
		}
	}

	return false, nil
}

func ExtractInputData(val reflect.Value) (interface{}, error) {
	if val.IsZero() {
		return nil, nil
	}


	iface := val.Interface()

	switch iface.(type) {
	case driver.Valuer:
		conv := iface.(driver.Valuer)
		v, err := conv.Value()

		return v, err
	case IsZeroer:
		conv := iface.(IsZeroer)

		if conv.IsZero() {
			return nil, nil
		}
	}

	return iface, nil

}

