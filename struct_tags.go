package pgxload

import (
	"reflect"
	"strings"
)

var structTagKey = "pgxload"
var defaultStructTagOpts = structTagOpts{}

func parseStructTag(tag reflect.StructTag) structTagOpts {

	return parseStructTagOptsStr(tag.Get(structTagKey))
}

func parseStructTagOptsStr(optsStr string) structTagOpts {

	opts := defaultStructTagOpts.copy()
	if optsStr != "" {

		for _, opt := range strings.Split(optsStr, ",") {
			opts = opts.setOpt(opt)
		}

		return opts
	}

	return defaultStructTagOpts
}

type structTagOpts struct {
	Omit        bool
	OmitZero    bool
	DefaultZero bool
	NullZero    bool
}

func (s structTagOpts) copy() structTagOpts {
	return structTagOpts{
		Omit:        s.Omit,
		OmitZero:    s.OmitZero,
		DefaultZero: s.DefaultZero,
		NullZero:    s.NullZero,
	}
}

func (s structTagOpts) setOpt(opt string) structTagOpts {

	switch strings.TrimSpace(opt) {
	case "omit":
		s.Omit = true
		return s
	case "omitZero":
		s.OmitZero = true
		return s
	case "defaultZero":
		s.DefaultZero = true
		return s
	case "nullZero":
		s.NullZero = true
		return s
	}

	return s
}
