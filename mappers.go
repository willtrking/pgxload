package pgxload

import (
	"bytes"
	"unicode"
)

// A smart-ish camel to snake case converter
// If input is all caps, will not convert to snake (e.g. ID will be id, not i_d)
func CamelToSnakeCase(local string) string {

	var remote bytes.Buffer

	allUpper := IsAllUpper(local)

	for idx, f := range local {
		if idx != 0 {
			if !allUpper && unicode.IsUpper(f) {
				remote.WriteRune('_')
			}
		}
		remote.WriteRune(unicode.ToLower(f))
	}

	return remote.String()
}
