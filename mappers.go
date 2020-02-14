package pgxload

import (
	"bytes"
	"unicode"
)

// A smart-ish camel to snake case converter
// Tries to make intelligent decisions about phrases that are all caps in a camel-cased string
// e.g. ID will be id, not i_d, and OrganizationID will be organization_id, not organization_i_d
func CamelToSnakeCase(local string) string {

	var remote bytes.Buffer

	lastLower := false
	for idx, f := range local {

		if unicode.IsUpper(f) {
			if idx != 0 && lastLower {
				remote.WriteRune('_')
			}
			lastLower = false
		} else {
			lastLower = true
		}

		remote.WriteRune(unicode.ToLower(f))
	}

	return remote.String()
}
