package pgxload

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCamelToSnakeCase(t *testing.T) {

	idConvert := CamelToSnakeCase("ID")
	allCapsConvert := CamelToSnakeCase("HELLOTHERE")
	oneLetter := CamelToSnakeCase("I")
	oneWord := CamelToSnakeCase("Hello")
	mutliWord := CamelToSnakeCase("HelloThereWorld")

	assert.Equal(t, "id", idConvert)
	assert.Equal(t, "hellothere", allCapsConvert)
	assert.Equal(t, "i", oneLetter)
	assert.Equal(t, "hello", oneWord)
	assert.Equal(t, "hello_there_world", mutliWord)

}
