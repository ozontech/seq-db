package seq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMIDFromString(t *testing.T) {
	id, err := FromString("abaf05877b010000-2402dc02d60615cc")

	assert.NoError(t, err)
	assert.Equal(t, MID(1630057901995), id.MID)
}

func TestNewMIDFromString(t *testing.T) {
	id, err := FromString("abaf05877b010000_2402dc02d60615cc")

	assert.NoError(t, err)
	// new format, convert micros to millis
	assert.Equal(t, MID(1630057901), id.MID)
}
