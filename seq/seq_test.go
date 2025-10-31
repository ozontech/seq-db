package seq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLegacyMIDFromString(t *testing.T) {
	id, err := FromString("abaf05877b010000-2402dc02d60615cc")

	assert.NoError(t, err)
	// converted legacy (millis) to micros
	assert.Equal(t, MID(1630057901995000), id.MID)
}

func TestFromString(t *testing.T) {
	id, err := FromString("abaf05877b010000_2402dc02d60615cc")

	assert.NoError(t, err)
	// no convertion, used as micros
	assert.Equal(t, MID(1630057901995), id.MID)
}
