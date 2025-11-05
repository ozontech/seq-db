package seq

import (
	"math"
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

func TestMillisToMID(t *testing.T) {
	assert.Equal(t, MID(1761812502000000), MillisToMID(1761812502000))

	// we can't convert millis this high to nanos (overflow), so we expect that user just want "infinite future"
	assert.Equal(t, MID(math.MaxUint64), MillisToMID(math.MaxUint64))
	assert.Equal(t, MID(math.MaxUint64/1000), MillisToMID(math.MaxUint64/1000))
}
