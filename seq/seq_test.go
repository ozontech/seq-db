package seq

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLegacyMIDFromString(t *testing.T) {
	id, err := FromString("abaf05877b010000-2402dc02d60615cc")

	assert.NoError(t, err)
	// converted legacy (milliseconds MID) to nanoseconds
	assert.Equal(t, MID(1630057901995000000), id.MID)
}

func TestFromString(t *testing.T) {
	id, err := FromString("abaf05877b010000_2402dc02d60615cc")

	assert.NoError(t, err)
	// no convertion, used as micros
	assert.Equal(t, MID(1630057901995), id.MID)
}

func TestMillisToMID(t *testing.T) {
	assert.Equal(t, MID(1761812502000000000), MillisToMID(1761812502000))

	// we can scale this value
	assert.Equal(t, MID(math.MaxUint64/3000000*1000000), MillisToMID(math.MaxUint64/3000000))

	// greatest milliseconds (year 2500) we can scale to nanoseconds
	assert.Equal(t, MID(18446744073709000000), MillisToMID(math.MaxUint64/1000000))

	// we can't scale millis this high to nanoseconds (overflow), so we expect that a user just wants an "infinite future"
	assert.Equal(t, MID(math.MaxUint64), MillisToMID(math.MaxUint64))
	assert.Equal(t, MID(math.MaxUint64/1000), MillisToMID(math.MaxUint64/1000))

}

func TestTimeToMIDConversionOverflow(t *testing.T) {
	timestamp := time.Now()
	assert.EqualExportedValues(t, timestamp, MID(timestamp.UnixNano()).Time())

	// check that we do not overflow on huge values
	maxMID := MID(math.MaxUint64)
	assert.Equal(t, 2554, maxMID.Time().Year())
	assert.Equal(t, 2554, MIDToTime(maxMID).Year())
}

func TestMIDToCeilingMillis(t *testing.T) {
	assert.Equal(t, uint64(14), MIDToCeilingMillis(MID(14000000)))
	assert.Equal(t, uint64(15), MIDToCeilingMillis(MID(14000001)))
	assert.Equal(t, uint64(15), MIDToCeilingMillis(MID(14999999)))
}

func TestString(t *testing.T) {
	assert.Equal(t, "2025-10-30 12:21:42", MID(1761812502000000000).String())
	assert.Equal(t, "2025-10-30 12:21:42.432", MID(1761812502432000000).String())
	assert.Equal(t, "2025-10-30 12:21:42.432", MID(1761812502432000773).String())
}
