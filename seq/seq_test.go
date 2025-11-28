package seq

import (
	"math"
	"testing"
	"time"

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
	// new format, should convert 1630057901995 nanoseconds to millis
	assert.Equal(t, MID(1630057), id.MID)
}

func TestTimeToMIDConversion(t *testing.T) {
	timestampNow := time.Now()
	assert.EqualExportedValues(t, timestampNow, MID(timestampNow.UnixNano()).Time())

	timestamp2 := MID(1763984556395).Time().UTC()
	assert.Equal(t, 2025, timestamp2.Year())
	assert.Equal(t, time.Month(11), timestamp2.Month())
	assert.Equal(t, 24, timestamp2.Day())
	assert.Equal(t, 11, timestamp2.Hour())
	assert.Equal(t, 42, timestamp2.Minute())
	assert.Equal(t, 36, timestamp2.Second())
	assert.Equal(t, 395000000, timestamp2.Nanosecond())

	// check that we do not overflow on huge values
	maxMID := MID(math.MaxUint64)
	assert.Equal(t, 292278994, maxMID.Time().Year())
	assert.Equal(t, 292278994, MIDToTime(maxMID).Year())
}
