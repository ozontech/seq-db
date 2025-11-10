package seq

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFromString(t *testing.T) {
	_, err := FromString("abaf05877b010000-2402dc02d60615cc")
	assert.NoError(t, err)
}

func TestTimeToMIDConversionOverflow(t *testing.T) {
	timestamp := time.Now()
	assert.EqualExportedValues(t, timestamp, MID(timestamp.UnixNano()).Time())

	// check that we do not overflow on huge values
	maxMID := MID(math.MaxUint64)
	assert.Equal(t, 584556019, maxMID.Time().Year())
	assert.Equal(t, 584556019, MIDToTime(maxMID).Year())
}
