package seq

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func getTime(s string) time.Time {
	r, e := time.Parse("2006-01-02 15:04:05", s)
	if e != nil {
		panic(e)
	}
	return r
}

func getMID(s string) MID {
	return MID(getTime(s).UnixMilli())
}

func TestMIDsDistribution(t *testing.T) {
	from := getTime("2023-04-09 14:00:00")
	to := getTime("2023-04-09 15:00:00")
	d := NewMIDsDistribution(from, to, time.Minute)

	d.Add(getMID("2023-04-09 14:00:00"))
	d.Add(getMID("2023-04-09 14:00:01"))
	d.Add(getMID("2023-04-09 14:01:30"))
	d.Add(getMID("2023-04-09 14:02:01"))
	d.Add(getMID("2023-04-09 14:58:30"))
	d.Add(getMID("2023-04-09 14:59:01"))

	// check out of bounds (empty)
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 15:00:01"), getMID("2023-04-09 15:00:10")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 13:00:50"), getMID("2023-04-09 13:00:55")))

	// add left out of bounds
	d.Add(getMID("2023-04-09 12:00:00"))

	// add right out of bounds
	d.Add(getMID("2023-04-09 15:35:00"))

	dist := d.GetDist()
	assert.Equal(t, getTime("2023-04-09 13:59:00"), dist[0])
	assert.Equal(t, getTime("2023-04-09 14:00:00"), dist[1])
	assert.Equal(t, getTime("2023-04-09 14:01:00"), dist[2])
	assert.Equal(t, getTime("2023-04-09 14:02:00"), dist[3])
	assert.Equal(t, getTime("2023-04-09 14:58:00"), dist[4])
	assert.Equal(t, getTime("2023-04-09 14:59:00"), dist[5])
	assert.Equal(t, getTime("2023-04-09 15:01:00"), dist[6])

	// check ranges
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:00:10"), getMID("2023-04-09 14:00:20")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:00:10"), getMID("2023-04-09 14:02:20")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:00:10"), getMID("2023-04-09 14:20:20")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:20:10"), getMID("2023-04-09 14:30:00")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:30:10"), getMID("2023-04-09 14:58:00")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:30:10"), getMID("2023-04-09 14:59:00")))

	// check out of bounds (not empty)
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 15:00:01"), getMID("2023-04-09 15:00:10")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 13:00:50"), getMID("2023-04-09 13:00:55")))
}

func TestMIDsDistributionMarshal(t *testing.T) {
	from := getTime("2023-04-09 14:00:00")
	to := getTime("2023-04-09 15:00:00")
	d1 := NewMIDsDistribution(from, to, time.Minute)

	d1.Add(getMID("2023-04-09 14:00:00"))
	d1.Add(getMID("2023-04-09 14:00:01"))
	d1.Add(getMID("2023-04-09 14:01:30"))
	d1.Add(getMID("2023-04-09 14:02:01"))
	d1.Add(getMID("2023-04-09 14:58:30"))
	d1.Add(getMID("2023-04-09 14:59:01"))

	data, err := json.Marshal(d1)
	assert.NoError(t, err)

	d2 := &MIDsDistribution{}
	err = json.Unmarshal(data, d2)
	assert.NoError(t, err)

	assert.Equal(t, d1, d2)
}

func TestMIDsDistributionAges(t *testing.T) {
	from := getTime("2023-04-09 14:00:00")
	to := getTime("2023-04-09 15:00:00")

	d := NewMIDsDistribution(from, to, time.Minute)
	d.Add(getMID("2023-04-09 13:59:30"))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 12:59:59"), getMID("2023-04-09 12:59:59")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 13:59:59"), getMID("2023-04-09 13:59:59")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 13:59:59"), getMID("2023-04-09 14:00:00")))

	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:00:00"), getMID("2023-04-09 14:00:00")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:00:00"), getMID("2023-04-09 14:00:01")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:00:01"), getMID("2023-04-09 14:00:02")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:00:01"), getMID("2023-04-09 14:01:02")))

	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:01:00"), getMID("2023-04-09 14:01:00")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:01:00"), getMID("2023-04-09 14:01:01")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:01:01"), getMID("2023-04-09 14:01:02")))

	d = NewMIDsDistribution(from, to, time.Minute)
	d.Add(getMID("2023-04-09 14:00:00"))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 12:59:59"), getMID("2023-04-09 12:59:59")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 13:59:59"), getMID("2023-04-09 13:59:59")))

	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 13:59:59"), getMID("2023-04-09 14:00:00")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:00:00"), getMID("2023-04-09 14:00:00")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:00:00"), getMID("2023-04-09 14:00:01")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:00:01"), getMID("2023-04-09 14:00:02")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:00:01"), getMID("2023-04-09 14:01:02")))

	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:01:00"), getMID("2023-04-09 14:01:00")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:01:00"), getMID("2023-04-09 14:01:01")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:01:01"), getMID("2023-04-09 14:01:02")))

	d = NewMIDsDistribution(from, to, time.Minute)
	d.Add(getMID("2023-04-09 14:01:01"))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 12:59:59"), getMID("2023-04-09 12:59:59")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 13:59:59"), getMID("2023-04-09 13:59:59")))

	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 13:59:59"), getMID("2023-04-09 14:00:00")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:00:00"), getMID("2023-04-09 14:00:00")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:00:00"), getMID("2023-04-09 14:00:01")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:00:01"), getMID("2023-04-09 14:00:02")))

	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:00:01"), getMID("2023-04-09 14:01:02")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:01:00"), getMID("2023-04-09 14:01:00")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:01:00"), getMID("2023-04-09 14:01:01")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:01:01"), getMID("2023-04-09 14:01:02")))

	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:02:00"), getMID("2023-04-09 14:02:00")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:02:00"), getMID("2023-04-09 14:02:01")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:02:01"), getMID("2023-04-09 14:02:30")))

	d = NewMIDsDistribution(from, to, time.Minute)
	d.Add(getMID("2023-04-09 14:58:00"))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:57:00"), getMID("2023-04-09 14:57:00")))

	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:57:01"), getMID("2023-04-09 14:58:00")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:57:01"), getMID("2023-04-09 14:58:02")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:58:00"), getMID("2023-04-09 14:58:00")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:58:00"), getMID("2023-04-09 14:58:01")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:58:01"), getMID("2023-04-09 14:58:02")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:57:00"), getMID("2023-04-09 14:58:00")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 14:57:00"), getMID("2023-04-09 14:59:00")))

	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:59:00"), getMID("2023-04-09 14:59:00")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:59:59"), getMID("2023-04-09 15:00:00")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:59:59"), getMID("2023-04-09 15:10:00")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 15:00:00"), getMID("2023-04-09 15:00:01")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 16:00:01"), getMID("2023-04-09 16:00:02")))

	d = NewMIDsDistribution(from, to, time.Minute)
	d.Add(getMID("2023-04-09 15:00:01"))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:57:00"), getMID("2023-04-09 14:57:00")))

	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:57:01"), getMID("2023-04-09 14:58:00")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:57:01"), getMID("2023-04-09 14:58:02")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:58:00"), getMID("2023-04-09 14:58:00")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:58:00"), getMID("2023-04-09 14:58:01")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:58:01"), getMID("2023-04-09 14:58:02")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:57:00"), getMID("2023-04-09 14:58:00")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:57:00"), getMID("2023-04-09 14:59:00")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:59:00"), getMID("2023-04-09 14:59:00")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:59:00"), getMID("2023-04-09 14:59:59")))
	assert.Equal(t, false, d.IsIntersecting(getMID("2023-04-09 14:59:59"), getMID("2023-04-09 15:00:00")))

	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 15:00:00"), getMID("2023-04-09 15:00:02")))
	assert.Equal(t, true, d.IsIntersecting(getMID("2023-04-09 16:00:01"), getMID("2023-04-09 16:00:02")))
}
