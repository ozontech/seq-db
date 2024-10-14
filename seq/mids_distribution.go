package seq

import (
	"encoding/json"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
)

type MIDsDistribution struct {
	from, to time.Time
	bucket   time.Duration
	bitmask  util.Bitmask
}

func NewMIDsDistribution(from, to time.Time, bucket time.Duration) *MIDsDistribution {
	d := &MIDsDistribution{
		from:   from.UTC(),
		to:     to.UTC(),
		bucket: bucket,
	}

	d.bitmask = util.NewBitmask(d.size())
	return d
}

func (d *MIDsDistribution) size() int {
	// [ from : from + bucket )
	// ...
	// [ from + i*bucket : from + (i+1)*bucket )
	// ...
	// [ from + (n-1)*bucket : from + n*bucket ), where "to" in this interval
	n := d.to.Sub(d.from)/d.bucket + 1
	n += 2 // reserve left (-inf:from) and right (to:+inf) buckets for out of bounds case
	return int(n)
}

func (d *MIDsDistribution) midToIndex(mid MID) int {
	t := mid.Time()

	if t.Before(d.from) {
		return 0
	}
	if t.After(d.to) {
		return d.bitmask.GetSize() - 1
	}

	return int(t.Sub(d.from)/d.bucket) + 1
}

func (d *MIDsDistribution) Add(mid MID) {
	i := d.midToIndex(mid)
	d.bitmask.Set(i, true)
}

func (d *MIDsDistribution) isUndefined() bool {
	return d.bucket == 0
}

func (d *MIDsDistribution) IsIntersecting(from, to MID) bool {
	if d.isUndefined() {
		return true
	}
	return d.bitmask.HasBitsIn(d.midToIndex(from), d.midToIndex(to))
}

func (d *MIDsDistribution) GetDist() []time.Time {
	dist := make([]time.Time, 0)
	for i := 0; i < d.bitmask.GetSize(); i++ {
		if d.bitmask.Get(i) {
			dist = append(dist, d.from.Add(d.bucket*time.Duration(i-1)))
		}
	}
	return dist
}

type midsDistributionJSON struct {
	From    uint64 `json:"from"`
	To      uint64 `json:"to"`
	Bucket  uint64 `json:"bucket"`
	Bitmask []byte `json:"bitmask"`
}

func (d *MIDsDistribution) MarshalJSON() ([]byte, error) {
	if d.isUndefined() {
		return []byte("null"), nil
	}
	distJSON := midsDistributionJSON{
		From:    uint64(d.from.UnixMilli()),
		To:      uint64(d.to.UnixMilli()),
		Bucket:  uint64(d.bucket.Seconds()),
		Bitmask: d.bitmask.GetBitmaskBinary(),
	}
	return json.Marshal(distJSON)
}

func (d *MIDsDistribution) UnmarshalJSON(data []byte) error {
	distJSON := midsDistributionJSON{}
	if err := json.Unmarshal(data, &distJSON); err != nil {
		logger.Error("distribution unmarshaling error", zap.Error(err))
		return nil
	}

	d.from = time.UnixMilli(int64(distJSON.From)).UTC()
	d.to = time.UnixMilli(int64(distJSON.To)).UTC()
	d.bucket = time.Second * time.Duration(distJSON.Bucket)
	if !d.isUndefined() {
		d.bitmask = util.LoadBitmask(d.size(), distJSON.Bitmask)
	}
	return nil
}
