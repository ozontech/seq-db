package processor

import "github.com/ozontech/seq-db/seq"

// HistMap is an optimized array-based map for histogram.
type HistMap struct {
	buckets  []uint64
	start    seq.MID
	interval seq.MID
	base     uint64
}

func NewHistMap(from, to seq.MID, intervalMillis uint64) HistMap {
	interval := seq.MillisToMID(intervalMillis)
	base := uint64(from) / uint64(interval)
	size := uint64(to)/uint64(interval) - base + 1
	return HistMap{
		buckets:  make([]uint64, size),
		start:    from - from%interval,
		interval: interval,
		base:     base,
	}
}

func (h *HistMap) Update(mids []seq.MID) {
	// TODO(cheb0): unroll/vectorize/whatever when we optimize everything else
	for _, mid := range mids {
		bucketIndex := uint64(mid)/uint64(h.interval) - h.base
		h.buckets[bucketIndex]++
	}
}

func (h HistMap) ToMap() map[seq.MID]uint64 {
	if len(h.buckets) == 0 {
		return nil
	}
	res := make(map[seq.MID]uint64, len(h.buckets))
	bucket := h.start
	for _, cnt := range h.buckets {
		if cnt > 0 {
			res[bucket] = cnt
		}
		bucket += h.interval
	}
	return res
}
