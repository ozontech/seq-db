package compaction

import (
	"cmp"
	"slices"
)

type strategySTCS struct {
	// To trigger compaction of bucket there must be
	// at least [mergeTrigger] fractions.
	mergeTrigger int

	// At most this many fractions are compacted from a single bucket
	// per compaction iteration.
	mergeFanIn      int
	mergeFanOutSize uint64

	// Fraction size must be within [bucketLowerbound, bucketUpperbound] * avg(bucket)
	// to be considered part of the bucket.
	bucketLowerbound float64
	bucketUpperbound float64
}

type bucket struct {
	sizeAvg uint64
	fracs   []fraction
}

func (s strategySTCS) Pick(candidates []fraction) bucket {
	if len(candidates) < s.mergeTrigger {
		return bucket{}
	}

	sorted := slices.Clone(candidates)
	slices.SortFunc(sorted, func(a, b fraction) int {
		return cmp.Compare(a.Info().IndexOnDisk, b.Info().IndexOnDisk)
	})

	buckets := s.group(sorted)
	// We are interested in buckets with the most amount of fractions.
	// Usually, these are the lowest tiers where all freshly sealed fractions end up.
	slices.SortFunc(buckets, func(x, y bucket) int {
		return -cmp.Compare(len(x.fracs), len(y.fracs))
	})

	for _, b := range buckets {
		if len(b.fracs) < s.mergeTrigger {
			continue
		}

		b.fracs = b.fracs[:min(len(b.fracs), s.mergeFanIn)]
		if picked := s.takeUntilSize(b); len(picked.fracs) >= s.mergeTrigger {
			return picked
		}
	}

	return bucket{}
}

func (s strategySTCS) group(sorted []fraction) []bucket {
	var (
		sum     uint64
		current []fraction
		buckets []bucket
	)

	for _, f := range sorted {
		size := f.Info().IndexOnDisk

		if len(current) == 0 {
			current = append(current, f)
			sum = size
			continue
		}

		avg := float64(sum) / float64(len(current))
		fsize := float64(size)

		lower := avg * s.bucketLowerbound
		upper := avg * s.bucketUpperbound

		if lower <= fsize && fsize <= upper {
			current = append(current, f)
			sum += size
			continue
		}

		buckets = append(buckets, bucket{uint64(avg), current})
		current = []fraction{f}
		sum = size
	}

	if len(current) > 0 {
		avg := float64(sum) / float64(len(current))
		buckets = append(buckets, bucket{uint64(avg), current})
	}

	return buckets
}

func (s strategySTCS) takeUntilSize(b bucket) bucket {
	var picked uint64

	for i := range b.fracs {
		picked += b.fracs[i].Info().IndexOnDisk
		if picked >= s.mergeFanOutSize {
			return bucket{b.sizeAvg, b.fracs[:i]}
		}
	}

	return b
}
