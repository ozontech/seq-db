package compaction

import (
	"cmp"
	"slices"

	"github.com/ozontech/seq-db/frac"
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

func (s strategySTCS) Pick(candidates []frac.Fraction) []frac.Fraction {
	if len(candidates) < s.mergeTrigger {
		return nil
	}

	sorted := slices.Clone(candidates)
	slices.SortFunc(sorted, func(a, b frac.Fraction) int {
		return cmp.Compare(a.Info().IndexOnDisk, b.Info().IndexOnDisk)
	})

	buckets := s.group(sorted)
	// We are interested in buckets with the most amount of fractions.
	// Usually, these are the lowest tiers where all freshly sealed fractions end up.
	slices.SortFunc(buckets, func(x, y []frac.Fraction) int {
		return -cmp.Compare(len(x), len(y))
	})

	for _, bucket := range buckets {
		if len(bucket) < s.mergeTrigger {
			continue
		}

		fracs := bucket[:min(len(bucket), s.mergeFanIn)]
		if picked := s.takeUntilSize(fracs); len(picked) > 0 {
			return picked
		}
	}

	return nil
}

func (s strategySTCS) group(sorted []frac.Fraction) [][]frac.Fraction {
	var (
		sum     uint64
		current []frac.Fraction
		buckets [][]frac.Fraction
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

		buckets = append(buckets, current)
		current = []frac.Fraction{f}
		sum = size
	}

	if len(current) > 0 {
		buckets = append(buckets, current)
	}

	return buckets
}

func (s strategySTCS) takeUntilSize(fracs []frac.Fraction) []frac.Fraction {
	var picked uint64

	for i := range fracs {
		picked += fracs[i].Info().IndexOnDisk
		if picked >= s.mergeFanOutSize {
			return fracs[:i]
		}
	}

	return fracs
}
