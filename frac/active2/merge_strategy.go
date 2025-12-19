package active2

import (
	"math"

	"github.com/ozontech/seq-db/logger"
	"go.uber.org/zap"
)

// Algorithm for selecting indexes for merging (merge):
//
// General concept:
// Indexes are grouped into "tiers" - levels based on their size.
// Merging is performed for indexes from adjacent tiers to minimize
// the size of the resulting index and avoid frequent rebuilds.

// pickMergeCandidates selects groups of indexes for merging based on their tier.
// items - slice of indexes to analyze.
// minMerge - minimum number of indexes that can be merged.
// Returns a slice of index slices - groups for merging.
func pickMergeCandidates(items []memIndexExt, minMerge int) [][]memIndexExt {
	if len(items) < minMerge {
		return nil
	}

	remains := len(items)

	dist := groupByTier(items)

	// win - size of the "sliding window" in number of tiers.
	// bucketSizePercent/tierSizeDeltaPercent determines how many tiers
	// to consider as one group when searching for merge candidates.
	win := int(math.Round(float64(bucketSizePercent) / tierSizeDeltaPercent))

	var batches [][]memIndexExt

	for remains > 1 {
		// forceMerge - flag for forced merging, activated when there are too many indexes.
		forceMerge := remains >= forceMergeThreshold

		// Find the most populated range of tiers.
		// batchSize - number of indexes in the found range.
		// first, last - boundaries of the tier range.
		batchSize, first, last := findBestRange(dist, minMerge, win, forceMerge)

		if batchSize == 0 {
			break
		}

		remains -= batchSize
		buf := make([]memIndexExt, 0, batchSize)
		batches = append(batches, takeFromTiers(buf, first, last, dist))
	}
	return batches
}

// groupByTier builds a distribution of indexes by their tiers.
// items - input indexes to distribute.
// Returns a slice of slices, where the outer slice index is the tier number,
// and the value is all indexes of that tier.
func groupByTier(items []memIndexExt) [][]memIndexExt {
	maxTier := 0
	dist := make([][]memIndexExt, maxTierCount)
	for _, index := range items {
		dist[index.tier] = append(dist[index.tier], index)
		if index.tier > maxTier {
			maxTier = index.tier
		}
	}
	return dist[:maxTier+1]
}

// takeFromTiers extracts indexes from the specified range of tiers.
// buf - buffer for collecting indexes (pre-allocated with the required capacity).
// first, last - boundaries of the tier range (inclusive).
// dist - distribution of indexes by tiers.
// Returns a slice of indexes from the specified range.
func takeFromTiers(buf []memIndexExt, first, last int, dist [][]memIndexExt) []memIndexExt {
	for tier := first; tier <= last; tier++ {
		buf = append(buf, dist[tier]...)
		dist[tier] = nil // Clear the distribution cell so these indexes don't participate in subsequent iterations.
	}
	return buf
}

// findBestRange searches for the most populated range of tiers.
// dist - distribution of indexes by tiers.
// minMerge - minimum number of indexes required for merging.
// win - window size (number of tiers in the range).
// forceMerge - flag for forced search (expands the window if unsuccessful).
// Returns: number of indexes, first tier, last tier.
func findBestRange(dist [][]memIndexExt, minMerge, win int, forceMerge bool) (int, int, int) {
	var bestEnd, bestSum int
	for {
		if bestEnd, bestSum = locateBestWindow(dist, win); bestSum == 0 { // Find the window with the maximum sum of indexes.
			return 0, 0, 0
		}

		if bestSum >= minMerge {
			first := max(0, bestEnd-win)
			last := bestEnd
			return bestSum, first, last
		}

		if !forceMerge {
			return 0, 0, 0
		}

		logger.Warn("insufficient indexes for merge, expanding window",
			zap.Int("win_before", win),
			zap.Int("win_after", win*2),
			zap.Int("found", bestSum),
			zap.Int("required", minMerge),
		)
		win *= 2
	}
}

// locateBestWindow finds the window (range of tiers) with the maximum number of indexes.
// dist - distribution of indexes by tiers.
// winSize - window size (number of tiers).
// Returns: the tier where the window with the maximum sum ends,
// and the maximum sum itself.
func locateBestWindow(dist [][]memIndexExt, winSize int) (int, int) {
	maxCount := 0
	bestEnd := 0

	win := winSum{buf: make([]int, winSize)}

	for tier, items := range dist {
		win.Add(len(items))
		if win.Total() >= maxCount {
			bestEnd = tier
			maxCount = win.sum
		}
	}
	return bestEnd, maxCount
}

// winSum - structure for implementing a sliding window sum calculation.
// Used for efficiently calculating the sum within a fixed-size window.
type winSum struct {
	buf []int // buffer to store values in the window.
	sum int   // current sum of values in the window.
	pos int   // current position in the ring buffer.
}

// Add adds a new value to the sliding window.
// v - new value to add.
// The method updates the sum: removes the oldest value and adds the new one.
func (w *winSum) Add(v int) {
	w.sum += v - w.buf[w.pos]
	w.buf[w.pos] = v
	w.pos++
	if w.pos == len(w.buf) {
		w.pos = 0
	}
}

func (w *winSum) Total() int {
	return w.sum
}
