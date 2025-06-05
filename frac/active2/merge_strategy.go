package active2

import (
	"math"
)

// SelectForMerge selects merge candidates based on their size.
// It groups items into sets within which the sizes of the items do not differ
// by more than a specified limit in percent (e.g. 50%)
func SelectForMerge(items []indexItem, minToMerge int) [][]indexItem {
	if len(items) < minToMerge {
		return nil
	}

	tiersDist := buildTiersDistribution(items)
	findAtAnyCost := len(items) >= mergeForcedThreshold
	winSize := int(math.Round(float64(bucketPercent) / tierDeltaPercent))

	var res [][]indexItem
	for {
		countInRange, firstTier, lastTier := mostPopulatedTiersRange(tiersDist, minToMerge, winSize, findAtAnyCost)
		if countInRange == 0 {
			break
		}
		buf := make([]indexItem, 0, countInRange)
		res = append(res, extractIndexesInRange(items, buf, firstTier, lastTier, tiersDist))
	}
	return res
}

func buildTiersDistribution(items []indexItem) []int {
	lastTier := 0
	tiersDist := make([]int, tierMax)
	for _, index := range items {
		tiersDist[index.tier]++
		if index.tier > lastTier {
			lastTier = index.tier
		}
	}
	return tiersDist[:lastTier]
}

func extractIndexesInRange(items []indexItem, buf []indexItem, firstTier, lastTier int, tiersDist []int) []indexItem {
	for _, index := range items {
		if firstTier <= index.tier && index.tier <= lastTier {
			buf = append(buf, index)
			tiersDist[index.tier]--
		}
	}
	return buf
}

func mostPopulatedTiersRange(tiersDist []int, minToMerge, winSize int, findAtAnyCost bool) (int, int, int) {
	var lastWinTier, maxWinSum int
	for {
		lastWinTier, maxWinSum = findMaxSumWindow(tiersDist, winSize)
		if maxWinSum >= minToMerge { // got it!
			break
		}
		if findAtAnyCost { // expand window size and find again
			// todo добавить логирования!
			winSize *= 2
			continue
		}
		return 0, 0, 0
	}

	firstTier := max(0, lastWinTier-winSize)
	lastTier := lastWinTier

	return maxWinSum, firstTier, lastTier
}

// sliding window sum
type winSum struct {
	buf []int
	sum int
	pos int
}

func (w *winSum) Add(v int) {
	w.sum += v - w.buf[w.pos]
	w.buf[w.pos] = v
	w.pos++
	if w.pos == len(w.buf) {
		w.pos = 0
	}
}

func findMaxSumWindow(tiersDist []int, winSize int) (int, int) {
	maxWinSum := 0
	lastWinTier := 0
	win := winSum{buf: make([]int, winSize)}

	for tier, size := range tiersDist {
		win.Add(size)
		if win.sum >= maxWinSum {
			lastWinTier = tier
			maxWinSum = win.sum
		}
	}
	return lastWinTier, maxWinSum
}
