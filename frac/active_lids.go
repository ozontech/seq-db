package frac

import (
	"cmp"
	"math"
	"slices"
	"sync"
)

type TokenLIDs struct {
	sortedMu sync.Mutex // global merge mutex, making the merge process strictly sequential
	sorted   []uint32   // slice of actual sorted and merged LIDs of token

	queueMu sync.Mutex
	queue   []uint32
}

func (tl *TokenLIDs) GetLIDs(mids, rids *UInt64s) []uint32 {
	tl.sortedMu.Lock()
	defer tl.sortedMu.Unlock()

	lids := tl.getQueuedLIDs()
	if len(lids) != 0 {

		mids := mids.GetVals()
		rids := rids.GetVals()

		slices.SortFunc(lids, func(i, j uint32) int {
			if mids[i] == mids[j] {
				if rids[i] == rids[j] {
					return -cmp.Compare(i, j)
				}
				return -cmp.Compare(rids[i], rids[j])
			}
			return -cmp.Compare(mids[i], mids[j])
		})

		tl.sorted = mergeSorted(tl.sorted, lids, mids, rids)
	}

	return tl.sorted
}

// SortedLIDs returns pre-merged LIDs.
// Only safe to call after the fraction is frozen and lids queue was drained.
func (tl *TokenLIDs) SortedLIDsUnsafe() []uint32 {
	return tl.sorted
}

type SeqIDCmp struct {
	mid []uint64
	rid []uint64
}

func (c *SeqIDCmp) compare(a, b uint32) int {
	midA, midB := c.mid[a], c.mid[b]

	if midA > midB {
		return 1
	}

	if midA < midB {
		return -1
	}

	ridA, ridB := c.rid[a], c.rid[b]

	if ridA > ridB {
		return 1
	}

	if ridA < ridB {
		return -1
	}

	if a < b {
		return -1
	}

	if a > b {
		return 1
	}

	return 0
}

func mergeSorted(right, left []uint32, mids, rids []uint64) []uint32 {
	l := 0
	r := 0
	val := uint32(0)
	prev := uint32(math.MaxUint32)

	c := SeqIDCmp{
		mid: mids,
		rid: rids,
	}

	result := make([]uint32, 0, len(right)+len(left))

	for l != len(left) && r != len(right) {
		ri, li := right[r], left[l]

		switch c.compare(ri, li) {
		case 0:
			val = ri
			r++
			l++
		case 1:
			val = ri
			r++
		case -1:
			val = li
			l++
		}

		if prev == val {
			continue
		}

		result = append(result, val)
		prev = val
	}
	if l == len(left) {
		result = append(result, right[r:]...)
	}
	if r == len(right) {
		for _, val = range left[l:] {
			if val == prev {
				continue
			}
			result = append(result, val)
			prev = val
		}
	}
	return result
}

func (tl *TokenLIDs) PutLIDsInQueue(lids []uint32) int {
	tl.queueMu.Lock()
	defer tl.queueMu.Unlock()

	tl.queue = append(tl.queue, lids...)
	return len(tl.queue)
}

func (tl *TokenLIDs) getQueuedLIDs() []uint32 {
	tl.queueMu.Lock()
	defer tl.queueMu.Unlock()

	if len(tl.queue) == 0 {
		return nil
	}

	lids := tl.queue
	tl.queue = nil
	return lids
}
