package token

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// [from, to)
func makeBlock(from, to int) *TableEntry {
	return &TableEntry{StartTID: uint32(from), ValCount: uint32(to - from)}
}

func makeBlocks(borders []int) []*TableEntry {
	var res []*TableEntry
	for i := 1; i < len(borders); i++ {
		res = append(res, makeBlock(borders[i-1], borders[i]))
	}
	return res
}

func checkIndexInBlock(t *testing.T, index int, block *TableEntry) {
	assert.GreaterOrEqual(t, index, int(block.StartTID), "index %d is not in [%d, %d) block", index, block.StartTID, block.StartTID+block.ValCount)
	assert.Less(t, index, int(block.StartTID+block.ValCount), "index %d is not in [%d, %d) block", index, block.StartTID, block.StartTID+block.ValCount)
}

func TestBlockFetcher(t *testing.T) {
	blocks := makeBlocks([]int{0, 1, 2, 3, 5, 9, 100, 101, 103, 105, 110})
	tp := NewProvider(nil, blocks)
	for tid := 1; tid < 110; tid++ {
		te := tp.findEntry(uint32(tid))
		checkIndexInBlock(t, int(tid), te)
	}
}
