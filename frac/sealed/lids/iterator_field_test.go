package lids

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/config"
)

func TestFieldIterator_DeltaEncodedBlock_CopyEntireBlock(t *testing.T) {
	source := &UnpackedBlock{
		LIDs:    []uint32{6, 3, 4, 5, 5},
		Offsets: []uint32{0, 1, 4, 5},
	}
	table := NewTable(config.CurrentFracVersion, 7, []uint32{0}, []uint32{2}, nil, nil, nil)
	iterator := NewFieldIterator(table, newFieldIteratorLoader(7, packBlock(t, source)), 0, 2, nil)

	lids, offsets, isFirst := iterator.NextBatch(nil, nil)
	assert.Equal(t, []uint32{6, 3, 4, 5, 5}, lids)
	assert.Equal(t, []uint32{0, 1, 4, 5}, offsets)
	assert.True(t, isFirst)
}

func TestFieldIterator_HybridBlock_CopyEntireBlock(t *testing.T) {
	// 3 or more LIDs - bitmaps are used
	source := &UnpackedBlock{
		LIDs:    []uint32{6, 3, 4, 3, 7},
		Offsets: []uint32{0, 1, 3, 5},
	}
	table := NewTable(config.CurrentFracVersion, 7, []uint32{0}, []uint32{2}, nil, nil, nil)
	iterator := NewFieldIterator(table, newFieldIteratorLoader(7, packBlock(t, source)), 0, 2, nil)

	lids, offsets, isFirst := iterator.NextBatch(nil, nil)
	assert.Equal(t, []uint32{6, 3, 4, 3, 7}, lids)
	assert.Equal(t, []uint32{0, 1, 3, 5}, offsets)
	assert.True(t, isFirst)
}

func TestFieldIterator_DeltaEncodedBlock_CopyFromMiddle(t *testing.T) {
	source := &UnpackedBlock{
		LIDs:    []uint32{6, 3, 4, 3, 7, 1, 2},
		Offsets: []uint32{0, 1, 3, 5, 6, 7},
	}
	table := NewTable(config.CurrentFracVersion, 7, []uint32{0}, []uint32{4}, nil, nil, nil)
	iterator := NewFieldIterator(table, newFieldIteratorLoader(7, packBlock(t, source)), 1, 3, nil)

	lids, offsets, isFirst := iterator.NextBatch(nil, nil)
	assert.Equal(t, []uint32{3, 4, 3, 7, 1}, lids)
	assert.Equal(t, []uint32{0, 2, 4, 5}, offsets)
	assert.True(t, isFirst)
}

func TestFieldIterator_HybridBlock_CopyFromMiddle(t *testing.T) {
	// 3 or more LIDs - bitmaps are used
	source := &UnpackedBlock{
		LIDs:    []uint32{6, 3, 4, 10, 3, 7, 1, 2},
		Offsets: []uint32{0, 1, 4, 6, 7, 8},
	}
	table := NewTable(config.CurrentFracVersion, 7, []uint32{0}, []uint32{4}, nil, nil, nil)
	iterator := NewFieldIterator(table, newFieldIteratorLoader(7, packBlock(t, source)), 1, 3, nil)

	lids, offsets, isFirst := iterator.NextBatch(nil, nil)
	assert.Equal(t, []uint32{3, 4, 10, 3, 7, 1}, lids)
	assert.Equal(t, []uint32{0, 3, 5, 6}, offsets)
	assert.True(t, isFirst)
}

func TestFieldIterator_DeltaBlock_EmptyLIDLists(t *testing.T) {
	source := &UnpackedBlock{
		LIDs:    []uint32{10, 11, 20},
		Offsets: []uint32{0, 2, 2, 3, 3},
	}
	table := NewTable(config.CurrentFracVersion, 7, []uint32{0}, []uint32{3}, nil, nil, nil)
	iterator := NewFieldIterator(table, newFieldIteratorLoader(7, packBlock(t, source)), 1, 3, nil)

	lids, offsets, isFirst := iterator.NextBatch(nil, nil)
	assert.Equal(t, []uint32{20}, lids)
	assert.Equal(t, []uint32{0, 0, 1, 1}, offsets)
	assert.True(t, isFirst)
}

func TestFieldIterator_HybridBlock_EmptyLIDLists(t *testing.T) {
	// 3 or more LIDs - bitmaps are used
	source := &UnpackedBlock{
		LIDs:    []uint32{10, 11, 20, 13},
		Offsets: []uint32{0, 3, 3, 4, 4},
	}
	table := NewTable(config.CurrentFracVersion, 7, []uint32{0}, []uint32{3}, nil, nil, nil)
	iterator := NewFieldIterator(table, newFieldIteratorLoader(7, packBlock(t, source)), 1, 3, nil)

	lids, offsets, isFirst := iterator.NextBatch(nil, nil)
	assert.Equal(t, []uint32{13}, lids)
	assert.Equal(t, []uint32{0, 0, 1, 1}, offsets)
	assert.True(t, isFirst)
}

func TestFieldIterator_DeltaBlock_LIDListSpansTwoBlocks(t *testing.T) {
	b1 := &UnpackedBlock{
		LIDs:    []uint32{1, 3},
		Offsets: []uint32{0, 1, 1, 2},
	}
	b2 := &UnpackedBlock{
		LIDs:    []uint32{5, 7},
		Offsets: []uint32{0, 1, 1, 2},
	}
	table := NewTable(
		config.CurrentFracVersion,
		9,
		[]uint32{0, 2},
		[]uint32{2, 4},
		nil,
		nil,
		nil,
	)
	counter := &testCounter{}
	iterator := NewFieldIterator(
		table,
		newFieldIteratorLoader(9, packBlock(t, b1), packBlock(t, b2)),
		0,
		4,
		counter,
	)

	lids, offsets, isFirst := iterator.NextBatch(nil, nil)
	assert.Equal(t, []uint32{1, 3}, lids)
	assert.Equal(t, []uint32{0, 1, 1, 2}, offsets)
	assert.True(t, isFirst)

	lids, offsets, isFirst = iterator.NextBatch(lids, offsets)
	assert.Equal(t, []uint32{5, 7}, lids)
	assert.Equal(t, []uint32{0, 1, 1, 2}, offsets)
	assert.False(t, isFirst)
	assert.Equal(t, 4, counter.total)

	rangeIterator := NewFieldIterator(
		table,
		newFieldIteratorLoader(9, packBlock(t, b1), packBlock(t, b2)),
		3,
		4,
		nil,
	)
	lids, offsets, isFirst = rangeIterator.NextBatch(nil, nil)
	assert.Equal(t, []uint32{7}, lids)
	assert.Equal(t, []uint32{0, 0, 1}, offsets)
	assert.True(t, isFirst)
}

func TestFieldIterator_MiddleBlocks(t *testing.T) {
	b0 := &UnpackedBlock{
		LIDs:    []uint32{0, 1},
		Offsets: []uint32{0, 1, 2},
	}
	b1 := &UnpackedBlock{
		LIDs:    []uint32{2, 3, 4, 5},
		Offsets: []uint32{0, 2, 4},
	}
	b2 := &UnpackedBlock{
		LIDs:    []uint32{6, 7},
		Offsets: []uint32{0, 1, 2},
	}
	b3 := &UnpackedBlock{
		LIDs:    []uint32{8, 9},
		Offsets: []uint32{0, 1, 2},
	}
	b4 := &UnpackedBlock{
		LIDs:    []uint32{10, 11},
		Offsets: []uint32{0, 1, 2},
	}
	table := NewTable(
		config.CurrentFracVersion,
		11,
		[]uint32{0, 2, 4, 6, 8},
		[]uint32{1, 3, 5, 7, 9},
		nil,
		nil,
		nil,
	)
	iterator := NewFieldIterator(
		table,
		newFieldIteratorLoader(11, packBlock(t, b0), packBlock(t, b1), packBlock(t, b2), packBlock(t, b3), packBlock(t, b4)),
		2,
		7,
		nil,
	)

	lids, offsets, isFirst := iterator.NextBatch(nil, nil)
	assert.Equal(t, []uint32{2, 3, 4, 5}, lids)
	assert.Equal(t, []uint32{0, 2, 4}, offsets)
	assert.True(t, isFirst)

	lids, offsets, isFirst = iterator.NextBatch(lids, offsets)
	assert.Equal(t, []uint32{6, 7}, lids)
	assert.Equal(t, []uint32{0, 1, 2}, offsets)
	assert.True(t, isFirst)

	lids, offsets, isFirst = iterator.NextBatch(lids, offsets)
	assert.Equal(t, []uint32{8, 9}, lids)
	assert.Equal(t, []uint32{0, 1, 2}, offsets)
	assert.True(t, isFirst)

	lids, offsets, isFirst = iterator.NextBatch(lids, offsets)
	assert.Empty(t, lids)
}

type stubBlockCache map[uint32]*Block

func (c stubBlockCache) Get(blockIndex uint32, _ cache.Loader[*Block]) (*Block, error) {
	block, ok := c[blockIndex]
	if !ok {
		return nil, fmt.Errorf("unexpected LID block index: %d", blockIndex)
	}
	return block, nil
}

func newFieldIteratorLoader(startBlockIndex uint32, blocks ...*Block) *Loader {
	cache := stubBlockCache{}
	for i, block := range blocks {
		cache[startBlockIndex+uint32(i)] = block
	}
	return &Loader{cache: cache, fracVer: config.CurrentFracVersion}
}

type testCounter struct {
	total int
}

func (c *testCounter) AddLIDsCount(n int) {
	c.total += n
}

func packBlock(t *testing.T, block *UnpackedBlock) *Block {
	t.Helper()

	packer := NewBlockPacker()
	packer.LidsBitmapThreshold = 3
	packed := packer.Pack(block, nil)

	var unpacked Block
	require.NoError(t, unpacked.Unpack(packed, config.CurrentFracVersion, &UnpackBuffer{}))
	return &unpacked
}
