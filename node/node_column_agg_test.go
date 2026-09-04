package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnAgg_MatchesOrTree(t *testing.T) {
	sources := [][]uint32{
		{1, 3, 5},
		{2, 4, 6},
	}

	tree := BuildORTreeAgg(MakeStaticNodes(sources))
	cursors := make([]BatchedNode, len(sources))
	for i, src := range sources {
		cursors[i] = NewStaticBatched(src, true)
	}
	column := NewColumnAgg(cursors, 1, 6, true)

	for _, lid := range []uint32{1, 2, 3, 4, 5, 6, 7} {
		treeID, treeSource, treeOK := consumeAt(tree, lid, true)
		colID, colSource, colOK := consumeAt(column, lid, true)
		assert.Equal(t, treeOK, colOK, "lid=%d", lid)
		if treeOK {
			assert.Equal(t, treeID.Unpack(), colID.Unpack(), "lid=%d", lid)
			assert.Equal(t, treeSource, colSource, "lid=%d", lid)
		}
	}
}

func TestColumnAgg_MatchesOrTree_DescLIDOrder(t *testing.T) {
	sources := [][]uint32{
		{1, 3, 5},
		{2, 4, 6},
	}

	tree := BuildORTreeAgg([]Node{
		NewStatic(sources[0], false),
		NewStatic(sources[1], false),
	})
	cursors := make([]BatchedNode, len(sources))
	for i, src := range sources {
		cursors[i] = NewStaticBatched(src, false)
	}
	column := NewColumnAgg(cursors, 1, 6, false)

	// Desc LID order (docs order asc): consume from high LID to low.
	for _, lid := range []uint32{6, 5, 4, 3, 2, 1, 0} {
		treeID, treeSource, treeOK := consumeAt(tree, lid, false)
		colID, colSource, colOK := consumeAt(column, lid, false)
		assert.Equal(t, treeOK, colOK, "lid=%d", lid)
		if treeOK {
			assert.Equal(t, treeID.Unpack(), colID.Unpack(), "lid=%d", lid)
			assert.Equal(t, treeSource, colSource, "lid=%d", lid)
		}
	}
}

func TestColumnAgg_NextSourcedGeq(t *testing.T) {
	sources := [][]uint32{
		{1, 2, 3, 5, 8, 15, 19},
		{2, 3, 4, 6, 8, 14, 20},
	}

	cursors := make([]BatchedNode, len(sources))
	for i, src := range sources {
		cursors[i] = NewStaticBatched(src, true)
	}
	column := NewColumnAgg(cursors, 1, 20, true)

	id, source := column.NextSourcedGeq(NewAscLID(3))
	assert.Equal(t, uint32(3), id.Unpack())
	assert.Equal(t, uint32(1), source)

	id, source = column.NextSourcedGeq(NewAscLID(6))
	assert.Equal(t, uint32(6), id.Unpack())
	assert.Equal(t, uint32(1), source)

	id, source = column.NextSourcedGeq(NewAscLID(17))
	assert.Equal(t, uint32(19), id.Unpack())
	assert.Equal(t, uint32(0), source)
}

func TestColumnAgg_NextSourcedGeq_DescLIDOrder(t *testing.T) {
	// asc=false: docs order asc, raw LIDs decrease
	sources := [][]uint32{
		{1, 2, 3, 5, 8, 15, 19},
		{2, 3, 4, 6, 8, 14, 20},
	}

	cursors := make([]BatchedNode, len(sources))
	for i, src := range sources {
		cursors[i] = NewStaticBatched(src, false)
	}
	column := NewColumnAgg(cursors, 1, 20, false)

	id, source := column.NextSourcedGeq(NewDescLID(17))
	assert.Equal(t, uint32(15), id.Unpack())
	assert.Equal(t, uint32(0), source)

	id, source = column.NextSourcedGeq(NewDescLID(8))
	assert.Equal(t, uint32(8), id.Unpack())
	assert.Equal(t, uint32(1), source)

	id, source = column.NextSourcedGeq(NewDescLID(4))
	assert.Equal(t, uint32(4), id.Unpack())
	assert.Equal(t, uint32(1), source)

	id, source = column.NextSourcedGeq(NewDescLID(1))
	assert.Equal(t, uint32(1), id.Unpack())
	assert.Equal(t, uint32(0), source)

	id, _ = column.NextSourcedGeq(NewDescLID(1))
	assert.True(t, id.IsNull())
}

func consumeAt(src Sourced, lid uint32, asc bool) (LID, uint32, bool) {
	id, source := src.NextSourcedGeq(NewLID(lid, asc))
	if id.IsNull() || id.Unpack() != lid {
		return id, source, false
	}
	return id, source, true
}
