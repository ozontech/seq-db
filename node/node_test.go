package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func readAllInto(node Node, ids []uint32) []uint32 {
	id, has := node.Next()
	for has {
		ids = append(ids, id)
		id, has = node.Next()
	}
	return ids
}

func readAll(node Node) []uint32 {
	return readAllInto(node, nil)
}

func getRemainingSlice(t *testing.T, node Node) []uint32 {
	static, is := node.(*staticAsc)
	require.True(t, is, "node is not static")
	return static.data[static.ptr:]
}

var (
	data = [][]uint32{
		{1, 5, 6, 7, 8, 9, 13},
		{2, 3, 5, 6, 13, 14},
	}
)

func TestNodeAnd(t *testing.T) {
	expect := []uint32{5, 6, 13}
	and := NewAnd(NewStatic(data[0], false), NewStatic(data[1], false), false)
	assert.Equal(t, expect, readAll(and))
}

func TestNodeOr(t *testing.T) {
	expect := []uint32{1, 2, 3, 5, 6, 7, 8, 9, 13, 14}
	or := NewOr(NewStatic(data[0], false), NewStatic(data[1], false), false)
	assert.Equal(t, expect, readAll(or))
}

func TestNodeNAnd(t *testing.T) {
	expect := []uint32{2, 3, 14}
	nand := NewNAnd(NewStatic(data[0], false), NewStatic(data[1], false), false)
	assert.Equal(t, expect, readAll(nand))
}

func TestNodeNot(t *testing.T) {
	expect := []uint32{1, 4, 7, 8, 9, 10, 11, 12, 15}
	nand := NewNot(NewStatic(data[1], false), 1, 15, false)
	assert.Equal(t, expect, readAll(nand))
}

// test, that if one source is ended, node doesn't read the second one to the end
func TestNodeLazyAnd(t *testing.T) {
	left := []uint32{1, 2}
	right := []uint32{1, 2, 3, 4, 5, 6}
	and := NewAnd(NewStatic(left, false), NewStatic(right, false), false)
	assert.Equal(t, []uint32{1, 2}, readAll(and))
	assert.Equal(t, []uint32{4, 5, 6}, getRemainingSlice(t, and.right))
	assert.Equal(t, []uint32(nil), readAll(and))
	assert.Equal(t, []uint32{4, 5, 6}, getRemainingSlice(t, and.right))
}

// test, that if reg source is ended, node doesn't read the neg to the end
func TestNodeLazyNAnd(t *testing.T) {
	left := []uint32{1, 2, 5, 6, 7, 8}
	right := []uint32{2, 4}
	nand := NewNAnd(NewStatic(left, false), NewStatic(right, false), false)
	assert.Equal(t, []uint32{4}, readAll(nand))
	assert.Equal(t, []uint32{6, 7, 8}, getRemainingSlice(t, nand.neg))
	assert.Equal(t, []uint32(nil), readAll(nand))
	assert.Equal(t, []uint32{6, 7, 8}, getRemainingSlice(t, nand.neg))
}

func isEmptyNode(node any) bool {
	if sw, is := node.(*sourcedNodeWrapper); is {
		node = sw.node
	}
	if ns, is := node.(*staticAsc); is {
		return len(ns.data) == 0
	}
	return false
}

func TestNodeTreeBuilding(t *testing.T) {
	t.Run("size_0", func(t *testing.T) {
		dn := MakeStaticNodes(make([][]uint32, 0))
		assert.True(t, isEmptyNode(BuildORTree(dn, false)), "expected empty node")
		assert.True(t, isEmptyNode(BuildORTreeAgg(dn)), "expected empty node")
	})
	t.Run("size_1", func(t *testing.T) {
		dn := MakeStaticNodes(make([][]uint32, 1))
		assert.Equal(t, "STATIC", BuildORTree(dn, false).String())
		assert.Equal(t, "SOURCED", BuildORTreeAgg(dn).String())
	})
	t.Run("size_2", func(t *testing.T) {
		dn := MakeStaticNodes(make([][]uint32, 2))
		assert.Equal(t, "(STATIC OR STATIC)", BuildORTree(dn, false).String())
		assert.Equal(t, "(SOURCED OR SOURCED)", BuildORTreeAgg(dn).String())
	})
	t.Run("size_3", func(t *testing.T) {
		dn := MakeStaticNodes(make([][]uint32, 3))
		assert.Equal(t, "(STATIC OR (STATIC OR STATIC))", BuildORTree(dn, false).String())
		assert.Equal(t, "(SOURCED OR (SOURCED OR SOURCED))", BuildORTreeAgg(dn).String())
	})
	t.Run("size_4", func(t *testing.T) {
		dn := MakeStaticNodes(make([][]uint32, 4))
		assert.Equal(t, "((STATIC OR STATIC) OR (STATIC OR STATIC))", BuildORTree(dn, false).String())
		assert.Equal(t, "((SOURCED OR SOURCED) OR (SOURCED OR SOURCED))", BuildORTreeAgg(dn).String())
	})
	t.Run("size_5", func(t *testing.T) {
		dn := MakeStaticNodes(make([][]uint32, 5))
		assert.Equal(t, "((STATIC OR STATIC) OR (STATIC OR (STATIC OR STATIC)))", BuildORTree(dn, false).String())
		assert.Equal(t, "((SOURCED OR SOURCED) OR (SOURCED OR (SOURCED OR SOURCED)))", BuildORTreeAgg(dn).String())
	})
	t.Run("size_6", func(t *testing.T) {
		labels := BuildORTree(MakeStaticNodes(make([][]uint32, 6)), false).String()
		assert.Equal(t, "((STATIC OR (STATIC OR STATIC)) OR (STATIC OR (STATIC OR STATIC)))", labels)
	})
	t.Run("size_7", func(t *testing.T) {
		labels := BuildORTree(MakeStaticNodes(make([][]uint32, 7)), false).String()
		assert.Equal(t, "((STATIC OR (STATIC OR STATIC)) OR ((STATIC OR STATIC) OR (STATIC OR STATIC)))", labels)
	})
	t.Run("size_8", func(t *testing.T) {
		labels := BuildORTree(MakeStaticNodes(make([][]uint32, 8)), false).String()
		assert.Equal(t, "(((STATIC OR STATIC) OR (STATIC OR STATIC)) OR ((STATIC OR STATIC) OR (STATIC OR STATIC)))", labels)
	})
}
