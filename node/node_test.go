package node

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func readAllInto(node Node, ids []uint32) []uint32 {
	batch := node.Next(math.MaxUint32)
	for batch != nil {
		ids = append(ids, batch...)
		batch = node.Next(math.MaxUint32)
	}
	return ids
}

func readAll(node Node) []uint32 {
	return readAllInto(node, nil)
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

func isEmptyNode(node any) bool {
	if sw, is := node.(*sourcedNodeWrapper); is {
		node = sw.node
	}
	if ns, is := node.(*staticNode); is {
		return len(ns.data) == 0
	}
	return false
}

func TestNodeTreeBuilding(t *testing.T) {
	t.Run("size_0", func(t *testing.T) {
		dn := MakeStaticNodes(make([][]uint32, 0))
		assert.True(t, isEmptyNode(BuildORTree(dn, false)), "expected empty node")
		assert.True(t, isEmptyNode(BuildORTreeAgg(dn, false)), "expected empty node")
	})
	t.Run("size_1", func(t *testing.T) {
		dn := MakeStaticNodes(make([][]uint32, 1))
		assert.Equal(t, "STATIC", BuildORTree(dn, false).String())
		assert.Equal(t, "SOURCED", BuildORTreeAgg(dn, false).String())
	})
	t.Run("size_2", func(t *testing.T) {
		dn := MakeStaticNodes(make([][]uint32, 2))
		assert.Equal(t, "(STATIC OR STATIC)", BuildORTree(dn, false).String())
		assert.Equal(t, "(SOURCED OR SOURCED)", BuildORTreeAgg(dn, false).String())
	})
	t.Run("size_3", func(t *testing.T) {
		dn := MakeStaticNodes(make([][]uint32, 3))
		assert.Equal(t, "(STATIC OR (STATIC OR STATIC))", BuildORTree(dn, false).String())
		assert.Equal(t, "(SOURCED OR (SOURCED OR SOURCED))", BuildORTreeAgg(dn, false).String())
	})
	t.Run("size_4", func(t *testing.T) {
		dn := MakeStaticNodes(make([][]uint32, 4))
		assert.Equal(t, "((STATIC OR STATIC) OR (STATIC OR STATIC))", BuildORTree(dn, false).String())
		assert.Equal(t, "((SOURCED OR SOURCED) OR (SOURCED OR SOURCED))", BuildORTreeAgg(dn, false).String())
	})
	t.Run("size_5", func(t *testing.T) {
		dn := MakeStaticNodes(make([][]uint32, 5))
		assert.Equal(t, "((STATIC OR STATIC) OR (STATIC OR (STATIC OR STATIC)))", BuildORTree(dn, false).String())
		assert.Equal(t, "((SOURCED OR SOURCED) OR (SOURCED OR (SOURCED OR SOURCED)))", BuildORTreeAgg(dn, false).String())
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
