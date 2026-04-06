package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeNAnd_NextGeq(t *testing.T) {
	neg := NewStatic([]uint32{1, 2, 7, 10, 20, 25, 26, 30, 50, 80, 90, 100}, false)
	reg := NewStatic([]uint32{1, 3, 4, 7, 9, 30, 40, 45, 60, 80, 110}, false)

	node := NewNAnd(neg, reg)

	id := node.NextGeq(NewDescLID(7))
	assert.Equal(t, uint32(9), id.Unpack())

	id = node.NextGeq(NewDescLID(50))
	assert.Equal(t, uint32(60), id.Unpack())

	id = node.NextGeq(NewDescLID(100))
	assert.Equal(t, uint32(110), id.Unpack())

	id = node.NextGeq(NewDescLID(100))
	assert.True(t, id.IsNull())
}

func TestNodeNAnd_NextGeq_Reverse(t *testing.T) {
	neg := NewStatic([]uint32{1, 2, 7, 10, 20, 25, 26, 30, 50, 80, 90, 100}, true)
	reg := NewStatic([]uint32{1, 3, 4, 7, 9, 30, 40, 45, 60, 80, 110}, true)

	node := NewNAnd(neg, reg)

	id := node.NextGeq(NewAscLID(80))
	assert.Equal(t, uint32(60), id.Unpack())

	id = node.NextGeq(NewAscLID(49))
	assert.Equal(t, uint32(45), id.Unpack())

	// call with same nextID, should just return next value
	id = node.NextGeq(NewAscLID(49))
	assert.Equal(t, uint32(40), id.Unpack())

	id = node.NextGeq(NewAscLID(49))
	assert.Equal(t, uint32(9), id.Unpack())

	id = node.NextGeq(NewAscLID(4))
	assert.Equal(t, uint32(4), id.Unpack())

	id = node.NextGeq(NewAscLID(1))
	assert.True(t, id.IsNull())
}
