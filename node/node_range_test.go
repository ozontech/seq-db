package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeRange_NextGeq_JumpToLast(t *testing.T) {
	node := NewRange(NewAscLID(3), NewAscLID(10))

	id := node.NextGeq(NewAscLID(10))
	assert.Equal(t, uint32(10), id.Unpack())
}

func TestNodeRange_NextGeq_SkipsWholeRange(t *testing.T) {
	node := NewRange(NewAscLID(3), NewAscLID(10))

	id := node.NextGeq(NewAscLID(11))
	assert.True(t, id.IsNull())
}

func TestNodeRange_NextGeq(t *testing.T) {
	node := NewRange(NewAscLID(3), NewAscLID(10))

	id := node.NextGeq(NewAscLID(5))
	assert.Equal(t, uint32(5), id.Unpack())

	id = node.NextGeq(NewAscLID(5))
	assert.Equal(t, uint32(6), id.Unpack())

	id = node.NextGeq(NewAscLID(10))
	assert.Equal(t, uint32(10), id.Unpack())

	id = node.NextGeq(NewAscLID(10))
	assert.True(t, id.IsNull())
}

func TestNodeRange_NextGeq_Reverse(t *testing.T) {
	node := NewRange(NewDescLID(10), NewDescLID(3))

	id := node.NextGeq(NewDescLID(9))
	assert.Equal(t, uint32(9), id.Unpack())

	id = node.NextGeq(NewDescLID(9))
	assert.Equal(t, uint32(8), id.Unpack())

	id = node.NextGeq(NewDescLID(4))
	assert.Equal(t, uint32(4), id.Unpack())

	id = node.NextGeq(NewDescLID(3))
	assert.Equal(t, uint32(3), id.Unpack())

	id = node.NextGeq(NewDescLID(3))
	assert.True(t, id.IsNull())
}
