package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeRange_NextGeq_JumpToLast(t *testing.T) {
	node := NewRange(NewDescLID(3), NewDescLID(10))

	id := node.NextGeq(NewDescLID(10))
	assert.Equal(t, uint32(10), id.Unpack())
}

func TestNodeRange_NextGeq_SkipsWholeRange(t *testing.T) {
	node := NewRange(NewDescLID(3), NewDescLID(10))

	id := node.NextGeq(NewDescLID(11))
	assert.True(t, id.IsNull())
}

func TestNodeRange_NextGeq(t *testing.T) {
	node := NewRange(NewDescLID(3), NewDescLID(10))

	id := node.NextGeq(NewDescLID(5))
	assert.Equal(t, uint32(5), id.Unpack())

	id = node.NextGeq(NewDescLID(5))
	assert.Equal(t, uint32(6), id.Unpack())

	id = node.NextGeq(NewDescLID(10))
	assert.Equal(t, uint32(10), id.Unpack())

	id = node.NextGeq(NewDescLID(10))
	assert.True(t, id.IsNull())
}

func TestNodeRange_NextGeq_Reverse(t *testing.T) {
	node := NewRange(NewAscLID(10), NewAscLID(3))

	id := node.NextGeq(NewAscLID(9))
	assert.Equal(t, uint32(9), id.Unpack())

	id = node.NextGeq(NewAscLID(9))
	assert.Equal(t, uint32(8), id.Unpack())

	id = node.NextGeq(NewAscLID(4))
	assert.Equal(t, uint32(4), id.Unpack())

	id = node.NextGeq(NewAscLID(3))
	assert.Equal(t, uint32(3), id.Unpack())

	id = node.NextGeq(NewAscLID(3))
	assert.True(t, id.IsNull())
}
