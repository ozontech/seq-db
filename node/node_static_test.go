package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStaticAscNextGeq(t *testing.T) {
	lids := []uint32{1, 3, 5, 7, 9}
	n := NewStatic(lids, false).(*staticAsc)

	id := n.NextGeq(NewCmpLIDOrderDesc(0))
	assert.False(t, id.IsNull())
	assert.Equal(t, uint32(1), id.Unpack())

	id = n.NextGeq(NewCmpLIDOrderDesc(4))
	assert.False(t, id.IsNull())
	assert.Equal(t, uint32(5), id.Unpack())

	// 5 has already been returned, so the next value >= 5 is 7.
	id = n.NextGeq(NewCmpLIDOrderDesc(5))
	assert.False(t, id.IsNull())
	assert.Equal(t, uint32(7), id.Unpack())

	id = n.NextGeq(NewCmpLIDOrderDesc(10))
	assert.True(t, id.IsNull())
}

func TestStaticDescNextGeq(t *testing.T) {
	lids := []uint32{1, 3, 5, 7, 9}
	n := NewStatic(lids, true).(*staticDesc)

	id := n.NextGeq(NewCmpLIDOrderDesc(10))
	assert.False(t, id.IsNull())
	assert.Equal(t, uint32(9), id.Unpack())

	id = n.NextGeq(NewCmpLIDOrderDesc(10))
	assert.False(t, id.IsNull())
	assert.Equal(t, uint32(7), id.Unpack())

	id = n.NextGeq(NewCmpLIDOrderDesc(10))
	assert.False(t, id.IsNull())
	assert.Equal(t, uint32(5), id.Unpack())
}

func TestStaticDescNextGeq_WithThreshold(t *testing.T) {
	lids := []uint32{1, 3, 5, 7, 9}
	n := NewStatic(lids, true).(*staticDesc)

	id := n.NextGeq(NewCmpLIDOrderDesc(8))
	assert.False(t, id.IsNull())
	assert.Equal(t, uint32(7), id.Unpack())

	id = n.NextGeq(NewCmpLIDOrderDesc(8))
	assert.False(t, id.IsNull())
	assert.Equal(t, uint32(5), id.Unpack())

	id = n.NextGeq(NewCmpLIDOrderDesc(8))
	assert.False(t, id.IsNull())
	assert.Equal(t, uint32(3), id.Unpack())

	id = n.NextGeq(NewCmpLIDOrderDesc(8))
	assert.False(t, id.IsNull())
	assert.Equal(t, uint32(1), id.Unpack())

	id = n.NextGeq(NewCmpLIDOrderDesc(8))
	assert.True(t, id.IsNull())
}
