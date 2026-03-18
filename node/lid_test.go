package node

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLID_Unpack_Desc(t *testing.T) {
	x := NewDescLID(5)
	assert.Equal(t, uint32(5), x.Unpack())

	x = NewDescLID(math.MaxUint32)
	assert.Equal(t, uint32(math.MaxUint32), x.Unpack())

	x = NewDescLID(0)
	assert.Equal(t, uint32(0), x.Unpack())

	x = NullLID()

	assert.True(t, x.IsNull())
}

func TestLID_Unpack_Asc(t *testing.T) {
	x := NewAscLID(5)
	assert.Equal(t, uint32(5), x.Unpack())

	x = NewAscLID(0)
	assert.Equal(t, uint32(0), x.Unpack())

	x = NewAscLID(math.MaxUint32)
	assert.Equal(t, uint32(math.MaxUint32), x.Unpack())

	x = NullLID()

	assert.True(t, x.IsNull())
}

func TestLID_Eq(t *testing.T) {
	assert.Equal(t, NewDescLID(6), NewDescLID(6))
	assert.Equal(t, NewDescLID(math.MaxUint32), NewDescLID(math.MaxUint32))

	assert.Equal(t, NewAscLID(6), NewAscLID(6))
	assert.Equal(t, NewAscLID(0), NewAscLID(0))
}

func TestLID_Less_Desc(t *testing.T) {
	assert.False(t, NewDescLID(6).Less(NewDescLID(6)))
	assert.True(t, NewDescLID(6).Less(NewDescLID(7)))
	assert.True(t, NewDescLID(0).Less(NewDescLID(5)))

	assert.True(t, NewDescLID(56000).Less(NullLID()))
}

func TestLID_Less_Asc(t *testing.T) {
	// for asc sort order larger values go first (order is reversed), i.e. greater values are "less" than lower values
	assert.False(t, NewAscLID(6).Less(NewAscLID(6)))
	assert.True(t, NewAscLID(10).Less(NewAscLID(1)))
	assert.True(t, NewAscLID(5).Less(NewAscLID(0)))

	assert.True(t, NewAscLID(56000).Less(NullLID()))
}
