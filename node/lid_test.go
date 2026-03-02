package node

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLID_Unpack_Desc(t *testing.T) {
	x := NewLIDOrderDesc(5)
	assert.Equal(t, uint32(5), x.Unpack())

	x = NewLIDOrderDesc(math.MaxUint32)
	assert.Equal(t, uint32(math.MaxUint32), x.Unpack())

	x = NewLIDOrderDesc(0)
	assert.Equal(t, uint32(0), x.Unpack())

	x = NullLID()

	assert.True(t, x.IsNull())
}

func TestLID_Unpack_Asc(t *testing.T) {
	x := NewLIDOrderAsc(5)
	assert.Equal(t, uint32(5), x.Unpack())

	x = NewLIDOrderAsc(0)
	assert.Equal(t, uint32(0), x.Unpack())

	x = NewLIDOrderAsc(math.MaxUint32)
	assert.Equal(t, uint32(math.MaxUint32), x.Unpack())

	x = NullLID()

	assert.True(t, x.IsNull())
}

func TestLID_Eq(t *testing.T) {
	assert.Equal(t, NewLIDOrderDesc(6), NewLIDOrderDesc(6))
	assert.Equal(t, NewLIDOrderDesc(math.MaxUint32), NewLIDOrderDesc(math.MaxUint32))

	assert.Equal(t, NewLIDOrderAsc(6), NewLIDOrderAsc(6))
	assert.Equal(t, NewLIDOrderAsc(0), NewLIDOrderAsc(0))
}

func TestLID_Less_Desc(t *testing.T) {
	assert.False(t, NewLIDOrderDesc(6).Less(NewLIDOrderDesc(6)))
	assert.True(t, NewLIDOrderDesc(6).Less(NewLIDOrderDesc(7)))
	assert.True(t, NewLIDOrderDesc(0).Less(NewLIDOrderDesc(5)))

	assert.True(t, NewLIDOrderDesc(56000).Less(NullLID()))
}

func TestLID_Less_Asc(t *testing.T) {
	// for asc sort order larger values go first (order is reversed), i.e. greater values are "less" than lower values
	assert.False(t, NewLIDOrderAsc(6).Less(NewLIDOrderAsc(6)))
	assert.True(t, NewLIDOrderAsc(10).Less(NewLIDOrderAsc(1)))
	assert.True(t, NewLIDOrderAsc(5).Less(NewLIDOrderAsc(0)))

	assert.True(t, NewLIDOrderAsc(56000).Less(NullLID()))
}
