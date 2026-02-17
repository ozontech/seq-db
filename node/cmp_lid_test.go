package node

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCmpLID_Unpack_Desc(t *testing.T) {
	x := NewCmpLIDOrderDesc(5)
	assert.Equal(t, uint32(5), x.Unpack())

	x = NewCmpLIDOrderDesc(math.MaxUint32)
	assert.Equal(t, uint32(math.MaxUint32), x.Unpack())

	x = NewCmpLIDOrderDesc(0)
	assert.Equal(t, uint32(0), x.Unpack())

	x = NullCmpLID()

	assert.True(t, x.IsNull())
}

func TestCmpLID_Unpack_Asc(t *testing.T) {
	x := NewCmpLIDOrderAsc(5)
	assert.Equal(t, uint32(5), x.Unpack())

	x = NewCmpLIDOrderAsc(0)
	assert.Equal(t, uint32(0), x.Unpack())

	x = NewCmpLIDOrderAsc(math.MaxUint32)
	assert.Equal(t, uint32(math.MaxUint32), x.Unpack())

	x = NullCmpLID()

	assert.True(t, x.IsNull())
}

func TestCmpLID_Eq(t *testing.T) {
	assert.Equal(t, NewCmpLIDOrderDesc(6), NewCmpLIDOrderDesc(6))
	assert.Equal(t, NewCmpLIDOrderDesc(math.MaxUint32), NewCmpLIDOrderDesc(math.MaxUint32))

	assert.Equal(t, NewCmpLIDOrderAsc(6), NewCmpLIDOrderAsc(6))
	assert.Equal(t, NewCmpLIDOrderAsc(0), NewCmpLIDOrderAsc(0))
}

func TestCmpLID_Less_Desc(t *testing.T) {
	assert.False(t, NewCmpLIDOrderDesc(6).Less(NewCmpLIDOrderDesc(6)))
	assert.True(t, NewCmpLIDOrderDesc(6).Less(NewCmpLIDOrderDesc(7)))
	assert.True(t, NewCmpLIDOrderDesc(0).Less(NewCmpLIDOrderDesc(5)))

	assert.True(t, NewCmpLIDOrderDesc(56000).Less(NullCmpLID()))
}

func TestCmpLID_Less_Asc(t *testing.T) {
	// for asc sort order larger values go first (order is reversed), i.e. greater values are "less" than lower values
	assert.False(t, NewCmpLIDOrderAsc(6).Less(NewCmpLIDOrderAsc(6)))
	assert.True(t, NewCmpLIDOrderAsc(10).Less(NewCmpLIDOrderAsc(1)))
	assert.True(t, NewCmpLIDOrderAsc(5).Less(NewCmpLIDOrderAsc(0)))

	assert.True(t, NewCmpLIDOrderAsc(56000).Less(NullCmpLID()))
}
