package node

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLID_Unpack_Desc(t *testing.T) {
	x := NewLIDDesc(5)
	assert.Equal(t, uint32(5), x.Unpack())

	x = NewLIDDesc(math.MaxUint32)
	assert.Equal(t, uint32(math.MaxUint32), x.Unpack())

	x = NewLIDDesc(0)
	assert.Equal(t, uint32(0), x.Unpack())

	x = NullLID()

	assert.True(t, x.IsNull())
}

func TestLID_Unpack_Asc(t *testing.T) {
	x := NewLIDAsc(5)
	assert.Equal(t, uint32(5), x.Unpack())

	x = NewLIDAsc(0)
	assert.Equal(t, uint32(0), x.Unpack())

	x = NewLIDAsc(math.MaxUint32)
	assert.Equal(t, uint32(math.MaxUint32), x.Unpack())

	x = NullLID()

	assert.True(t, x.IsNull())
}

func TestLID_Eq(t *testing.T) {
	assert.Equal(t, NewLIDDesc(6), NewLIDDesc(6))
	assert.Equal(t, NewLIDDesc(math.MaxUint32), NewLIDDesc(math.MaxUint32))

	assert.Equal(t, NewLIDAsc(6), NewLIDAsc(6))
	assert.Equal(t, NewLIDAsc(0), NewLIDAsc(0))
}

func TestLID_Less_Desc(t *testing.T) {
	assert.False(t, NewLIDDesc(6).Less(NewLIDDesc(6)))
	assert.True(t, NewLIDDesc(6).Less(NewLIDDesc(7)))
	assert.True(t, NewLIDDesc(0).Less(NewLIDDesc(5)))

	assert.True(t, NewLIDDesc(56000).Less(NullLID()))
}

func TestLID_Less_Asc(t *testing.T) {
	// for asc sort order larger values go first (order is reversed), i.e. greater values are "less" than lower values
	assert.False(t, NewLIDAsc(6).Less(NewLIDAsc(6)))
	assert.True(t, NewLIDAsc(10).Less(NewLIDAsc(1)))
	assert.True(t, NewLIDAsc(5).Less(NewLIDAsc(0)))

	assert.True(t, NewLIDAsc(56000).Less(NullLID()))
}
