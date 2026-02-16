package node

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCmpLID_ConvertionDesc(t *testing.T) {
	x := NewCmpLIDOrderDesc(5)
	assert.Equal(t, uint32(5), x.Unpack())

	x = NewCmpLIDOrderDesc(math.MaxUint32)
	assert.Equal(t, uint32(math.MaxUint32), x.Unpack())

	x = NewCmpLIDOrderDesc(0)
	assert.Equal(t, uint32(0), x.Unpack())

	x = NullCmpLID()

	assert.True(t, x.IsNull())
}

func TestCmpLID_ConvertionAsc(t *testing.T) {
	x := NewCmpLIDOrderAsc(5)
	assert.Equal(t, uint32(5), x.Unpack())

	x = NewCmpLIDOrderAsc(0)
	assert.Equal(t, uint32(0), x.Unpack())

	x = NewCmpLIDOrderAsc(math.MaxUint32)
	assert.Equal(t, uint32(math.MaxUint32), x.Unpack())

	x = NullCmpLID()

	assert.True(t, x.IsNull())
}
