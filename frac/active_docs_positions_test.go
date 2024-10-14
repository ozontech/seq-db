package frac

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPackDocPos(t *testing.T) {
	// min
	origOffset := uint64(0)
	origIndex := uint32(0)

	docPos := PackDocPos(origIndex, origOffset)
	resIndex, resOffset := docPos.Unpack()

	assert.Equal(t, origOffset, resOffset)
	assert.Equal(t, origIndex, resIndex)

	// max
	origOffset = uint64(maxDocOffset)
	origIndex = uint32(maxBlockIndex)

	docPos = PackDocPos(origIndex, origOffset)
	resIndex, resOffset = docPos.Unpack()

	assert.Equal(t, origOffset, resOffset)
	assert.Equal(t, origIndex, resIndex)

	// rand
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 100; i++ {
		origOffset = uint64(rand.Float64() * maxDocOffset)
		origIndex = uint32(rand.Float64() * maxBlockIndex)

		docPos = PackDocPos(origIndex, origOffset)
		resIndex, resOffset = docPos.Unpack()

		assert.Equal(t, origOffset, resOffset)
		assert.Equal(t, origIndex, resIndex)
	}
}
