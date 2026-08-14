package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCorruptionDetection(t *testing.T) {
	payload := []byte("test payload for test of checking corrupted data")
	block := CompressWalBlock(payload, nil, 1)

	assert.True(t, block.IsCorrect())

	// change 1 byte of data
	prev := block[35]
	block[35] = 3
	assert.False(t, block.IsCorrect())
	block[35] = prev
	assert.True(t, block.IsCorrect())

	// change type (the first byte)
	block[0] = 7
	assert.False(t, block.IsCorrect())
	block[0] = WalBlockMagic

	// change version (second byte)
	block[1] = 233
	assert.False(t, block.IsCorrect())
	block[1] = 1 // TODO use meta block version const

	// change length of block
	truncated := block[0 : len(block)-3]
	assert.False(t, truncated.IsCorrect())

	block = append(block, 1, 2, 3)
	assert.False(t, block.IsCorrect())
}
