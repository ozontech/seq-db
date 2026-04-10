package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestConvertDocToWalBlock(t *testing.T) {
	payload := []byte("test test payload")

	docBlock := CompressDocBlock(payload, nil, 1)
	docBlock.SetExt2(11111)

	walBlock := PackDocBlockToWalBlock(docBlock)

	assert.Equal(t, CodecZSTD, walBlock.Codec())
	assert.Equal(t, uint32(len(payload)), walBlock.RawLen())
	assert.Equal(t, uint64(11111), walBlock.DocsOffset())
	assert.True(t, walBlock.IsCorrect())

	decompressed, err := walBlock.DecompressTo(nil)
	require.NoError(t, err)
	assert.Equal(t, payload, decompressed)
}

func TestConvertMetaToDocBlock(t *testing.T) {
	payload := []byte("test payload data")

	walBlock := CompressWalBlock(payload, nil, 1)
	walBlock.SetDocsOffset(22222)

	docBlock := PackWalBlockToDocBlock(walBlock, nil)

	assert.Equal(t, CodecZSTD, docBlock.Codec())
	assert.Equal(t, uint64(len(payload)), docBlock.RawLen())
	assert.Equal(t, uint64(22222), docBlock.GetExt2())

	decompressed, err := docBlock.DecompressTo(nil)
	require.NoError(t, err)
	assert.Equal(t, payload, decompressed)
}
