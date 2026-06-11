package token

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/config"
)

func TestBlock_PackUnpack_NoFreq(t *testing.T) {
	src := Block{
		Payload: packTokenPayload([]byte("foo"), []byte("bar")),
	}

	var buf []uint32
	packed := src.Pack(nil, buf)
	var dst Block
	require.NoError(t, dst.Unpack(packed, config.BinaryDataV5, &UnpackBuffer{}))

	assert.Equal(t, 2, dst.Len())
	assert.Equal(t, []byte("foo"), dst.GetToken(0))
	assert.Equal(t, []byte("bar"), dst.GetToken(1))

	assert.Empty(t, dst.FreqIndexes)
	assert.Empty(t, dst.Freqs)
}

func TestBlock_PackUnpack_WithFreq(t *testing.T) {
	src := Block{
		Payload:     packTokenPayload([]byte("dog"), []byte("cat"), []byte("horse"), []byte("duck")),
		FreqIndexes: []uint16{0, 2},
		Freqs:       []uint32{100, 200},
	}

	var buf []uint32
	packed := src.Pack(nil, buf)
	var dst Block
	require.NoError(t, dst.Unpack(packed, config.BinaryDataV5, &UnpackBuffer{}))

	assert.Equal(t, src.Payload, dst.Payload)

	assert.Equal(t, uint32(100), dst.GetFreq(0))
	assert.Equal(t, uint32(0), dst.GetFreq(1))
	assert.Equal(t, uint32(200), dst.GetFreq(2))
	assert.Equal(t, uint32(0), dst.GetFreq(3))
}

func TestBlock_Unpack_Legacy(t *testing.T) {
	legacy := packTokenPayload([]byte("legacy"))

	var dst Block
	require.NoError(t, dst.Unpack(legacy, config.BinaryDataV4, &UnpackBuffer{}))

	assert.Equal(t, legacy, dst.Payload)
	assert.Equal(t, []uint32{0}, dst.Offsets)
	assert.Empty(t, dst.FreqIndexes)
	assert.Empty(t, dst.Freqs)
}

func TestBlock_UnpackBufferReuse(t *testing.T) {
	src := Block{
		Payload:     packTokenPayload([]byte("a"), []byte("b")),
		FreqIndexes: []uint16{1},
		Freqs:       []uint32{64},
	}

	var packBuf []uint32
	packed := src.Pack(nil, packBuf)

	var dst1, dst2 Block
	require.NoError(t, dst1.Unpack(packed, config.BinaryDataV5, &UnpackBuffer{}))
	require.NoError(t, dst2.Unpack(packed, config.BinaryDataV5, &UnpackBuffer{}))

	assert.Equal(t, dst1.FreqIndexes, dst2.FreqIndexes)
	assert.Equal(t, dst1.Freqs, dst2.Freqs)

	assert.Equal(t, uint32(0), dst2.GetFreq(0))
	assert.Equal(t, uint32(64), dst2.GetFreq(1))
}

func packTokenPayload(tokens ...[]byte) []byte {
	var payload []byte
	for _, tok := range tokens {
		payload = binary.LittleEndian.AppendUint32(payload, uint32(len(tok)))
		payload = append(payload, tok...)
	}
	return payload
}
