package encoding

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/seq"
)

func TestString(t *testing.T) {
	values := []string{
		"",
		"hello",
		"привет",
		"service-a",
		"46e48be997010000-e70163d0fa7582e4",
	}
	for _, v := range values {
		assert.Equal(t, v, StringFromBytes(StringToBytes(v)))
	}
}

func TestSeqID(t *testing.T) {
	values := []seq.ID{
		{},
		{MID: 1, RID: 0},
		{MID: 0, RID: 1},
		{MID: math.MaxUint64, RID: math.MaxUint64},
		{MID: 0x46e48be997010000, RID: 0xe70163d0fa7582e4},
	}
	for _, v := range values {
		assert.Equal(t, v, SeqIDFromBytes(SeqIDToBytes(v)))
	}
}

func TestUint64(t *testing.T) {
	values := []uint64{
		0,
		1,
		42,
		math.MaxUint32,
		math.MaxUint64,
		0x46e48be997010000,
	}
	for _, v := range values {
		assert.Equal(t, v, Uint64FromBytes(Uint64ToBytes(v)))
	}
}

func TestUint32(t *testing.T) {
	values := []uint32{
		0,
		1,
		42,
		math.MaxUint16,
		math.MaxUint32,
	}
	for _, v := range values {
		assert.Equal(t, v, Uint32FromBytes(Uint32ToBytes(v)))
	}
}

func TestInt64(t *testing.T) {
	values := []int64{
		math.MinInt64,
		-1,
		0,
		1,
		math.MaxInt64,
		0x46e48be997010000,
	}
	for _, v := range values {
		assert.Equal(t, v, Int64FromBytes(Int64ToBytes(v)))
	}
}

func TestInt32(t *testing.T) {
	values := []int32{
		math.MinInt32,
		-1,
		0,
		1,
		math.MaxInt32,
	}
	for _, v := range values {
		assert.Equal(t, v, Int32FromBytes(Int32ToBytes(v)))
	}
}

func TestFloat64(t *testing.T) {
	values := []float64{
		0,
		1,
		-1,
		42.5,
		math.Pi,
		math.MaxFloat64,
		math.SmallestNonzeroFloat64,
		-math.MaxFloat64,
	}
	for _, v := range values {
		assert.Equal(t, v, Float64FromBytes(Float64ToBytes(v)))
	}
}

// TestEncodedByteLengths verifies that each encoder produces the expected
// fixed-size buffer, since the decoders read that many bytes unguarded.
func TestEncodedByteLengths(t *testing.T) {
	assert.Len(t, SeqIDToBytes(seq.ID{MID: 1, RID: 2}), 16, "SeqID")
	assert.Len(t, Uint64ToBytes(1), 8, "Uint64")
	assert.Len(t, Uint32ToBytes(1), 4, "Uint32")
	assert.Len(t, Int64ToBytes(1), 8, "Int64")
	assert.Len(t, Int32ToBytes(1), 4, "Int32")
	assert.Len(t, Float64ToBytes(1), 8, "Float64")
}

func TestFloat64Array(t *testing.T) {
	cases := [][]float64{
		nil,
		{},
		{0},
		{1, -1, 42.5, math.Pi, math.MaxFloat64, math.SmallestNonzeroFloat64, -math.MaxFloat64},
	}
	for _, v := range cases {
		// nil and an empty slice both encode as a zero length and decode to an
		// empty (non-nil) slice, so compare elements rather than nil-ness.
		assert.ElementsMatch(t, v, Float64ArrayFromBytes(Float64ArrayToBytes(v)))
	}
	assert.Len(t, Float64ArrayFromBytes(Float64ArrayToBytes(nil)), 0)
}

func TestFloat64ArrayByteLength(t *testing.T) {
	assert.Len(t, Float64ArrayToBytes(nil), 8)
	assert.Len(t, Float64ArrayToBytes([]float64{}), 8)
	assert.Len(t, Float64ArrayToBytes([]float64{1, 2, 3}), 8+3*8)
}
