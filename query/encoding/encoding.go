package encoding

import (
	"encoding/binary"
	"math"

	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

// TODO: use buffer pools (???)

func StringToBytes(val string) []byte {
	return util.StringToByteUnsafe(val)
}

func StringFromBytes(val []byte) string {
	return string(val)
}

func SeqIDToBytes(id seq.ID) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b[:8], uint64(id.MID))
	binary.LittleEndian.PutUint64(b[8:], uint64(id.RID))
	return b
}

func SeqIDFromBytes(b []byte) seq.ID {
	return seq.ID{
		MID: seq.MID(binary.LittleEndian.Uint64(b[:8])),
		RID: seq.RID(binary.LittleEndian.Uint64(b[8:])),
	}
}

func Uint64ToBytes(val uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)
	return b
}

func Uint64FromBytes(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

func Uint32ToBytes(val uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, val)
	return b
}

func Uint32FromBytes(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

func Int64ToBytes(val int64) []byte {
	return Uint64ToBytes(uint64(val))
}

func Int64FromBytes(b []byte) int64 {
	return int64(binary.LittleEndian.Uint64(b))
}

func Int32ToBytes(val int32) []byte {
	return Uint32ToBytes(uint32(val))
}

func Int32FromBytes(b []byte) int32 {
	return int32(binary.LittleEndian.Uint32(b))
}

func Float64ToBytes(val float64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(val))
	return b
}

func Float64FromBytes(b []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(b))
}

// Float64ArrayToBytes encodes a float64 slice as an 8-byte little-endian length
// prefix followed by the little-endian raw bytes of each element. An empty or
// nil slice is encoded as a zero length (8 zero bytes).
func Float64ArrayToBytes(v []float64) []byte {
	b := make([]byte, 8+len(v)*8)
	binary.LittleEndian.PutUint64(b, uint64(len(v)))
	for i, f := range v {
		binary.LittleEndian.PutUint64(b[8+i*8:], math.Float64bits(f))
	}
	return b
}

// Float64ArrayFromBytes is the inverse of Float64ArrayToBytes.
func Float64ArrayFromBytes(b []byte) []float64 {
	if len(b) < 8 {
		return nil
	}
	n := binary.LittleEndian.Uint64(b)
	v := make([]float64, n)
	for i := range n {
		v[i] = math.Float64frombits(binary.LittleEndian.Uint64(b[8+i*8:]))
	}
	return v
}
