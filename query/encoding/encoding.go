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
func Float64ArrayToBytes(val []float64) []byte {
	b := make([]byte, 8+len(val)*8)
	binary.LittleEndian.PutUint64(b, uint64(len(val)))
	for i, f := range val {
		binary.LittleEndian.PutUint64(b[8+i*8:], math.Float64bits(f))
	}
	return b
}

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

// StringArrayToBytes encodes a string slice as an 8-byte little-endian length
// prefix (number of strings) followed by each string prefixed with its own
// 8-byte little-endian length. An empty or nil slice is encoded as a zero
// length (8 zero bytes).
func StringArrayToBytes(val []string) []byte {
	// 8 bytes for the count, plus 8 bytes + len(s) per string.
	size := 8
	for _, s := range val {
		size += 8 + len(s)
	}
	b := make([]byte, size)
	binary.LittleEndian.PutUint64(b, uint64(len(val)))
	offset := 8
	for _, s := range val {
		binary.LittleEndian.PutUint64(b[offset:], uint64(len(s)))
		offset += 8
		copy(b[offset:], s)
		offset += len(s)
	}
	return b
}

func StringArrayFromBytes(b []byte) []string {
	if len(b) < 8 {
		return nil
	}
	n := binary.LittleEndian.Uint64(b)
	v := make([]string, n)
	offset := 8
	for i := range n {
		slen := binary.LittleEndian.Uint64(b[offset:])
		offset += 8
		v[i] = string(b[offset : offset+int(slen)])
		offset += int(slen)
	}
	return v
}
