package util

import (
	"encoding/binary"
	"unsafe"
)

const (
	sizeOfUint32 = int(unsafe.Sizeof(uint32(0)))
	sizeOfUint64 = int(unsafe.Sizeof(uint64(0)))
)

var (
	littleEndian = detectLittleEndian()
)

func detectLittleEndian() bool {
	var i int32 = 1
	b := *(*byte)(unsafe.Pointer(&i))
	return b == 1
}

// CopyUints32 copies srt to dst byte slice. If host is little-endian, then uses direct memory copy instead of loop.
func CopyUints32(src []uint32, dst []byte) []byte {
	if littleEndian {
		oldLen := len(dst)
		newLen := oldLen + len(src)*sizeOfUint32

		dst = ensureCap(dst, newLen)

		tail := dst[oldLen:newLen]
		u32 := unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(tail))), len(src))
		copy(u32, src)
	} else {
		for _, v := range src {
			dst = binary.LittleEndian.AppendUint32(dst, v)
		}
	}
	return dst
}

// CopyUints64 copies srt to dst byte slice. If host is little-endian, then uses direct memory copy instead of loop.
func CopyUints64(src []uint64, dst []byte) []byte {
	if littleEndian {
		oldLen := len(dst)
		newLen := oldLen + len(src)*sizeOfUint64

		dst = ensureCap(dst, newLen)

		tail := dst[oldLen:newLen]
		u64 := unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(tail))), len(src))
		copy(u64, src)
	} else {
		for _, v := range src {
			dst = binary.LittleEndian.AppendUint64(dst, v)
		}
	}
	return dst
}

func ensureCap(dst []byte, newLen int) []byte {
	if cap(dst) < newLen {
		buf := make([]byte, newLen)
		copy(buf, dst)
		dst = buf
	} else {
		dst = dst[:newLen]
	}
	return dst
}

// CastAsUint32 allows working on []byte slice as []uint32. Uses unsafe casts for little endian, allocates
// a new buf and copies on big endian hosts. The caller must work as read only.
func CastAsUint32(buf []byte) []uint32 {
	n := len(buf) / sizeOfUint32
	if n == 0 {
		return nil
	}

	if littleEndian {
		return unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(buf))), n)
	}

	res := make([]uint32, n)
	for i := 0; i < n; i++ {
		res[i] = binary.LittleEndian.Uint32(buf)
		buf = buf[sizeOfUint32:]
	}
	return res
}

// CastAsUint64 allows working on []byte slice as []uint64. Uses unsafe casts for little endian, allocates
// a new buf and copies on big endian hosts. The caller must work as read only.
func CastAsUint64(buf []byte) []uint64 {
	n := len(buf) / sizeOfUint64
	if n == 0 {
		return nil
	}

	if littleEndian {
		return unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(buf))), n)
	}

	res := make([]uint64, n)
	for i := 0; i < n; i++ {
		res[i] = binary.LittleEndian.Uint64(buf)
		buf = buf[sizeOfUint64:]
	}
	return res
}
