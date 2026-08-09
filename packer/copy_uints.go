// Byte->word conversion of the little-endian streams.
//
// On little-endian hosts the in-memory representation of []uint32/[]uint64 is
// exactly the on-disk little-endian stream, so the conversion is a bulk copy
// (runtime.memmove) instead of an element-wise decode loop. The loop tops out
// at ~4.5 GiB/s regardless of size (instruction-bound); memmove runs at
// memory/cache bandwidth (30-70 GiB/s), see copy_uints_test.go benchmarks.
// For the smallest inputs (streams of a couple of words: offsets arrays of
// blocks dominated by one heavy token, fraction-tail blocks) the memmove call
// overhead loses to a plain loop, so those stay element-wise.
//
// Big-endian hosts use the element-wise fallback in copy_uints_be.go.

//go:build 386 || amd64 || arm || arm64 || loong64 || mipsle || mips64le || ppc64le || riscv64 || wasm

package packer

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

// smallCopyWords: at or below this the plain loop beats the memmove call
// overhead. Measured crossover (BenchmarkSmallCopyCrossover, Cascade Lake):
// the loop wins at 1-2 words, ~ties at 3, memmove wins from 4 on (-15%)
// and widens quickly (-48% at 8 words).
const smallCopyWords = 3

// copyAsUints32 reinterprets dst as bytes and bulk-copies src into it.
// Panics if len(src) is not a multiple of the word size — same fail-fast
// behavior on a corrupted stream as the element-wise decode had; callers
// validate the length beforehand.
func copyAsUints32(src []byte, dst []uint32) []uint32 {
	if len(src)%sizeOfUint32 != 0 {
		panic(fmt.Sprintf("packer: ragged uint32 stream: %d bytes", len(src)))
	}
	n := len(src) / sizeOfUint32
	if cap(dst) < n {
		dst = make([]uint32, n)
	}
	dst = dst[:n]
	if n == 0 {
		return dst
	}
	if n <= smallCopyWords {
		for i := range dst {
			dst[i] = binary.LittleEndian.Uint32(src[i*sizeOfUint32:])
		}
		return dst
	}
	copy(unsafe.Slice((*byte)(unsafe.Pointer(&dst[0])), n*sizeOfUint32), src)
	return dst
}

// copyAsUints64 reinterprets dst as bytes and bulk-copies src into it.
// Panics if len(src) is not a multiple of the word size (see copyAsUints32).
func copyAsUints64(src []byte, dst []uint64) []uint64 {
	if len(src)%8 != 0 {
		panic(fmt.Sprintf("packer: ragged uint64 stream: %d bytes", len(src)))
	}
	n := len(src) / 8
	if cap(dst) < n {
		dst = make([]uint64, n)
	}
	dst = dst[:n]
	if n == 0 {
		return dst
	}
	if n <= smallCopyWords {
		for i := range dst {
			dst[i] = binary.LittleEndian.Uint64(src[i*8:])
		}
		return dst
	}
	copy(unsafe.Slice((*byte)(unsafe.Pointer(&dst[0])), n*8), src)
	return dst
}
