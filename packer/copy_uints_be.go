// Big-endian fallback: element-wise little-endian decode with the same
// contract as the bulk-copy variant (including the ragged-length panic).
// No release target or CI runs big-endian, so this path is not exercised
// by any automated build.

//go:build !(386 || amd64 || arm || arm64 || loong64 || mipsle || mips64le || ppc64le || riscv64 || wasm)

package packer

import (
	"encoding/binary"
	"fmt"
)

func copyAsUints32(src []byte, dst []uint32) []uint32 {
	if len(src)%sizeOfUint32 != 0 {
		panic(fmt.Sprintf("packer: ragged uint32 stream: %d bytes", len(src)))
	}
	n := len(src) / sizeOfUint32
	if cap(dst) < n {
		dst = make([]uint32, n)
	}
	dst = dst[:n]
	for i := range dst {
		dst[i] = binary.LittleEndian.Uint32(src[i*sizeOfUint32:])
	}
	return dst
}

func copyAsUints64(src []byte, dst []uint64) []uint64 {
	if len(src)%8 != 0 {
		panic(fmt.Sprintf("packer: ragged uint64 stream: %d bytes", len(src)))
	}
	n := len(src) / 8
	if cap(dst) < n {
		dst = make([]uint64, n)
	}
	dst = dst[:n]
	for i := range dst {
		dst[i] = binary.LittleEndian.Uint64(src[i*8:])
	}
	return dst
}
