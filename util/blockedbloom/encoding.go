package blockedbloom

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"unsafe"
)

// Bloom header format: MMMM : VV : KK : NNNN-NNNN : 44 bytes of padding : CCCC
// Multi-byte integers are little-endian.

const (
	bloomMagic   = "BBLM"
	bloomVersion = 1
)

const (
	offsetBloomMagic          = 0  // 4 bytes  (M) Magic
	offsetBloomVersion        = 4  // 2 bytes  (V) Version
	offsetBloomProbeCount     = 6  // 2 bytes  (K) Number of probes per key
	offsetBloomBlockCount     = 8  // 8 bytes  (N) Number of blocks
	offsetBloomPadding        = 16 // 44 bytes (P) Zero padding
	offsetBloomHeaderChecksum = 60 // 4 bytes  (C) Header checksum
)

const (
	headerSize   = 64
	checksumSize = 4
	groupSize    = 4096
)

const blocksPerGroup = groupSize / BlockSize

const (
	maxBloomBlocks = maxBloomGroups * blocksPerGroup
	maxBloomGroups = (math.MaxInt/groupSize*groupSize - headerSize) /
		(groupSize + checksumSize)
)

var ErrChecksumMismatch = errors.New("bloom checksum mismatch")

func (l bloomLayout) groupCount() int {
	return l.blocks / blocksPerGroup
}

func (l bloomLayout) dataOffset() int {
	tableEnd := headerSize + l.groupCount()*checksumSize
	// Section with checksums is padded until [groupSize].
	return (tableEnd + groupSize - 1) / groupSize * groupSize
}

func (l bloomLayout) putHeader(header []byte) {
	copy(header[offsetBloomMagic:], bloomMagic)

	binary.LittleEndian.PutUint16(header[offsetBloomVersion:], bloomVersion)
	binary.LittleEndian.PutUint16(header[offsetBloomProbeCount:], uint16(l.probes))
	binary.LittleEndian.PutUint64(header[offsetBloomBlockCount:offsetBloomPadding], uint64(l.blocks))

	checksum := crc32.ChecksumIEEE(header[:offsetBloomHeaderChecksum])
	binary.LittleEndian.PutUint32(header[offsetBloomHeaderChecksum:], checksum)
}

func (l bloomLayout) putChecksums(checksums, data []byte) {
	for i := range l.groupCount() {
		start := i * groupSize
		checksum := crc32.ChecksumIEEE(data[start : start+groupSize])
		binary.LittleEndian.PutUint32(checksums[i*checksumSize:], checksum)
	}
}

func (f *Filter) WriteTo(w io.Writer) (int64, error) {
	if f.layout.blocks == 0 {
		return 0, fmt.Errorf("cannot write an uninitialized bloom filter")
	}

	first := unsafe.SliceData(f.blocks)
	data := unsafe.Slice((*byte)(unsafe.Pointer(first)), len(f.blocks)*BlockSize)

	prefix := make([]byte, f.layout.dataOffset())
	f.layout.putHeader(prefix[:headerSize])
	f.layout.putChecksums(prefix[headerSize:], data)

	n, err := w.Write(prefix)
	written := int64(n)
	if err != nil {
		return written, fmt.Errorf("write bloom prefix: %w", err)
	}

	n, err = w.Write(data)
	written += int64(n)
	if err != nil {
		return written, fmt.Errorf("write bloom blocks: %w", err)
	}

	return written, nil
}

func decodeLayout(header [headerSize]byte) (bloomLayout, error) {
	if string(header[offsetBloomMagic:offsetBloomVersion]) != bloomMagic {
		return bloomLayout{}, fmt.Errorf("invalid bloom magic")
	}

	version := binary.LittleEndian.Uint16(header[offsetBloomVersion:])
	if version != bloomVersion {
		return bloomLayout{}, fmt.Errorf("unsupported bloom version %d", version)
	}

	checksum := binary.LittleEndian.Uint32(header[offsetBloomHeaderChecksum:])
	if crc32.ChecksumIEEE(header[:offsetBloomHeaderChecksum]) != checksum {
		return bloomLayout{}, fmt.Errorf("bloom header: %w", ErrChecksumMismatch)
	}

	probes := binary.LittleEndian.Uint16(header[offsetBloomProbeCount:])
	if probes == 0 || probes > maxBloomProbes {
		return bloomLayout{}, fmt.Errorf("invalid bloom probe count %d", probes)
	}

	blocks := binary.LittleEndian.Uint64(header[offsetBloomBlockCount:offsetBloomPadding])
	if blocks == 0 || blocks > maxBloomBlocks || blocks%blocksPerGroup != 0 {
		return bloomLayout{}, fmt.Errorf("invalid bloom block count %d", blocks)
	}

	return bloomLayout{blocks: int(blocks), probes: int(probes)}, nil
}
