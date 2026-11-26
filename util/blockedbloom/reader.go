package blockedbloom

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
)

var bloomGroupPool = sync.Pool{
	New: func() any { return new([groupSize]byte) },
}

type BlockReader struct {
	reader     io.ReaderAt
	layout     bloomLayout
	checksums  []uint32
	dataOffset int
}

func Open(r io.ReaderAt) (*BlockReader, error) {
	var header [headerSize]byte
	if err := readFullAt(r, header[:], 0); err != nil {
		return nil, fmt.Errorf("read bloom header: %w", err)
	}

	layout, err := decodeLayout(header)
	if err != nil {
		return nil, err
	}

	groups := layout.groupCount()
	table := make([]byte, groups*checksumSize)
	if err := readFullAt(r, table, headerSize); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("read bloom checksum table: %w", err)
	}

	checksums := make([]uint32, groups)
	for i := range checksums {
		checksums[i] = binary.LittleEndian.Uint32(table[i*checksumSize:])
	}

	return &BlockReader{
		reader:     r,
		layout:     layout,
		checksums:  checksums,
		dataOffset: layout.dataOffset(),
	}, nil
}

func (r *BlockReader) MayContain(hash uint64) (bool, error) {
	blockOffset := r.layout.blockOffset(hash)
	group := blockOffset / groupSize
	offset := r.dataOffset + group*groupSize

	page := bloomGroupPool.Get().(*[groupSize]byte)
	defer bloomGroupPool.Put(page)

	if err := readFullAt(r.reader, page[:], int64(offset)); err != nil {
		return false, fmt.Errorf(
			"read bloom group %d at offset %d: %w",
			group, offset, err,
		)
	}

	if crc32.ChecksumIEEE(page[:]) != r.checksums[group] {
		return false, fmt.Errorf(
			"bloom group %d at offset %d: %w",
			group, offset, ErrChecksumMismatch,
		)
	}

	block := (*[BlockSize]byte)(page[blockOffset%groupSize:])
	return r.layout.mayContainBlock(block, hash), nil
}

func readFullAt(r io.ReaderAt, p []byte, offset int64) error {
	n, err := r.ReadAt(p, offset)
	switch {
	case n == len(p) && err == io.EOF:
		return nil
	case n != len(p) && (err == nil || err == io.EOF && n > 0):
		return io.ErrUnexpectedEOF
	default:
		return err
	}
}
