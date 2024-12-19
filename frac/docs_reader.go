package frac

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/disk"
)

type DocsReader struct {
	File   *os.File
	Reader *disk.Reader
	Cache  *cache.Cache[[]byte]
}

func NewDocsReader(file *os.File, reader *disk.Reader, cache *cache.Cache[[]byte]) DocsReader {
	return DocsReader{
		File:   file,
		Reader: reader,
		Cache:  cache,
	}
}

func (f *DocsReader) Read(blockPos uint64, docPos []uint64) ([][]byte, error) {
	block, err := f.Cache.GetWithError(uint32(blockPos), func() ([]byte, int, error) {
		block, _, err := f.Reader.ReadDocBlockPayload(f.File, int64(blockPos))
		if err != nil {
			return nil, 0, fmt.Errorf("can't fetch doc at pos %d: %w", blockPos, err)
		}
		return block, cap(block), nil
	})

	if err != nil {
		return nil, err
	}

	return extractDocsFromBlock(block, docPos), nil
}

func extractDocsFromBlock(block []byte, docPos []uint64) [][]byte {
	var totalDocsSize uint32
	docSizes := make([]uint32, len(docPos))
	for i, pos := range docPos {
		size := binary.LittleEndian.Uint32(block[pos:])
		docSizes[i] = size
		totalDocsSize += size
	}

	buf := make([]byte, 0, totalDocsSize)
	res := make([][]byte, len(docPos))
	for i, pos := range docPos {
		bufPos := len(buf)
		buf = append(buf, block[4+pos:4+pos+uint64(docSizes[i])]...)
		res[i] = buf[bufPos:]
	}

	return res
}
