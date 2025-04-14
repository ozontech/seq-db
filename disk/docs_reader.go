package disk

import (
	"encoding/binary"
	"fmt"

	"github.com/ozontech/seq-db/cache"
)

type DocsReader struct {
	reader DocBlocksReader
	cache  *cache.Cache[[]byte]
}

func NewDocsReader(br DocBlocksReader, docsCache *cache.Cache[[]byte]) DocsReader {
	return DocsReader{
		reader: br,
		cache:  docsCache,
	}
}

func (r *DocsReader) ReadDocs(blockOffset uint64, docOffsets []uint64) ([][]byte, error) {
	block, err := r.cache.GetWithError(uint32(blockOffset), func() ([]byte, int, error) {
		block, _, err := r.reader.ReadDocBlockPayload(int64(blockOffset))
		if err != nil {
			return nil, 0, fmt.Errorf("can't fetch doc at pos %d: %w", blockOffset, err)
		}
		return block, cap(block), nil
	})

	if err != nil {
		return nil, err
	}

	return extractDocsFromBlock(block, docOffsets), nil
}

func extractDocsFromBlock(block []byte, docOffsets []uint64) [][]byte {
	var totalDocsSize uint32
	docSizes := make([]uint32, len(docOffsets))
	for i, offset := range docOffsets {
		size := binary.LittleEndian.Uint32(block[offset:])
		docSizes[i] = size
		totalDocsSize += size
	}

	buf := make([]byte, 0, totalDocsSize)
	res := make([][]byte, len(docOffsets))
	for i, offset := range docOffsets {
		bufPos := len(buf)
		buf = append(buf, block[4+offset:4+offset+uint64(docSizes[i])]...)
		res[i] = buf[bufPos:]
	}

	return res
}
