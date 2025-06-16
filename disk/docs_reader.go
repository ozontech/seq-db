package disk

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/ozontech/seq-db/cache"
)

type DocsReader struct {
	reader DocBlocksReader
	cache  *cache.Cache[[]byte]
}

func NewDocsReader(reader *ReadLimiter, file *os.File, docsCache *cache.Cache[[]byte]) DocsReader {
	return DocsReader{
		reader: NewDocBlocksReader(reader, file),
		cache:  docsCache,
	}
}

func (r *DocsReader) ReadDocs(blockOffset uint64, docOffsets []uint64) ([][]byte, error) {
	totalSize := 0
	res := make([][]byte, 0, len(docOffsets))
	r.ReadDocsFunc(blockOffset, docOffsets, func(doc []byte) error {
		totalSize += len(doc) - 4
		res = append(res, doc[4:])
		return nil
	})
	// copy
	buf := make([]byte, 0, totalSize)
	for i, doc := range res {
		pos := len(buf)
		buf = append(buf, doc...)
		res[i] = buf[pos:]
	}
	return res, nil
}

func (r *DocsReader) ReadDocsFunc(blockOffset uint64, docOffsets []uint64, cb func(doc []byte) error) error {
	block, err := r.cache.GetWithError(uint32(blockOffset), func() ([]byte, int, error) {
		block, _, err := r.reader.ReadDocBlockPayload(int64(blockOffset))
		if err != nil {
			return nil, 0, fmt.Errorf("can't fetch doc at pos %d: %w", blockOffset, err)
		}
		return block, cap(block), nil
	})
	if err != nil {
		return err
	}
	return extractDocsFromBlockFunc(block, docOffsets, cb)
}

func extractDocsFromBlockFunc(block []byte, docOffsets []uint64, cb func([]byte) error) error {
	for _, offset := range docOffsets {
		size := int(binary.LittleEndian.Uint32(block[offset:])) + 4
		if err := cb(block[offset : offset+uint64(size)]); err != nil {
			return err
		}
	}
	return nil
}
