package storage

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ozontech/seq-db/cache"
)

type DocsReader struct {
	reader DocBlocksReader
	cache  *cache.Cache[[]byte]
}

func NewDocsReader(limiter *ReadLimiter, reader io.ReaderAt, docsCache *cache.Cache[[]byte]) DocsReader {
	return DocsReader{
		reader: NewDocBlocksReader(limiter, reader),
		cache:  docsCache,
	}
}

func (r *DocsReader) ReadDocs(blockOffset uint64, docOffsets []uint64) ([][]byte, error) {
	bufSize := 0
	res := make([][]byte, 0, len(docOffsets))
	err := r.ReadDocsFunc(blockOffset, docOffsets, func(doc []byte) error {
		bufSize += len(doc)
		res = append(res, doc)
		return nil
	})
	if err != nil {
		return nil, err
	}
	// copy so as not to keep the entire block in memory
	buf := make([]byte, 0, bufSize)
	for i, doc := range res {
		pos := len(buf)
		buf = append(buf, doc...)
		res[i] = buf[pos:]
	}
	return res, nil
}

func (r *DocsReader) ReadDocsFunc(blockOffset uint64, docOffsets []uint64, cb func([]byte) error) error {
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
		size := binary.LittleEndian.Uint32(block[offset:])
		docStart := offset + 4
		docEnd := docStart + uint64(size)
		doc := block[docStart:docEnd]
		if err := cb(doc); err != nil {
			return err
		}
	}
	return nil
}
