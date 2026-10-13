package storage

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ozontech/seq-db/cache"
)

type DocsReader struct {
	reader       DocBlocksReader
	cache        *cache.ConcurrentCache[[]byte]
	blockOffsets []uint64
}

func NewDocsReader(
	limiter *ReadLimiter,
	reader io.ReaderAt,
	docsCache *cache.ConcurrentCache[[]byte],
	blockOffsets []uint64,
) DocsReader {
	return DocsReader{
		reader:       NewDocBlocksReader(limiter, reader),
		cache:        docsCache,
		blockOffsets: blockOffsets,
	}
}

func (r *DocsReader) ReadDocs(blockIndex uint32, docOffsets []uint64) ([][]byte, error) {
	bufSize := 0
	res := make([][]byte, 0, len(docOffsets))
	err := r.ReadDocsFunc(blockIndex, docOffsets, func(doc []byte) error {
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

func (r *DocsReader) Load(blockIndex uint32) ([]byte, int, error) {
	if uint64(blockIndex) >= uint64(len(r.blockOffsets)) {
		return nil, 0, fmt.Errorf(
			"doc block index %d is out of range [0, %d)",
			blockIndex, len(r.blockOffsets),
		)
	}

	blockOffset := r.blockOffsets[blockIndex]
	block, _, err := r.reader.ReadDocBlockPayload(int64(blockOffset))
	if err != nil {
		return nil, 0, fmt.Errorf("can't fetch doc at pos %d: %w", blockOffset, err)
	}

	return block, cap(block), nil
}

func (r *DocsReader) ReadDocsFunc(blockIndex uint32, docOffsets []uint64, cb func([]byte) error) error {
	block, err := r.cache.Get(blockIndex, r)
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
