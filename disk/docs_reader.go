package disk

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/cache"
)

type DocsReader struct {
	limiter *ReadLimiter
	File    *os.File
	Cache   *cache.Cache[[]byte]
}

func NewDocsReader(reader *ReadLimiter, file *os.File, docsCache *cache.Cache[[]byte]) *DocsReader {
	return &DocsReader{
		limiter: reader,
		File:    file,
		Cache:   docsCache,
	}
}

func (r *DocsReader) getDocBlockLen(offset int64) (uint64, error) {
	buf := bytespool.Acquire(DocBlockHeaderLen)
	defer bytespool.Release(buf)

	n, err := r.limiter.ReadAt(r.File, buf.B, offset)
	if err != nil {
		return uint64(n), err
	}

	return DocBlock(buf.B).FullLen(), nil
}

func (r *DocsReader) ReadDocBlock(offset int64) ([]byte, uint64, error) {
	l, err := r.getDocBlockLen(offset)
	if err != nil {
		return nil, 0, err
	}

	buf := make([]byte, l)
	n, err := r.limiter.ReadAt(r.File, buf, offset)

	return buf, uint64(n), err
}

func (r *DocsReader) ReadDocBlockPayload(offset int64) ([]byte, uint64, error) {
	l, err := r.getDocBlockLen(offset)
	if err != nil {
		return nil, 0, err
	}

	buf := bytespool.Acquire(int(l))
	defer bytespool.Release(buf)

	n, err := r.limiter.ReadAt(r.File, buf.B, offset)
	if err != nil {
		return nil, uint64(n), err
	}

	// decompress
	docBlock := DocBlock(buf.B)
	dst, err := docBlock.DecompressTo(make([]byte, docBlock.RawLen()))
	return dst, uint64(n), err
}

func (r *DocsReader) ReadDocs(blockOffset uint64, docOffsets []uint64) ([][]byte, error) {
	block, err := r.Cache.GetWithError(uint32(blockOffset), func() ([]byte, int, error) {
		block, _, err := r.ReadDocBlockPayload(int64(blockOffset))
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
