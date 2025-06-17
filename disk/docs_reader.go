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
	file    *os.File
	cache   *cache.Cache[[]byte]
}

func NewDocsReader(reader *ReadLimiter, file *os.File, docsCache *cache.Cache[[]byte]) *DocsReader {
	return &DocsReader{
		limiter: reader,
		file:    file,
		cache:   docsCache,
	}
}

func (r *DocsReader) getDocBlockLen(offset int64) (uint64, error) {
	buf := bytespool.Acquire(DocBlockHeaderLen)
	defer bytespool.Release(buf)

	n, err := r.limiter.ReadAt(r.file, buf.B, offset)
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
	n, err := r.limiter.ReadAt(r.file, buf, offset)

	return buf, uint64(n), err
}

func (r *DocsReader) ReadDocBlockPayload(offset int64) ([]byte, uint64, error) {
	l, err := r.getDocBlockLen(offset)
	if err != nil {
		return nil, 0, err
	}

	buf := bytespool.Acquire(int(l))
	defer bytespool.Release(buf)

	n, err := r.limiter.ReadAt(r.file, buf.B, offset)
	if err != nil {
		return nil, uint64(n), err
	}

	// decompress
	docBlock := DocBlock(buf.B)
	dst, err := docBlock.DecompressTo(make([]byte, docBlock.RawLen()))
	return dst, uint64(n), err
}

func (r *DocsReader) ReadDocs(blockOffset uint64, docOffsets []uint64) ([][]byte, error) {
	totalSize := 0
	res := make([][]byte, 0, len(docOffsets))
	r.ReadDocsFunc(blockOffset, docOffsets, func(doc []byte) error {
		totalSize += len(doc)
		res = append(res, doc)
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

func (r *DocsReader) ReadDocsFunc(blockOffset uint64, docOffsets []uint64, cb func([]byte) error) error {
	block, err := r.cache.GetWithError(uint32(blockOffset), func() ([]byte, int, error) {
		block, _, err := r.ReadDocBlockPayload(int64(blockOffset))
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
