package storage

import (
	"io"

	"github.com/ozontech/seq-db/bytespool"
)

type DocBlocksReader struct {
	limiter *ReadLimiter
	reader  io.ReaderAt
}

func NewDocBlocksReader(limiter *ReadLimiter, reader io.ReaderAt) DocBlocksReader {
	return DocBlocksReader{
		limiter: limiter,
		reader:  reader,
	}
}

func (r *DocBlocksReader) getDocBlockLen(offset int64) (uint64, error) {
	buf := bytespool.AcquireLen(DocBlockHeaderLen)
	defer bytespool.Release(buf)

	n, err := r.limiter.ReadAt(r.reader, buf.B, offset)
	if err != nil {
		return uint64(n), err
	}

	return DocBlock(buf.B).FullLen(), nil
}

func (r *DocBlocksReader) ReadDocBlock(offset int64) ([]byte, uint64, error) {
	l, err := r.getDocBlockLen(offset)
	if err != nil {
		return nil, 0, err
	}

	buf := make([]byte, l)
	n, err := r.limiter.ReadAt(r.reader, buf, offset)

	return buf, uint64(n), err
}

func (r *DocBlocksReader) ReadDocBlockPayload(offset int64) ([]byte, uint64, error) {
	l, err := r.getDocBlockLen(offset)
	if err != nil {
		return nil, 0, err
	}

	buf := bytespool.AcquireLen(int(l))
	defer bytespool.Release(buf)

	n, err := r.limiter.ReadAt(r.reader, buf.B, offset)
	if err != nil {
		return nil, uint64(n), err
	}

	// decompress
	docBlock := DocBlock(buf.B)
	dst, err := docBlock.DecompressTo(make([]byte, docBlock.RawLen()))
	return dst, uint64(n), err
}
