package disk

import (
	"os"

	"github.com/ozontech/seq-db/bytespool"
)

type DocBlocksReader struct {
	limiter *ReadLimiter
	file    *os.File
}

func NewDocBlocksReader(reader *ReadLimiter, file *os.File) DocBlocksReader {
	return DocBlocksReader{
		limiter: reader,
		file:    file,
	}
}

func (r *DocBlocksReader) getDocBlockLen(offset int64) (uint64, error) {
	buf := bytespool.Acquire(DocBlockHeaderLen)
	defer bytespool.Release(buf)

	n, err := r.limiter.ReadAt(r.file, buf.B, offset)
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
	n, err := r.limiter.ReadAt(r.file, buf, offset)

	return buf, uint64(n), err
}

func (r *DocBlocksReader) ReadDocBlockPayload(offset int64) ([]byte, uint64, error) {
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
