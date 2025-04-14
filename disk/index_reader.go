package disk

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/util"
)

type IndexReader struct {
	limiter *ReadLimiter
	file    *os.File
	cache   *cache.Cache[[]byte]
}

func NewIndexReader(reader *ReadLimiter, file *os.File, registryCache *cache.Cache[[]byte]) IndexReader {
	return IndexReader{
		limiter: reader,
		file:    file,
		cache:   registryCache,
	}
}

func (r *IndexReader) GetBlockHeader(index uint32) (IndexBlockHeader, error) {
	registry, err := r.cache.GetWithError(1, func() ([]byte, int, error) {
		data, err := r.readRegistry()
		return data, cap(data), err
	})
	if err != nil {
		return nil, err
	}
	if (uint64(index)+1)*IndexBlockHeaderSize > uint64(len(registry)) {
		return nil, fmt.Errorf(
			"too large index block in file %s, with index %d, registry size %d",
			r.file.Name(),
			index,
			len(registry),
		)
	}
	pos := index * IndexBlockHeaderSize
	return registry[pos : pos+IndexBlockHeaderSize], nil
}

func (r *IndexReader) readRegistry() ([]byte, error) {
	numBuf := make([]byte, 16)

	n, err := r.limiter.ReadAt(r.file, numBuf, 0)
	if err != nil {
		return nil, fmt.Errorf("can't read disk registry, %s", err.Error())
	}
	if n == 0 {
		return nil, fmt.Errorf("can't read disk registry, n=0")
	}

	pos := binary.LittleEndian.Uint64(numBuf)
	l := binary.LittleEndian.Uint64(numBuf[8:])
	buf := make([]byte, l)

	n, err = r.limiter.ReadAt(r.file, buf, int64(pos))
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("can't read disk registry, %s", err.Error())
	}

	if uint64(n) != l {
		return nil, fmt.Errorf("can't read disk registry, read=%d, requested=%d", n, l)
	}

	if len(buf)%IndexBlockHeaderSize != 0 {
		return nil, fmt.Errorf("wrong registry format")
	}

	return buf, nil
}

func (r *IndexReader) ReadIndexBlock(blockIndex uint32, dst []byte) ([]byte, uint64, error) {
	header, err := r.GetBlockHeader(blockIndex)
	if err != nil {
		return nil, 0, err
	}

	if header.Codec() == CodecNo {
		dst = util.EnsureSliceSize(dst, int(header.Len()))
		n, err := r.limiter.ReadAt(r.file, dst, int64(header.GetPos()))
		return dst, uint64(n), err
	}

	buf := bytespool.Acquire(int(header.Len()))
	defer bytespool.Release(buf)

	n, err := r.limiter.ReadAt(r.file, buf.B, int64(header.GetPos()))
	if err != nil {
		return nil, uint64(n), err
	}

	dst = util.EnsureSliceSize(dst, int(header.RawLen()))
	dst, err = header.Codec().decompressBlock(int(header.RawLen()), buf.B, dst)

	return dst, uint64(n), err
}
