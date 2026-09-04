package storage

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/util"
)

const registryCacheKey = 1

type IndexReader struct {
	limiter *ReadLimiter

	reader     io.ReaderAt
	readerName string

	cache cache.Cache[[]byte]
}

func NewIndexReader(
	limiter *ReadLimiter, readerName string,
	reader io.ReaderAt, registryCache cache.Cache[[]byte],
) IndexReader {
	return IndexReader{
		limiter:    limiter,
		reader:     reader,
		readerName: readerName,
		cache:      registryCache,
	}
}

type registryLoader IndexReader

func (rl *registryLoader) Load(uint32) ([]byte, int, error) {
	prefix := make([]byte, 16)

	n, err := rl.limiter.ReadAt(rl.reader, prefix, 0)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"can't read disk registry from file %s: %s",
			rl.readerName, err.Error(),
		)
	}

	if n == 0 {
		return nil, 0, fmt.Errorf(
			"can't read disk registry from file %s, n=0",
			rl.readerName,
		)
	}

	pos := binary.LittleEndian.Uint64(prefix)
	size := binary.LittleEndian.Uint64(prefix[8:])

	buf := make([]byte, size)
	n, err = rl.limiter.ReadAt(rl.reader, buf, int64(pos))
	if err != nil && err != io.EOF {
		return nil, 0, fmt.Errorf(
			"can't read disk registry from file %s: %s",
			rl.readerName, err.Error(),
		)
	}

	if uint64(n) != size {
		return nil, 0, fmt.Errorf(
			"can't read disk registry from file %s: read=%d, requested=%d",
			rl.readerName, n, size,
		)
	}

	if len(buf)%IndexBlockHeaderSize != 0 {
		return nil, 0, fmt.Errorf(
			"cannot read disk registry from file %s: wrong registry format",
			rl.readerName,
		)
	}

	return buf, cap(buf), nil
}

func (r *IndexReader) registry() ([]byte, error) {
	return r.cache.Get(registryCacheKey, (*registryLoader)(r))
}

func (r *IndexReader) GetBlockHeader(index uint32) (IndexBlockHeader, error) {
	reg, err := r.registry()
	if err != nil {
		return nil, err
	}

	if (uint64(index)+1)*IndexBlockHeaderSize > uint64(len(reg)) {
		return nil, fmt.Errorf(
			"too large index block in file %s, with index %d, registry size %d",
			r.readerName, index, len(reg),
		)
	}

	pos := index * IndexBlockHeaderSize
	return reg[pos : pos+IndexBlockHeaderSize], nil
}

func (r *IndexReader) ReadIndexBlock(blockIndex uint32, dst []byte) ([]byte, uint64, error) {
	header, err := r.GetBlockHeader(blockIndex)
	if err != nil {
		return nil, 0, err
	}

	if header.Codec() == CodecNo {
		dst = util.EnsureSliceSize(dst, int(header.Len()))
		n, err := r.limiter.ReadAt(r.reader, dst, int64(header.GetPos()))
		return dst, uint64(n), err
	}

	buf := bytespool.AcquireLen(int(header.Len()))
	defer bytespool.Release(buf)

	n, err := r.limiter.ReadAt(r.reader, buf.B, int64(header.GetPos()))
	if err != nil {
		return nil, uint64(n), err
	}

	dst = util.EnsureSliceSize(dst, int(header.RawLen()))
	dst, err = header.Codec().decompressBlock(int(header.RawLen()), buf.B, dst)

	return dst, uint64(n), err
}

func (r *IndexReader) BlocksCount() (int, error) {
	reg, err := r.registry()
	if err != nil {
		return 0, err
	}

	return len(reg) / IndexBlockHeaderSize, nil
}
