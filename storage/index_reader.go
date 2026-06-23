package storage

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/util"
)

type ReaderProvider struct {
	limiter *ReadLimiter

	reader     io.ReaderAt
	readerName string

	cache *cache.Cache[[]byte]
}

func NewReaderProvider(
	limiter *ReadLimiter,
	readerName string,
	reader io.ReaderAt,
	registryCache *cache.Cache[[]byte],
) *ReaderProvider {
	return &ReaderProvider{
		limiter:    limiter,
		readerName: readerName,
		reader:     reader,
		cache:      registryCache,
	}
}

func (r *ReaderProvider) GetReader() (IndexReader, error) {
	registry, err := r.cache.GetWithError(1, func() ([]byte, int, error) {
		data, err := r.readRegistry()
		return data, cap(data), err
	})
	if err != nil {
		return IndexReader{}, err
	}

	return NewIndexReader(r.limiter, r.readerName, r.reader, registry), nil
}

func (r *ReaderProvider) readRegistry() ([]byte, error) {
	numBuf := make([]byte, 16)

	n, err := r.limiter.ReadAt(r.reader, numBuf, 0)
	if err != nil {
		return nil, fmt.Errorf("can't read disk registry from file %s: %s", r.readerName, err.Error())
	}
	if n == 0 {
		return nil, fmt.Errorf("can't read disk registry from file %s, n=0", r.readerName)
	}

	pos := binary.LittleEndian.Uint64(numBuf)
	l := binary.LittleEndian.Uint64(numBuf[8:])
	buf := make([]byte, l)

	n, err = r.limiter.ReadAt(r.reader, buf, int64(pos))
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("can't read disk registry from file %s: %s", r.readerName, err.Error())
	}

	if uint64(n) != l {
		return nil, fmt.Errorf("can't read disk registry, read=%d, requested=%d in file %s", n, l, r.readerName)
	}

	if len(buf)%IndexBlockHeaderSize != 0 {
		return nil, fmt.Errorf("wrong registry format in file %s", r.readerName)
	}

	return buf, nil
}

type IndexReader struct {
	limiter *ReadLimiter

	reader     io.ReaderAt
	readerName string

	registry []byte
}

func NewIndexReader(
	limiter *ReadLimiter,
	readerName string,
	reader io.ReaderAt,
	registry []byte,
) IndexReader {
	return IndexReader{
		limiter:    limiter,
		reader:     reader,
		readerName: readerName,
		registry:   registry,
	}
}

func (r *IndexReader) GetBlockHeader(index uint32) (IndexBlockHeader, error) {
	if (uint64(index)+1)*IndexBlockHeaderSize > uint64(len(r.registry)) {
		return nil, fmt.Errorf(
			"too large index block in file %s, with index %d, registry size %d",
			r.readerName, index, len(r.registry),
		)
	}

	pos := index * IndexBlockHeaderSize
	return r.registry[pos : pos+IndexBlockHeaderSize], nil
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

func (r *IndexReader) BlocksCount() int {
	return len(r.registry) / IndexBlockHeaderSize
}
