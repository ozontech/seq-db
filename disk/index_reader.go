package disk

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
	"go.uber.org/zap"
)

type IndexReader struct {
	file   *os.File
	cache  *cache.Cache[[]byte]
	reader *MReader
}

func NewIndexReader(cache *cache.Cache[[]byte], file *os.File, reader *MReader) *IndexReader {
	return &IndexReader{
		cache:  cache,
		file:   file,
		reader: reader,
	}
}

func (r *IndexReader) File() *os.File {
	return r.file
}

func (r *IndexReader) TryGetBlockHeader(index uint32) (RegistryEntry, error) {
	data := r.getRegistry()

	if (uint64(index)+1)*BlocksRegistryEntrySize > uint64(len(data)) {
		return nil, fmt.Errorf(
			"too large index block in file %s, with index %d, registry size %d",
			r.file.Name(),
			index,
			len(data),
		)
	}

	pos := index * BlocksRegistryEntrySize
	return data[pos : pos+BlocksRegistryEntrySize], nil
}

func (r *IndexReader) GetBlockHeader(index uint32) RegistryEntry {
	block, err := r.TryGetBlockHeader(index)
	if err != nil {
		logger.Panic("error reading block header", zap.Error(err))
	}
	return block
}

func (r *IndexReader) getRegistry() []byte {
	data, err := r.cache.GetWithError(1, func() ([]byte, int, error) {
		data, err := r.readRegistry()
		return data, cap(data), err
	})
	if err != nil {
		logger.Panic("failed to read registry", zap.Error(err))
	}

	return data
}

func (r *IndexReader) readRegistry() ([]byte, error) {
	numBuf := make([]byte, 16)
	n, err := r.reader.ReadAt(r.file, numBuf, 0)

	if err != nil {
		return nil, fmt.Errorf("can't read disk registry, %s", err.Error())
	}
	if n == 0 {
		return nil, fmt.Errorf("can't read disk registry, n=0")
	}

	pos := binary.LittleEndian.Uint64(numBuf)
	l := binary.LittleEndian.Uint64(numBuf[8:])
	buf := make([]byte, l)

	n, err = r.reader.ReadAt(r.file, buf, int64(pos))

	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("can't read disk registry, %s", err.Error())
	}

	if uint64(n) != l {
		return nil, fmt.Errorf("can't read disk registry, read=%d, requested=%d", n, l)
	}

	if len(buf)%BlocksRegistryEntrySize != 0 {
		return nil, fmt.Errorf("wrong registry format")
	}

	return buf, nil
}

func (r *IndexReader) ReadIndexBlock(blockIndex uint32, dst []byte) ([]byte, uint64, error) {
	header, err := r.TryGetBlockHeader(blockIndex)
	if err != nil {
		return nil, 0, err
	}

	if header.Codec() == CodecNo {
		dst = util.EnsureSliceSize(dst, int(header.Len()))
		n, err := r.reader.ReadAt(r.file, dst, int64(header.GetPos()))
		return dst, uint64(n), err
	}

	readBuf := bytespool.Acquire(int(header.Len()))
	defer bytespool.Release(readBuf)

	n, err := r.reader.ReadAt(r.file, readBuf.B, int64(header.GetPos()))
	if err != nil {
		return nil, uint64(n), err
	}

	dst = util.EnsureSliceSize(dst, int(header.RawLen()))
	dst, err = header.Codec().decompressBlock(int(header.RawLen()), readBuf.B, dst)

	return dst, uint64(n), err
}
