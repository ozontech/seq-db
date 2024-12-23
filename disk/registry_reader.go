package disk

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/logger"
)

type RegistryReader struct {
	file   *os.File
	cache  *cache.Cache[[]byte]
	reader *MReader
}

func NewRegistryReader(cache *cache.Cache[[]byte], file *os.File, reader *MReader) *RegistryReader {
	return &RegistryReader{
		cache:  cache,
		file:   file,
		reader: reader,
	}
}

func (r *RegistryReader) File() *os.File {
	return r.file
}

func (r *RegistryReader) TryGetBlockHeader(index uint32) (RegistryEntry, error) {
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

func (r *RegistryReader) GetBlockHeader(index uint32) RegistryEntry {
	block, err := r.TryGetBlockHeader(index)
	if err != nil {
		logger.Panic("error reading block header", zap.Error(err))
	}
	return block
}

func (r *RegistryReader) getRegistry() []byte {
	data, err := r.cache.GetWithError(1, func() ([]byte, int, error) {
		data, err := r.readRegistry()
		return data, cap(data), err
	})
	if err != nil {
		logger.Panic("failed to read registry", zap.Error(err))
	}

	return data
}

func (r *RegistryReader) readRegistry() ([]byte, error) {
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
