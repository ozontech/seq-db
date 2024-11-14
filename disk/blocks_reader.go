package disk

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/logger"
)

type BlocksReader struct {
	file       *os.File
	cache      *cache.Cache[[]byte]
	fileName   string
	fileMu     sync.Mutex
	readMetric prometheus.Counter
}

func NewBlocksReader(c *cache.Cache[[]byte], fileName string, readMetric prometheus.Counter) *BlocksReader {
	return &BlocksReader{
		cache:      c,
		fileName:   fileName,
		readMetric: readMetric,
	}
}

func (r *BlocksReader) GetFileName() string {
	return r.fileName
}

// GetFileStat only used during loading
func (r *BlocksReader) GetFileStat() (os.FileInfo, error) {
	r.fileMu.Lock()
	defer r.fileMu.Unlock()

	return r.file.Stat()
}

func (r *BlocksReader) GetBlockHeader(index uint32) BlocksRegistryEntry {
	data := r.getRegistry()

	if (uint64(index)+1)*BlocksRegistryEntrySize > uint64(len(data)) {
		logger.Panic(
			"too large block index",
			zap.Uint32("index", index),
			zap.String("file", r.fileName),
			zap.Int("data_len", len(data)),
		)
	}

	pos := index * BlocksRegistryEntrySize
	return data[pos : pos+BlocksRegistryEntrySize]
}

func (r *BlocksReader) getRegistry() []byte {
	data, err := r.cache.GetWithError(1, func() ([]byte, int, error) {
		data, err := r.readRegistry()
		return data, cap(data), err
	})
	if err != nil {
		logger.Panic("failed to read registry", zap.Error(err))
	}

	return data
}

func (r *BlocksReader) tryOpenFile() *os.File {
	r.fileMu.Lock()
	defer r.fileMu.Unlock()

	if r.file != nil {
		return r.file
	}

	file, err := os.Open(r.fileName)
	if err != nil {
		// give hdd a second chance
		file, err = os.Open(r.fileName)
		if err != nil {
			logger.Panic("can't open file by blocks reader",
				zap.String("file", r.fileName),
				zap.Error(err),
			)
		}
	}

	r.file = file
	return file
}

func (r *BlocksReader) reportReadBytes(n int) {
	if r.readMetric != nil {
		r.readMetric.Add(float64(n))
	}
}

func (r *BlocksReader) readRegistry() ([]byte, error) {
	file := r.tryOpenFile()

	numBuf := make([]byte, 16)
	n, err := file.ReadAt(numBuf, 0)
	r.reportReadBytes(n)

	if err != nil {
		return nil, fmt.Errorf("can't read disk registry, %s", err.Error())
	}
	if n == 0 {
		return nil, fmt.Errorf("can't read disk registry, n=0")
	}

	pos := binary.LittleEndian.Uint64(numBuf)
	l := binary.LittleEndian.Uint64(numBuf[8:])
	buf := make([]byte, l)

	n, err = file.ReadAt(buf, int64(pos))
	r.reportReadBytes(n)

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

// Close only used in frac.Sealed.Suicide under frac.Sealed.loadMu
func (r *BlocksReader) Close() error {
	r.fileMu.Lock()
	defer r.fileMu.Unlock()

	file := r.file
	r.file = nil
	return file.Close()
}
