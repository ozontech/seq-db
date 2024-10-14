package lids

import (
	"time"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/packer"
)

type Stats interface {
	AddLIDBytesRead(uint64)
	AddReadLIDTimeNS(time.Duration)
	AddDecodeLIDTimeNS(time.Duration)
	AddLIDBlocksSearchTimeNS(time.Duration)
}

type unpackBuffer struct {
	lids    []uint32
	offsets []uint32
}

// Loader is responsible for reading from disk, unpacking and caching LID.
// NOT THREAD SAFE. Do not use concurrently.
// Use your own Loader instance for each search query
type Loader struct {
	cache            *cache.Cache[*Chunks]
	diskReader       *disk.Reader
	diskBlocksReader *disk.BlocksReader
	stats            Stats
	unpackBuf        *unpackBuffer
	outBuf           []byte
}

func NewLoader(
	diskReader *disk.Reader,
	diskBlocksReader *disk.BlocksReader,
	chunkCache *cache.Cache[*Chunks],
	stats Stats,
) *Loader {
	return &Loader{
		cache:            chunkCache,
		diskReader:       diskReader,
		diskBlocksReader: diskBlocksReader,
		stats:            stats,
		unpackBuf:        &unpackBuffer{},
	}
}

func (l *Loader) GetLIDsChunks(blockIndex uint32) (*Chunks, error) {
	return l.cache.GetWithError(blockIndex, func() (*Chunks, int, error) {
		chunks, err := l.readLIDsChunks(blockIndex)
		if err != nil {
			return chunks, 0, err
		}
		chunksSize := chunks.GetSizeBytes()
		return chunks, chunksSize, nil
	})
}

func (l *Loader) readLIDsChunks(blockIndex uint32) (*Chunks, error) {
	ts := time.Now()
	readTask := l.diskReader.ReadIndexBlock(l.diskBlocksReader, blockIndex, l.outBuf)
	l.outBuf = readTask.Buf
	if readTask.Err != nil {
		return nil, readTask.Err
	}
	l.stats.AddLIDBytesRead(readTask.N)
	l.stats.AddReadLIDTimeNS(time.Since(ts))

	ts = time.Now()
	chunks := &Chunks{}
	err := chunks.unpack(packer.NewBytesUnpacker(readTask.Buf), l.unpackBuf)
	if err != nil {
		return nil, err
	}
	l.stats.AddDecodeLIDTimeNS(time.Since(ts))

	return chunks, err
}
