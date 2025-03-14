package lids

import (
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/packer"
)

type unpackBuffer struct {
	lids    []uint32
	offsets []uint32
}

// Loader is responsible for reading from disk, unpacking and caching LID.
// NOT THREAD SAFE. Do not use concurrently.
// Use your own Loader instance for each search query
type Loader struct {
	cache     *cache.Cache[*Chunks]
	reader    *disk.IndexReader
	unpackBuf *unpackBuffer
	blockBuf  []byte
}

func NewLoader(reader *disk.IndexReader, chunkCache *cache.Cache[*Chunks]) *Loader {
	return &Loader{
		cache:     chunkCache,
		reader:    reader,
		unpackBuf: &unpackBuffer{},
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
	var err error
	l.blockBuf, _, err = l.reader.ReadIndexBlock(blockIndex, l.blockBuf)
	if err != nil {
		return nil, err
	}

	chunks := &Chunks{}
	err = chunks.unpack(packer.NewBytesUnpacker(l.blockBuf), l.unpackBuf)
	if err != nil {
		return nil, err
	}

	return chunks, err
}
