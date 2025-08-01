package lids

import (
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/storage"
)

type UnpackBuffer struct {
	lids    []uint32
	offsets []uint32
}

// Loader is responsible for reading from disk, unpacking and caching LID.
// NOT THREAD SAFE. Do not use concurrently.
// Use your own Loader instance for each search query
type Loader struct {
	cache     *cache.Cache[*Block]
	reader    *storage.IndexReader
	unpackBuf *UnpackBuffer
	blockBuf  []byte
}

func NewLoader(r *storage.IndexReader, c *cache.Cache[*Block]) *Loader {
	return &Loader{
		cache:     c,
		reader:    r,
		unpackBuf: &UnpackBuffer{},
	}
}

func (l *Loader) GetLIDsBlock(blockIndex uint32) (*Block, error) {
	return l.cache.GetWithError(blockIndex, func() (*Block, int, error) {
		block, err := l.readLIDsBlock(blockIndex)
		if err != nil {
			return block, 0, err
		}
		size := block.GetSizeBytes()
		return block, size, nil
	})
}

func (l *Loader) readLIDsBlock(blockIndex uint32) (*Block, error) {
	var err error
	l.blockBuf, _, err = l.reader.ReadIndexBlock(blockIndex, l.blockBuf)
	if err != nil {
		return nil, err
	}

	block := &Block{}
	err = block.Unpack(l.blockBuf, l.unpackBuf)
	if err != nil {
		return nil, err
	}

	return block, err
}
