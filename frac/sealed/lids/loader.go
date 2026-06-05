package lids

import (
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/storage"
)

// UnpackBuffer keeps intermediate temporary buffers for decoding. Used only for a single fraction.
type UnpackBuffer struct {
	lids         []uint32 // temp buffer (varints only) for decoding lids
	offsets      []uint32
	decompressed []uint32 // temp buffer (bitpack only) for decompressed data
	compressed   []uint32 // temp buffer (bitpack only) for compressed data (intcomp works with uint32 slices)
}

func (b *UnpackBuffer) Reset(fracVer config.BinaryDataVersion) {
	if b.offsets == nil {
		b.offsets = make([]uint32, 0, 8)
	} else {
		b.offsets = b.offsets[:0]
	}
	if fracVer >= config.BinaryDataV4 {
		if b.decompressed == nil {
			b.decompressed = make([]uint32, 0, consts.DefaultLIDBlockCap)
		} else {
			b.decompressed = b.decompressed[:0]
		}
		if b.compressed == nil {
			b.compressed = make([]uint32, 0, consts.DefaultLIDBlockCap)
		} else {
			b.compressed = b.compressed[:0]
		}
	} else {
		if b.lids == nil {
			b.lids = make([]uint32, 0, 128)
		} else {
			b.lids = b.lids[:0]
		}
	}
}

// Loader is responsible for reading from disk, unpacking and caching LID.
// NOT THREAD SAFE. Do not use concurrently.
// Use your own Loader instance for each search query
type Loader struct {
	cache     *cache.Cache[*Block]
	reader    *storage.IndexReader
	unpackBuf *UnpackBuffer
	blockBuf  []byte
	fracVer   config.BinaryDataVersion
}

func NewLoader(fracVer config.BinaryDataVersion, r *storage.IndexReader, c *cache.Cache[*Block]) *Loader {
	return &Loader{
		cache:     c,
		reader:    r,
		unpackBuf: &UnpackBuffer{},
		fracVer:   fracVer,
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
	err = block.Unpack(l.blockBuf, l.fracVer, l.unpackBuf)
	if err != nil {
		return nil, err
	}

	return block, err
}
