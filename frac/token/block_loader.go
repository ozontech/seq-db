package token

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/logger"
)

const sizeOfUint32 = uint32(unsafe.Sizeof(uint32(0)))

type Block struct {
	Payload []byte
	Offsets []uint32
}

func (b *Block) GetSize() int {
	const selfSize = int(unsafe.Sizeof(Block{}))
	return selfSize + cap(b.Payload) + cap(b.Offsets)*int(sizeOfUint32)
}

func (b *Block) Unpack(data []byte) error {
	var offset uint32
	b.Payload = data
	for i := 0; len(data) != 0; i++ {
		l := binary.LittleEndian.Uint32(data)
		data = data[sizeOfUint32:]
		offset += sizeOfUint32

		if l == math.MaxUint32 {
			continue
		}
		if l > uint32(len(data)) {
			return fmt.Errorf("wrong field block for token %d, in pos %d", i, offset)
		}
		b.Offsets = append(b.Offsets, offset-sizeOfUint32)

		data = data[l:]
		offset += l
	}
	return nil
}

func (b *Block) GetToken(index uint32) []byte {
	offset := b.Offsets[index]
	l := binary.LittleEndian.Uint32(b.Payload[offset:])
	offset += sizeOfUint32 // skip val length
	return b.Payload[offset : offset+l]
}

// BlockLoader is responsible for Reading from disk, unpacking and caching tokens blocks.
// NOT THREAD SAFE. Do not use concurrently.
// Use your own BlockLoader instance for each search query
type BlockLoader struct {
	fracName string
	cache    *cache.Cache[*Block]
	reader   *disk.IndexReader
}

func NewBlockLoader(fracName string, reader *disk.IndexReader, c *cache.Cache[*Block]) *BlockLoader {
	return &BlockLoader{
		fracName: fracName,
		cache:    c,
		reader:   reader,
	}
}

func (l *BlockLoader) Load(index uint32) *Block {
	block := l.cache.Get(index, func() (*Block, int) {
		block := l.read(index)
		return block, block.GetSize()
	})
	return block
}

func (l *BlockLoader) read(index uint32) *Block {
	data, _, err := l.reader.ReadIndexBlock(index, nil)
	if err != nil {
		logger.Panic("BUG: reading token block payload", zap.Error(err))
	}
	block := Block{}
	if err := block.Unpack(data); err != nil {
		logger.Panic("error reading tokens block",
			zap.Error(err),
			zap.Uint32("index", index),
			zap.String("frac", l.fracName),
		)
	}
	return &block
}
