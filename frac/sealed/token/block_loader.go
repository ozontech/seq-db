package token

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/storage"
)

const sizeOfUint32 = uint32(unsafe.Sizeof(uint32(0)))

type Block struct {
	Payload []byte
	Offsets []uint32
}

func (b *Block) Size() int {
	const selfSize = int(unsafe.Sizeof(Block{}))
	return selfSize + cap(b.Payload) + cap(b.Offsets)*int(sizeOfUint32)
}

func (b Block) Pack(dst []byte) []byte {
	return append(dst, b.Payload...)
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

func (b *Block) Len() int {
	return len(b.Offsets)
}

func (b *Block) GetToken(index int) []byte {
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
	reader   *storage.IndexReader
}

func NewBlockLoader(fracName string, reader *storage.IndexReader, c *cache.Cache[*Block]) *BlockLoader {
	return &BlockLoader{
		fracName: fracName,
		cache:    c,
		reader:   reader,
	}
}

func (l *BlockLoader) Load(index uint32) *Block {
	block := l.cache.Get(index, func() (*Block, int) {
		block, err := l.read(index)
		if err != nil {
			logger.Panic("error reading tokens block", // todo: get rid of panic here
				zap.Error(err),
				zap.Any("index", index),
				zap.String("frac", l.fracName),
			)
		}
		size := block.Size()
		return block, size
	})
	return block
}

func (l *BlockLoader) read(index uint32) (*Block, error) {
	data, _, err := l.reader.ReadIndexBlock(index, nil)
	if err != nil {
		return nil, err
	}
	block := Block{}
	err = block.Unpack(data)
	return &block, err
}
