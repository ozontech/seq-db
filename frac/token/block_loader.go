package token

import (
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/logger"
)

type entryData struct {
	block *Block
	entry *TableEntry
}

func newEntryData(entry *TableEntry, block *Block) *entryData {
	return &entryData{
		entry: entry,
		block: block,
	}
}

func (e *entryData) GetValByTID(tid uint32) []byte {
	return e.block.GetToken(e.entry.getIndexInTokensBlock(tid))
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

func (l *BlockLoader) Load(entry *TableEntry) *entryData {
	block := l.cache.Get(entry.BlockIndex, func() (*Block, int) {
		block := l.read(entry)
		size := block.GetSize()
		return block, size
	})
	return newEntryData(entry, block)
}

func (l *BlockLoader) read(entry *TableEntry) *Block {
	data, _, err := l.reader.ReadIndexBlock(entry.BlockIndex, nil)
	if err != nil {
		logger.Panic("todo: handle read err", zap.Error(err))
	}
	block := &Block{
		Offsets: make([]uint32, 0, int(entry.ValCount)), // preallocate offsets
	}
	if err := block.Unpack(data); err != nil {
		logger.Panic("error reading tokens block",
			zap.Error(err),
			zap.Any("entry", entry),
			zap.String("frac", l.fracName),
		)
	}
	return block
}
