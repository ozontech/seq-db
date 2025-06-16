package token

import (
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/logger"
	"go.uber.org/zap"
)

const CacheKeyTable = 1

type TableLoader struct {
	fracName string
	reader   *disk.IndexReader
	cache    *cache.Cache[Table]
	i        uint32
	buf      []byte
}

func NewTableLoader(fracName string, reader *disk.IndexReader, c *cache.Cache[Table]) *TableLoader {
	return &TableLoader{
		fracName: fracName,
		reader:   reader,
		cache:    c,
	}
}

func (l *TableLoader) Load() Table {
	table, err := l.cache.GetWithError(CacheKeyTable, func() (Table, int, error) {
		blocks, err := l.load()
		if err != nil {
			return nil, 0, err
		}
		table := make(Table)
		for _, block := range blocks {
			block.FillTable(table)
		}
		return table, table.Size(), nil
	})
	if err != nil {
		logger.Fatal("load token table error",
			zap.String("frac", l.fracName),
			zap.Error(err))
	}
	return table
}

func (l *TableLoader) readHeader() disk.IndexBlockHeader {
	h, e := l.reader.GetBlockHeader(l.i)
	if e != nil {
		logger.Panic("error reading block header", zap.Error(e))
	}
	l.i++
	return h
}

func (l *TableLoader) readBlock() ([]byte, error) {
	block, _, err := l.reader.ReadIndexBlock(l.i, l.buf)
	l.buf = block
	l.i++
	return block, err
}

func (l *TableLoader) load() ([]TableBlock, error) {
	// todo: scan all headers in sealed_loader and remember startIndex for each sections
	// todo: than use this startIndex to load sections on demand (do not scan every time)
	l.i = 1
	for h := l.readHeader(); h.Len() > 0; { // skip actual token blocks, go for token table
		h = l.readHeader()
	}

	blocks := make([]TableBlock, 0)
	for blockData, err := l.readBlock(); len(blockData) > 0; blockData, err = l.readBlock() {
		if err != nil {
			return nil, err
		}
		tb := TableBlock{}
		tb.Unpack(blockData)
		blocks = append(blocks, tb)
	}
	return blocks, nil
}
