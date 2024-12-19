package token

import (
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/packer"
	"go.uber.org/zap"
)

const CacheKeyTable = 1

type TableLoader struct {
	fracName string
	cache    *cache.Cache[Table]
	reader   *disk.Reader
	br       *disk.BlocksReader
	i        uint32
	buf      []byte
}

func NewTableLoader(fracName string, reader *disk.Reader, br *disk.BlocksReader, c *cache.Cache[Table]) *TableLoader {
	return &TableLoader{
		fracName: fracName,
		cache:    c,
		reader:   reader,
		br:       br,
	}
}

func (l *TableLoader) Load() Table {
	table, err := l.cache.GetWithError(CacheKeyTable, l.load)
	if err != nil {
		logger.Fatal("load token table error",
			zap.String("frac", l.fracName),
			zap.Error(err))
	}
	return table
}

func (l *TableLoader) readHeader() disk.BlocksRegistryEntry {
	h := l.br.GetBlockHeader(l.i)
	l.i++
	return h
}

func (l *TableLoader) readBlock() ([]byte, error) {
	block, _, err := l.reader.ReadIndexBlock(l.br, l.i, l.buf)
	l.buf = block
	l.i++
	return block, err
}

func (l *TableLoader) load() (Table, int, error) {
	// todo: scan all headers in sealed_loader and remember startIndex for each sections
	// todo: than use this startIndex to load sections on demand (do not scan every time)
	l.i = 1
	for h := l.readHeader(); h.Len() > 0; { // skip actual token blocks, go for token table
		h = l.readHeader()
	}

	size := 0
	tokenTable := make(map[string]*FieldData)
	for block, err := l.readBlock(); len(block) > 0; block, err = l.readBlock() {
		if err != nil {
			return nil, 0, err
		}

		unpacker := packer.NewBytesUnpacker(block)
		for unpacker.Len() > 0 {
			fieldName := string(unpacker.GetBinary())
			field := FieldData{Entries: make([]*TableEntry, unpacker.GetUint32())}
			entries := make([]TableEntry, len(field.Entries))
			for i := range field.Entries {
				e := &entries[i]
				e.StartTID = unpacker.GetUint32()
				e.ValCount = unpacker.GetUint32()
				e.StartIndex = unpacker.GetUint32() // todo: it seems we can calculate this field using valCount but not store startIndex on disk
				e.BlockIndex = unpacker.GetUint32()
				minVal := unpacker.GetBinary()
				if i == 0 {
					field.MinVal = string(minVal)
				}
				e.MaxVal = string(unpacker.GetBinary())
				field.Entries[i] = e
				size += len(e.MaxVal)
			}
			tokenTable[fieldName] = &field
			size += len(fieldName) + len(entries)*int(TableEntrySize) + len(field.MinVal)
		}
	}
	size += len(tokenTable) * int(FieldDataSize)

	return tokenTable, size, nil
}
