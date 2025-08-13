package token

import (
	"encoding/binary"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/packer"
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
		blocks, err := l.loadBlocks()
		if err != nil {
			return nil, 0, err
		}
		table := TableFromBlocks(blocks)
		return table, table.Size(), nil
	})
	if err != nil {
		logger.Fatal("load token table error",
			zap.String("frac", l.fracName),
			zap.Error(err))
	}
	return table
}

func TableFromBlocks(blocks []TableBlock) Table {
	table := make(Table)
	for _, block := range blocks {
		for _, ft := range block.FieldsTables {
			fd, ok := table[ft.Field]
			minVal := ft.Entries[0].MinVal
			if !ok {
				fd = &FieldData{
					MinVal:  minVal,
					Entries: make([]*TableEntry, 0, len(ft.Entries)),
				}
			} else if minVal < fd.MinVal {
				fd.MinVal = minVal
			}
			for _, e := range ft.Entries {
				e.MinVal = ""
				fd.Entries = append(fd.Entries, e)
			}
			table[ft.Field] = fd
		}
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

func (l *TableLoader) loadBlocks() ([]TableBlock, error) {
	// todo: scan all headers in sealed_loader and remember startIndex for each sections
	// todo: than use this startIndex to load sections on demand (do not scan every time)
	l.i = 1
	for h := l.readHeader(); h.Len() > 0; h = l.readHeader() { // skip actual token blocks, go for token table
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

type TableBlock struct {
	FieldsTables []FieldTable
}

type FieldTable struct {
	Field   string
	Entries []*TableEntry // expect that TableEntry are necessarily ordered by StartTID here
}

func (b TableBlock) Pack(buf []byte) []byte {
	for _, fieldData := range b.FieldsTables {
		// field name
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(fieldData.Field)))
		buf = append(buf, fieldData.Field...)

		// entries count
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(fieldData.Entries)))

		// entries
		for _, entry := range fieldData.Entries {
			buf = binary.LittleEndian.AppendUint32(buf, entry.StartTID)
			buf = binary.LittleEndian.AppendUint32(buf, entry.ValCount)
			buf = binary.LittleEndian.AppendUint32(buf, entry.StartIndex)
			buf = binary.LittleEndian.AppendUint32(buf, entry.BlockIndex)
			// MinVal
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entry.MinVal)))
			buf = append(buf, entry.MinVal...)
			// MaxVal
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entry.MaxVal)))
			buf = append(buf, entry.MaxVal...)
		}
	}
	return buf
}

func (b *TableBlock) Unpack(data []byte) {
	b.FieldsTables = make([]FieldTable, 0)
	unpacker := packer.NewBytesUnpacker(data)

	for unpacker.Len() > 0 {
		fieldName := string(unpacker.GetBinary())
		entriesCount := unpacker.GetUint32()
		ft := FieldTable{
			Field:   fieldName,
			Entries: make([]*TableEntry, entriesCount),
		}
		entries := make([]TableEntry, entriesCount)
		for i := range ft.Entries {
			e := &entries[i]
			e.StartTID = unpacker.GetUint32()
			e.ValCount = unpacker.GetUint32()
			e.StartIndex = unpacker.GetUint32()
			e.BlockIndex = unpacker.GetUint32()
			minVal := string(unpacker.GetBinary())
			maxVal := string(unpacker.GetBinary())
			if i == 0 {
				e.MinVal = minVal
			}
			e.MaxVal = maxVal
			ft.Entries[i] = e
		}
		b.FieldsTables = append(b.FieldsTables, ft)
	}
}
