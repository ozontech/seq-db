package token

import (
	"encoding/binary"
	"slices"
	"sync"
	"unsafe"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/packer"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
)

const CacheKeyTable = 1

type TableLoader struct {
	fracName string
	fracVer  config.BinaryDataVersion
	isLegacy bool

	reader *storage.IndexReader
	cache  cache.Wrapper[Table]

	once       sync.Once
	tableIndex uint32

	buf []byte
}

func NewTableLoader(
	fracName string,
	fracVer config.BinaryDataVersion,
	isLegacy bool,
	reader *storage.IndexReader,
	c cache.Wrapper[Table],
) *TableLoader {
	return &TableLoader{
		fracName: fracName,
		fracVer:  fracVer,
		isLegacy: isLegacy,
		reader:   reader,
		cache:    c,
	}
}

type tableLoader TableLoader

func (s *tableLoader) Load(uint32) (Table, int, error) {
	l := (*TableLoader)(s)

	var (
		blocks []TableBlock
		err    error
	)

	l.advanceToTable()

	if l.isLegacy {
		blocks, err = l.loadBlocksLegacy()
	} else {
		blocks, err = l.loadBlocks()
	}

	if err != nil {
		return nil, 0, err
	}

	table := TableFromBlocks(blocks)
	return table, table.Size(), nil
}

func (l *TableLoader) Load() Table {
	table, err := l.cache.Get(CacheKeyTable, (*tableLoader)(l))
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

func (l *TableLoader) readHeader(idx uint32) storage.IndexBlockHeader {
	h, e := l.reader.GetBlockHeader(idx)
	if e != nil {
		logger.Panic("error reading block header", zap.Error(e))
	}
	return h
}

func (l *TableLoader) readBlock(idx uint32) ([]byte, error) {
	block, _, err := l.reader.ReadIndexBlock(idx, l.buf)
	l.buf = block
	return block, err
}

func (l *TableLoader) loadBlocksLegacy() ([]TableBlock, error) {
	blocks := make([]TableBlock, 0)

	blockIndex := l.tableIndex
	for blockData, err := l.readBlock(blockIndex); len(blockData) > 0; blockData, err = l.readBlock(blockIndex) {
		if err != nil {
			return nil, err
		}

		var tb TableBlock
		tb.Unpack(blockData, l.fracVer)

		blocks = append(blocks, tb)
		blockIndex += 1
	}

	return blocks, nil
}

func (l *TableLoader) loadBlocks() ([]TableBlock, error) {
	blocksCount, err := l.reader.BlocksCount()
	if err != nil {
		return nil, err
	}

	var blocks []TableBlock
	for blockIndex := l.tableIndex; blockIndex < uint32(blocksCount); blockIndex++ {
		data, err := l.readBlock(blockIndex)
		if err != nil {
			return nil, err
		}

		var tb TableBlock
		tb.Unpack(data, l.fracVer)

		blocks = append(blocks, tb)
	}

	return blocks, nil
}

func (l *TableLoader) advanceToTable() {
	l.once.Do(func() {
		// This is correct for both legacy and non-legacy sealed fractions:
		// 	- in legacy fractions we have following layout: [info][token][separator][token-table][...];
		//	- in non-legacy fraction we have following layout: [token][separator][token-table];
		// As you can see, it is safe to start from 0-th block in both cases.
		blockIndex := uint32(0)

		for h := l.readHeader(blockIndex); h.Len() > 0; h = l.readHeader(blockIndex) {
			// Skip token blocks, go for token table.
			blockIndex += 1
		}

		// We've stopped iterating on section separator.
		// Therefore increment is required to reach index of actual token table.
		l.tableIndex = blockIndex + 1
	})
}

// TableBlock represents how token.Table is stored on disk
type TableBlock struct {
	FieldsTables []FieldTable
}

type FieldTable struct {
	Field   string
	Entries []*TableEntry // expect that TableEntry are necessarily ordered by StartTID here
}

func (b TableBlock) packedSize() int {
	const sizeOfUint32 = int(unsafe.Sizeof(uint32(0)))
	size := 0
	for _, fieldData := range b.FieldsTables {
		// field name
		size += sizeOfUint32
		size += len(fieldData.Field)
		// entries count
		size += sizeOfUint32
		for _, entry := range fieldData.Entries {
			size += sizeOfUint32
			size += sizeOfUint32
			size += sizeOfUint32
			size += sizeOfUint32
			// MinVal
			size += sizeOfUint32
			size += len(entry.MinVal)
			// MaxVal
			size += sizeOfUint32
			size += len(entry.MaxVal)
			// Letters
			size += sizeOfUint32
		}
	}
	return size
}

func (b TableBlock) Pack(buf []byte) []byte {
	buf = slices.Grow(buf, b.packedSize())
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
			// Letters
			buf = binary.LittleEndian.AppendUint32(buf, uint32(entry.Letters))
		}
	}
	return buf
}

func (b *TableBlock) Unpack(data []byte, fracVer config.BinaryDataVersion) {
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
			if fracVer >= config.BinaryDataV5 {
				e.Letters = util.LettersBitset(unpacker.GetUint32())
			} else {
				e.Letters = util.NewLettersBitsetNil()
			}
			ft.Entries[i] = e
		}
		b.FieldsTables = append(b.FieldsTables, ft)
	}
}
