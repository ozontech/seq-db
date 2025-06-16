package token

import (
	"encoding/binary"

	"github.com/ozontech/seq-db/packer"
)

type TableBlock struct {
	FieldsTables []FieldTable
}
type FieldTable struct {
	Field   string
	Entries []*TableEntry
}

func (b *TableBlock) FillTable(table Table) {
	for _, ft := range b.FieldsTables {
		fd, ok := table[ft.Field]
		if !ok {
			fd = &FieldData{
				MinVal:  ft.Entries[0].MaxVal,
				Entries: make([]*TableEntry, 0, len(ft.Entries)),
			}
		}
		for _, e := range ft.Entries {
			e.MinVal = ""
			fd.Entries = append(fd.Entries, e)
		}
		table[ft.Field] = fd
	}
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
