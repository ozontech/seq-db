package main

import (
	"encoding/binary"
	"math"
	"unsafe"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/packer"
)

func unpackInfo(result []byte) *frac.Info {
	result = result[4:]
	info := &frac.Info{}
	info.Load(result)
	return info
}

func unpackTokens(data []byte, dst [][]byte) [][]byte {
	const sizeOfUint32 = uint32(unsafe.Sizeof(uint32(0)))
	for len(data) != 0 {
		l := binary.LittleEndian.Uint32(data)
		data = data[sizeOfUint32:]
		if l == math.MaxUint32 {
			continue
		}
		dst = append(dst, data[:l])
		data = data[l:]
	}
	return dst
}

func unpackTokenTable(data []byte, tokenTable token.Table) {
	unpacker := packer.NewBytesUnpacker(data)
	for unpacker.Len() > 0 {
		fieldName := string(unpacker.GetBinary())
		field := token.FieldData{Entries: make([]*token.TableEntry, unpacker.GetUint32())}
		entries := make([]token.TableEntry, len(field.Entries))
		for i := range field.Entries {
			e := &entries[i]
			e.StartTID = unpacker.GetUint32()
			e.ValCount = unpacker.GetUint32()
			e.StartIndex = unpacker.GetUint32()
			e.BlockIndex = unpacker.GetUint32()
			minVal := unpacker.GetBinary()
			if i == 0 {
				field.MinVal = string(minVal)
			}
			e.MaxVal = string(unpacker.GetBinary())
			field.Entries[i] = e
		}
		tokenTable[fieldName] = &field
	}
}

func unpackLIDsChunks(data []byte) *lids.Chunks {
	c := &lids.Chunks{
		LIDs:      []uint32{},
		Offsets:   []uint32{0}, // first offset is always zero
		IsLastLID: true,
	}

	var lid, offset uint32

	unpacker := packer.NewBytesUnpacker(data)

	for unpacker.Len() > 0 {
		delta, err := unpacker.GetVarint()
		if err != nil {
			logger.Fatal("error decoding LIDs block", zap.Error(err))
		}
		lid += uint32(delta)

		if lid == math.MaxUint32 { // end of LIDs of current TID, see Chunks.Pack() method
			offset = uint32(len(c.LIDs))
			c.Offsets = append(c.Offsets, offset)
			lid -= uint32(delta)
			continue
		}
		c.LIDs = append(c.LIDs, lid)
	}

	if int(offset) < len(c.LIDs) {
		c.IsLastLID = false
		c.Offsets = append(c.Offsets, uint32(len(c.LIDs)))
	}
	return c
}
