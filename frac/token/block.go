package token

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"
)

const sizeOfUint32 = uint32(unsafe.Sizeof(uint32(0)))

type Block struct {
	MinTID  uint32
	MaxTID  uint32
	Payload []byte
	Offsets []uint32
}

func (b *Block) GetSize() int {
	const selfSize = int(unsafe.Sizeof(Block{}))
	return selfSize + cap(b.Payload) + cap(b.Offsets)*int(sizeOfUint32)
}

func (b *Block) Pack() []byte {
	return b.Payload
}

func (b *Block) Unpack(data []byte) error {
	var offset uint32
	b.Payload = data
	for i := 0; len(data) != 0; i++ {
		l := binary.LittleEndian.Uint32(data)
		data = data[sizeOfUint32:]
		if l == math.MaxUint32 {
			offset += sizeOfUint32
			continue
		}
		if l > uint32(len(data)) {
			return fmt.Errorf("wrong field block for token %d, in pos %d", i, offset)
		}
		b.Offsets = append(b.Offsets, offset)
		offset += l + sizeOfUint32
		data = data[l:]
	}
	return nil
}

func (b *Block) GetToken(index uint32) []byte {
	offset := b.Offsets[index]
	l := binary.LittleEndian.Uint32(b.Payload[offset:])
	offset += sizeOfUint32 // skip val length
	return b.Payload[offset : offset+l]
}
