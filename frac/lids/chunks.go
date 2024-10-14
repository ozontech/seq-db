package lids

import (
	"math"
	"unsafe"

	"github.com/ozontech/seq-db/packer"
)

type Chunks struct {
	LIDs    []uint32
	Offsets []uint32
	// todo remove this legacy field
	IsLastLID bool
}

func (c *Chunks) getCount() int {
	return len(c.Offsets) - 1
}

func (c *Chunks) getLIDs(i int) []uint32 {
	return c.LIDs[c.Offsets[i]:c.Offsets[i+1]]
}

func (c *Chunks) Pack(b *packer.BytesPacker) {
	lastLID := int64(0)
	last := c.getCount() - 1

	for i := 0; i <= last; i++ {
		for _, lid := range c.getLIDs(i) {
			b.PutVarint(int64(lid) - lastLID)
			lastLID = int64(lid)
		}

		if i < last || c.IsLastLID {
			// when we add this value to prev we must get -1 (or math.MaxUint32 for uint32)
			// it is the end-marker; see Chunks.unpack()
			b.PutVarint(-1 - lastLID)
		}
	}
}

func (c *Chunks) GetSizeBytes() int {
	const (
		uint32Size = int(unsafe.Sizeof(uint32(0)))
		chunksSize = int(unsafe.Sizeof(*c))
	)
	return chunksSize + uint32Size*len(c.LIDs) + uint32Size*len(c.Offsets)
}

func (c *Chunks) unpack(data *packer.BytesUnpacker, buf *unpackBuffer) error {
	var lid, offset uint32

	c.IsLastLID = true

	buf.lids = buf.lids[:0]
	buf.offsets = buf.offsets[:0]
	buf.offsets = append(buf.offsets, 0) // first offset is always zero

	for data.Len() > 0 {
		delta, err := data.GetVarint()
		if err != nil {
			return err
		}
		lid += uint32(delta)

		if lid == math.MaxUint32 { // end of LIDs of current TID, see Chunks.Pack() method
			offset = uint32(len(buf.lids))
			buf.offsets = append(buf.offsets, offset)
			lid -= uint32(delta)
			continue
		}

		buf.lids = append(buf.lids, lid)
	}

	if int(offset) < len(buf.lids) {
		c.IsLastLID = false
		buf.offsets = append(buf.offsets, uint32(len(buf.lids)))
	}

	// copy from buffer
	c.LIDs = append([]uint32{}, buf.lids...)
	c.Offsets = append([]uint32{}, buf.offsets...)

	return nil
}
