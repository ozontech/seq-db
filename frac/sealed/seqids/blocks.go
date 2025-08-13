package seqids

import (
	"encoding/binary"
	"errors"

	"github.com/ozontech/seq-db/config"
)

type BlockMIDs struct {
	Values []uint64
}

func (b BlockMIDs) Pack(dst []byte) []byte {
	var prev uint64
	for _, mid := range b.Values {
		dst = binary.AppendVarint(dst, int64(mid-prev))
		prev = mid
	}
	return dst
}

func (b *BlockMIDs) Unpack(data []byte) error {
	values, err := unpackRawIDsVarint(data, b.Values)
	if err != nil {
		return err
	}
	b.Values = values
	return nil
}

type BlockRIDs struct {
	fracVersion config.BinaryDataVersion
	Values      []uint64
}

func (b BlockRIDs) Pack(dst []byte) []byte {
	for _, rid := range b.Values {
		dst = binary.LittleEndian.AppendUint64(dst, rid)
	}
	return dst
}

func (b *BlockRIDs) Unpack(data []byte) error {
	if b.fracVersion < config.BinaryDataV1 {
		values, err := unpackRawIDsVarint(data, b.Values)
		if err != nil {
			return err
		}
		b.Values = values
		return nil
	}
	b.Values = unpackRawIDsNoVarint(data, b.Values)
	return nil
}

type BlockParams struct {
	Values []uint64
}

func (b BlockParams) Pack(dst []byte) []byte {
	var prev uint64
	for _, pos := range b.Values {
		dst = binary.AppendVarint(dst, int64(pos-prev))
		prev = pos
	}
	return dst
}

func (b *BlockParams) Unpack(data []byte) error {
	values, err := unpackRawIDsVarint(data, b.Values)
	if err != nil {
		return err
	}
	b.Values = values
	return nil
}

func unpackRawIDsVarint(src []byte, dst []uint64) ([]uint64, error) {
	dst = dst[:0]
	id := uint64(0)
	for len(src) != 0 {
		delta, n := binary.Varint(src)
		if n <= 0 {
			return nil, errors.New("varint decoded with error")
		}
		src = src[n:]
		id += uint64(delta)
		dst = append(dst, id)
	}
	return dst, nil
}

func unpackRawIDsNoVarint(src []byte, dst []uint64) []uint64 {
	dst = dst[:0]
	for len(src) != 0 {
		dst = append(dst, binary.LittleEndian.Uint64(src))
		src = src[8:]
	}
	return dst
}
