package seqids

import (
	"encoding/binary"
	"errors"
	"unsafe"

	"github.com/ronanh/intcomp"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/seq"
)

type BlockMIDs struct {
	Values []uint64
}

func (b BlockMIDs) Pack(dst []byte) []byte {
	if len(b.Values) == consts.IDsPerBlock {
		dst = binary.LittleEndian.AppendUint32(dst, 1)

		_, compressed := intcomp.CompressDeltaBinPackUint64(b.Values, nil)
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(compressed)))
		for _, val := range compressed {
			dst = binary.LittleEndian.AppendUint64(dst, val)
		}
	} else {
		dst = binary.LittleEndian.AppendUint32(dst, 0)

		var prev uint64
		for _, mid := range b.Values {
			dst = binary.AppendVarint(dst, int64(mid-prev))
			prev = mid
		}
	}
	return dst
}

func (b *BlockMIDs) Unpack(data []byte, fracVersion config.BinaryDataVersion, cache *unpackCache) error {
	if fracVersion >= config.BinaryDataV3 {
		fastPath := binary.LittleEndian.Uint32(data)
		data = data[4:]

		if fastPath == 1 {
			valuesCount := binary.LittleEndian.Uint32(data)
			data = data[4:]

			// TODO this is unsafe, we rely that we running on little-endian host
			cache.compressed = cache.compressed[:valuesCount]
			byteLen := int(valuesCount) * 8
			src := unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(data[:byteLen]))), valuesCount)
			copy(cache.compressed, src)

			_, b.Values = intcomp.UncompressDeltaBinPackUint64(cache.compressed, b.Values)
			return nil
		}
	}

	values, err := unpackRawMIDsVarint(data, b.Values, fracVersion)
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

// unpackRawMIDsVarint is a dedicated method for unpacking delta encoded MIDs. The reason a dedicated method exists
// is that we want to unpack values and potentially convert legacy frac version in one pass.
func unpackRawMIDsVarint(src []byte, dst []uint64, fracVersion config.BinaryDataVersion) ([]uint64, error) {
	dst = dst[:0]
	id := uint64(0)
	for len(src) != 0 {
		udelta, n := binary.Uvarint(src)
		if n <= 0 {
			return nil, errors.New("varint decoded with error")
		}

		delta := int64(udelta >> 1)
		if udelta&1 != 0 {
			delta = ^delta
		}

		id += uint64(delta)
		if fracVersion >= config.BinaryDataV2 {
			dst = append(dst, id)
		} else {
			// Legacy format - scale millis to nanos
			dst = append(dst, uint64(seq.MillisToMID(id)))
		}

		src = src[n:]
	}
	return dst, nil
}

func unpackRawIDsVarint(src []byte, dst []uint64) ([]uint64, error) {
	dst = dst[:0]
	id := uint64(0)
	for len(src) != 0 {
		udelta, n := binary.Uvarint(src)
		if n <= 0 {
			return nil, errors.New("varint decoded with error")
		}

		delta := int64(udelta >> 1)
		if udelta&1 != 0 {
			delta = ^delta
		}

		id += uint64(delta)
		dst = append(dst, id)

		src = src[n:]
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
