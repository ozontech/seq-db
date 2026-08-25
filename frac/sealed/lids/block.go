package lids

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sort"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/packer"
)

const (
	defaultLidsBitmapThreshold = math.MaxInt // bitmaps disabled by default
)

type BlockPacker struct {
	buf                 []uint32 // bitpack buffer (reusable across packing)
	bmIndexes           []uint32 // bitmap indexes (reusable across packing)
	bitpackLIDs         []uint32
	bitpackOffsets      []uint32
	bm                  *roaring.Bitmap // reusable across packing
	LidsBitmapThreshold int
}

func NewBlockPacker() *BlockPacker {
	return &BlockPacker{
		bm:                  roaring.NewBitmap(),
		buf:                 make([]uint32, 0, consts.DefaultLIDBlockCap),
		bitpackLIDs:         make([]uint32, 0, consts.DefaultLIDBlockCap),
		bitpackOffsets:      make([]uint32, 0, consts.DefaultLIDBlockCap),
		bmIndexes:           make([]uint32, 0, consts.DefaultLIDBlockCap),
		LidsBitmapThreshold: defaultLidsBitmapThreshold,
	}
}

// UnpackedBlock contains accumulated LIDs ready to pack. It's only used on sealing/compaction (index writing) time.
type UnpackedBlock struct {
	LIDs    []uint32
	Offsets []uint32
}

// Block contains LIDs in variable format. It's used during search/queries processing.
// Each posting list is either a roaring bitmap or a delta-encoded slice.
// bitmapIndexes holds the list indexes that were stored as bitmaps.
//
// On-disk format:
//
//	[bitmapsCount: uint32]   — number of lists stored as roaring bitmaps
//	[bitmaps: bitmapsCount × roaring bitmap, serial format]
//	[bitmapIndexes: delta-bitpack []uint32] — sorted list indices encoded as bitmaps
//	[offsets: delta-bitpack []uint32]     — slice boundaries in the delta-encoded LIDs array
//	[lids: delta-bitpack []uint32]        — concatenated delta-encoded LID values
type Block struct {
	lids          []uint32 // all LIDs which are delta-encoded as a flat array
	offsets       []uint32 // offsets for delta-encoded LIDs
	bitmapIndexes []uint32 // indexes of lists which are stored as bitmaps
	bitmaps       []*roaring.Bitmap
}

func (b *Block) GetCount() int {
	n := len(b.bitmapIndexes)
	if len(b.offsets) > 0 {
		n += len(b.offsets) - 1
	}
	return n
}

func (b *Block) GetLIDs(i int) node.LIDBatch {
	slot, isBitmap := b.getListSlot(i)
	if isBitmap {
		return node.NewBitmapBatch(b.bitmaps[slot])
	}
	return node.NewSliceBatch(b.lids[b.offsets[slot]:b.offsets[slot+1]])
}

func (b *Block) AppendLIDsTo(idx int, dst []uint32) []uint32 {
	slot, isBitmap := b.getListSlot(idx)
	if isBitmap {
		return b.copyLIDsFromBitmap(slot, dst)
	}
	return append(dst, b.lids[b.offsets[slot]:b.offsets[slot+1]]...)
}

// getListSlot returns either a slot into bitmaps if the corresponding list is a bitmap. Otherwise, returns
// a slot into offsets if the list is delta-encoded.
func (b *Block) getListSlot(i int) (slot int, isBitmap bool) {
	n := len(b.bitmapIndexes)
	if n == 0 {
		return i, false
	}
	slot = sort.Search(n, func(j int) bool {
		return b.bitmapIndexes[j] >= uint32(i)
	})
	if slot < n && b.bitmapIndexes[slot] == uint32(i) {
		return slot, true
	}
	return i - slot, false
}

func (b *Block) copyLIDsFromBitmap(slot int, buf []uint32) []uint32 {
	bitmap := b.bitmaps[slot]
	n := int(bitmap.GetCardinality())
	oldLen := len(buf)

	buf = slices.Grow(buf, n)[:oldLen+n]
	dest := buf[oldLen:]
	bitmap.ToExistingArray(&dest)
	return buf
}

func (b *Block) GetSizeBytes() int {
	const uint32Size = int(unsafe.Sizeof(uint32(0)))
	size := int(unsafe.Sizeof(*b)) + uint32Size*cap(b.bitmapIndexes) + uint32Size*cap(b.lids) + uint32Size*cap(b.offsets)
	for _, bm := range b.bitmaps {
		if bm != nil {
			size += int(bm.GetSizeInBytes())
		}
	}
	return size
}

func (p *BlockPacker) Pack(b *UnpackedBlock, dst []byte) []byte {
	p.buf = p.buf[:0]
	totalLists := len(b.Offsets) - 1
	bmCount := 0 // count of lid lists that will be stored as bitmaps
	bmIndexes := p.bmIndexes[:0]
	for i := 0; i < totalLists; i++ {
		if int(b.Offsets[i+1]-b.Offsets[i]) >= p.LidsBitmapThreshold {
			bmCount++
			bmIndexes = append(bmIndexes, uint32(i))
		}
	}

	var numBuf [4]byte
	binary.LittleEndian.PutUint32(numBuf[:], uint32(bmCount))
	dst = append(dst, numBuf[:]...)

	var (
		bitpackLIDs    []uint32
		bitpackOffsets []uint32
	)

	if bmCount > 0 {
		bitpackLIDs = p.bitpackLIDs[:0]
		bitpackOffsets = p.bitpackOffsets[:0]
		if bmCount < totalLists {
			bitpackOffsets = append(bitpackOffsets, 0)
		}

		for i := 0; i < totalLists; i++ {
			lids := b.LIDs[b.Offsets[i]:b.Offsets[i+1]]
			if len(lids) >= p.LidsBitmapThreshold {
				dst = p.packBitmap(dst, lids)
			} else {
				bitpackLIDs = append(bitpackLIDs, lids...)
				bitpackOffsets = append(bitpackOffsets, uint32(len(bitpackLIDs)))
			}
		}
	} else {
		bitpackLIDs = b.LIDs
		bitpackOffsets = b.Offsets
	}

	dst = packer.CompressDeltaBitpackUint32(dst, bmIndexes, p.buf)
	dst = packer.CompressDeltaBitpackUint32(dst, bitpackOffsets, p.buf)
	dst = packer.CompressDeltaBitpackUint32(dst, bitpackLIDs, p.buf)
	return dst
}

func (p *BlockPacker) packBitmap(dst []byte, lids []uint32) []byte {
	p.bm.Clear()
	p.bm.AddMany(lids)
	p.bm.RunOptimize()

	wrt := bytes.NewBuffer(dst)
	_, err := p.bm.WriteTo(wrt)
	if err != nil {
		panic(fmt.Errorf("bitmap write failed: %w", err))
	}
	return wrt.Bytes()
}

func (b *Block) Unpack(data []byte, fracVer config.BinaryDataVersion, buf *UnpackBuffer) error {
	buf.Reset(fracVer)

	if fracVer < config.BinaryDataV4 {
		return b.unpackVarintsV1(data, buf)
	}
	if fracVer < config.BinaryDataV6 {
		return b.unpackBitpackV4(data, buf)
	}

	return b.unpackBlockV6(data, buf)
}

// unpackBlockV6 unpacks the mixed bitmap / delta-bitpack format (BinaryDataV6+).
func (b *Block) unpackBlockV6(data []byte, buf *UnpackBuffer) error {
	bitmapCount := int(binary.LittleEndian.Uint32(data[:4]))
	data = data[4:]

	bitmaps := make([]*roaring.Bitmap, bitmapCount)
	for i := 0; i < bitmapCount; i++ {
		rb := roaring.NewBitmap()
		n, err := rb.ReadFrom(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("read bitmap %d: %w", i, err)
		}
		data = data[n:]
		bitmaps[i] = rb
	}

	var (
		err           error
		bitmapIndexes []uint32
	)
	data, bitmapIndexes, err = packer.DecompressDeltaBitpackUint32(data, buf.decompressed, buf.compressed)
	if err != nil {
		return err
	}
	b.bitmapIndexes = append([]uint32{}, bitmapIndexes...)

	var values []uint32
	data, values, err = packer.DecompressDeltaBitpackUint32(data, buf.decompressed, buf.compressed)
	if err != nil {
		return err
	}
	offsets := append([]uint32{}, values...)

	_, values, err = packer.DecompressDeltaBitpackUint32(data, buf.decompressed, buf.compressed)
	if err != nil {
		return err
	}
	lids := append([]uint32{}, values...)

	b.lids = lids
	b.offsets = offsets
	b.bitmaps = bitmaps
	return nil
}

func (b *Block) unpackBitpackV4(data []byte, buf *UnpackBuffer) error {
	var err error
	var values []uint32

	data, values, err = packer.DecompressDeltaBitpackUint32(data, buf.decompressed, buf.compressed)
	if err != nil {
		return err
	}
	offsets := append([]uint32{}, values...)

	_, values, err = packer.DecompressDeltaBitpackUint32(data, buf.decompressed, buf.compressed)
	if err != nil {
		return err
	}
	lids := append([]uint32{}, values...)

	b.bitmapIndexes = nil
	b.lids = lids
	b.offsets = offsets
	b.bitmaps = nil
	return nil
}

func (b *Block) unpackVarintsV1(data []byte, buf *UnpackBuffer) error {
	var lid, offset uint32
	buf.offsets = append(buf.offsets, 0) // first offset is always zero

	unpacker := packer.NewBytesUnpacker(data)
	for unpacker.Len() > 0 {
		delta, err := unpacker.GetVarint()
		if err != nil {
			return err
		}
		lid += uint32(delta)

		if lid == math.MaxUint32 {
			offset = uint32(len(buf.lids))
			buf.offsets = append(buf.offsets, offset)
			lid -= uint32(delta)
			continue
		}

		buf.lids = append(buf.lids, lid)
	}

	if int(offset) < len(buf.lids) {
		buf.offsets = append(buf.offsets, uint32(len(buf.lids)))
	}

	b.bitmapIndexes = nil
	b.lids = append([]uint32{}, buf.lids...)
	b.offsets = append([]uint32{}, buf.offsets...)
	b.bitmaps = nil
	return nil
}
