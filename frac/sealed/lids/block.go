package lids

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
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
	LIDs      []uint32
	Offsets   []uint32
	IsLastLID bool
}

// Block contains LIDs in variable format. It's used during search/queries processing.
// Field types is used to distinguish format for every LID list. If it's positive or zero, then it's a slot index in offsets
// (delta-encoding is used). If it's negative, then it's a bitmap slot index (index starts from -1, so -1 stands for bitmaps[0]).
// If types is nil, then the entire block is delta-encoded.
//
// On-disk format:
//
//	[listsCount: uint32]     — number of LID lists in the block
//	[bitmapsCount: uint32]   — number of lists stored as roaring bitmaps
//	[bitmaps: bitmapsCount × roaring bitmap, serial format]
//	[bitmapIndexes: delta-bitpack []uint32] — sorted list indices encoded as bitmaps
//	[offsets: delta-bitpack []uint32]     — slice boundaries in the delta-encoded LIDs array
//	[lids: delta-bitpack []uint32]        — concatenated delta-encoded LID values
//
// Each list i in [0, listsCount) is either a roaring bitmap (when i appears in bitmapIndexes)
// or a delta-encoded slice lids[offsets[k]:offsets[k+1]], where k is its delta-encoded slot index.
// Lists with length >= LidsBitmapThreshold are stored as bitmaps; shorter lists use delta-encoding.
type Block struct {
	types   []int32           // determines LID list type: delta-encoded (non-negative value) or bitmap (negative value). nil for delta-encoded blocks
	lids    []uint32          // all LIDs which are delta-encoded as a flat array
	offsets []uint32          // offsets for delta-encoded LIDs
	bitmaps []*roaring.Bitmap // all LIDs lists which are stored as bitmaps
	lastLID bool              // legacy field, will be removed soon
}

func (b *Block) GetCount() int {
	if b.types != nil {
		return len(b.types)
	}

	return len(b.offsets) - 1
}

func (b *Block) IsLastLID() bool {
	return b.lastLID
}

func (b *Block) GetLIDs(i int) node.LIDBatch {
	if b.types == nil {
		return node.NewSliceBatch(b.lids[b.offsets[i]:b.offsets[i+1]])
	}
	t := b.types[i]
	if t >= 0 {
		return node.NewSliceBatch(b.lids[b.offsets[t]:b.offsets[t+1]])
	}
	return node.NewBitmapBatch(b.bitmaps[-t-1])
}

func (b *Block) CopyLIDs(idx int, dst []uint32) []uint32 {
	if b.types == nil {
		dst = append(dst, b.lids[b.offsets[idx]:b.offsets[idx+1]]...)
		return dst
	}
	t := b.types[idx]
	if t >= 0 {
		dst = append(dst, b.lids[b.offsets[t]:b.offsets[t+1]]...)
		return dst
	}
	return b.copyLIDsFromBitmap(t, dst)
}

func (b *Block) copyLIDsFromBitmap(ref int32, buf []uint32) []uint32 {
	bitmap := b.bitmaps[-ref-1]
	n := int(bitmap.GetCardinality())
	oldLen := len(buf)

	buf = slices.Grow(buf, n)[:oldLen+n]
	dest := buf[oldLen:]
	bitmap.ToExistingArray(&dest)
	return buf
}

func (b *Block) GetSizeBytes() int {
	const uint32Size = int(unsafe.Sizeof(uint32(0)))
	size := int(unsafe.Sizeof(*b)) + uint32Size*cap(b.types) + uint32Size*cap(b.lids) + uint32Size*cap(b.offsets)
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

	// write total number of LID lists and bitmap indexes
	var numBuf [4]byte
	binary.LittleEndian.PutUint32(numBuf[:], uint32(totalLists))
	dst = append(dst, numBuf[:]...)
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
	listsCount := int(binary.LittleEndian.Uint32(data[:4]))
	data = data[4:]
	bitmapsCount := int(binary.LittleEndian.Uint32(data[:4]))
	data = data[4:]

	bitmaps := make([]*roaring.Bitmap, bitmapsCount)
	for i := 0; i < bitmapsCount; i++ {
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
	b.types = deriveTypes(listsCount, bitmapIndexes)

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

// deriveTypes derives types array (only for LID blocks which have at least one bitmap).
func deriveTypes(totalLists int, bitmapIndexes []uint32) []int32 {
	if len(bitmapIndexes) == 0 {
		return nil
	}
	listTypes := make([]int32, totalLists)
	bmIdx := 0
	deltaIdx := 0
	bmSlotIdx := 0
	for i := 0; i < totalLists; i++ {
		if bmSlotIdx < len(bitmapIndexes) && bitmapIndexes[bmSlotIdx] == uint32(i) {
			listTypes[i] = -int32(bmIdx + 1)
			bmIdx++
			bmSlotIdx++
		} else {
			listTypes[i] = int32(deltaIdx)
			deltaIdx++
		}
	}
	return listTypes
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

	b.types = nil
	b.lids = lids
	b.offsets = offsets
	b.bitmaps = nil
	b.lastLID = false
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

	lastLID := true
	if int(offset) < len(buf.lids) {
		buf.offsets = append(buf.offsets, uint32(len(buf.lids)))
	}

	b.types = nil
	b.lids = append([]uint32{}, buf.lids...)
	b.offsets = append([]uint32{}, buf.offsets...)
	b.bitmaps = nil
	b.lastLID = lastLID
	return nil
}
