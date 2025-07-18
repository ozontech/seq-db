package frac

import (
	"io"
	"time"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type DiskBlocksWriter struct {
	buf    []byte
	writer *disk.BlocksWriter
	stats  disk.SealingStats

	startOfIDsBlockIndex uint32
}

func NewSealedBlockWriter(ws io.WriteSeeker) *DiskBlocksWriter {
	return &DiskBlocksWriter{
		writer: disk.NewBlocksWriter(ws),
		stats:  make(disk.SealingStats, 0),
	}
}

func (w *DiskBlocksWriter) resetBuf(size int) []byte {
	w.buf = util.EnsureSliceSize(w.buf, size)[:0]
	return w.buf
}

func (w *DiskBlocksWriter) NewBlockFormer(name string, size int) *disk.BlockFormer {
	return disk.NewBlockFormer(name, w.writer, size, w.resetBuf(size))
}

func (w *DiskBlocksWriter) writeInfoBlock(block *BlockInfo) error {
	now := time.Now()
	w.buf = block.Pack(w.resetBuf(consts.RegularBlockSize))
	n, err := w.writer.WriteBlock("info", w.buf, false, 0, 0, 0)
	if err != nil {
		return err
	}

	w.stats = append(w.stats, &disk.BlockStats{
		Name:     "info",
		Raw:      uint64(len(w.buf)),
		Comp:     uint64(n),
		Blocks:   1,
		Duration: time.Since(now),
	})

	return nil
}

func (w *DiskBlocksWriter) writePositionsBlock(zstdCompressLevel int, block *DiskPositionsBlock) error {
	now := time.Now()
	w.buf = block.pack(w.resetBuf(consts.IDsBlockSize))
	n, err := w.writer.WriteBlock("positions", w.buf, true, zstdCompressLevel, 0, 0)
	if err != nil {
		return err
	}

	w.stats = append(w.stats, &disk.BlockStats{
		Name:     "positions",
		Raw:      uint64(len(w.buf)),
		Comp:     uint64(n),
		Blocks:   1,
		Duration: time.Since(now),
	})

	return nil
}

func (w *DiskBlocksWriter) writeIDsBlocks(zstdLevel int, generateBlocks func(func(*DiskIDsBlock) error) error) ([]seq.ID, error) {
	w.startOfIDsBlockIndex = w.writer.GetBlockIndex()

	levelOpt := disk.WithZstdCompressLevel(zstdLevel)

	former := w.NewBlockFormer("ids", consts.IDsBlockSize)

	minBlockIDs := make([]seq.ID, 0)

	push := func(block *DiskIDsBlock) error {
		former.Buf = block.packMIDs(former.Buf)
		if err := former.FlushForced(disk.WithExt(block.getExtForRegistry()), levelOpt); err != nil {
			return err
		}

		former.Buf = block.packRIDs(former.Buf)
		if err := former.FlushForced(levelOpt); err != nil {
			return err
		}

		former.Buf = block.packPos(former.Buf)
		if err := former.FlushForced(levelOpt); err != nil {
			return err
		}

		minBlockIDs = append(minBlockIDs, block.getMinID())
		return nil
	}

	if err := generateBlocks(push); err != nil {
		return nil, err
	}

	w.writer.WriteEmptyBlock()

	w.stats = append(w.stats, former.GetStats())

	return minBlockIDs, nil
}

func (w *DiskBlocksWriter) writeTokensBlocks(zstdCompressLevel int, generateBlocks func(func(*tokensBlock) error) error) (token.Table, error) {
	var startIndex uint32
	tokenTable := make(token.Table)

	opts := []disk.FlushOption{disk.WithZstdCompressLevel(zstdCompressLevel)}

	former := w.NewBlockFormer("tokens", consts.RegularBlockSize)

	push := func(block *tokensBlock) error {
		if block.isStartOfField && block.totalSizeOfField > consts.RegularBlockSize {
			if err := former.FlushForced(opts...); err != nil {
				return err
			}
			startIndex = 0
		}

		tokenTableEntry := block.createTokenTableEntry(startIndex, w.writer.GetBlockIndex())
		fieldData, ok := tokenTable[block.field]
		if !ok {
			minVal := string(block.payload.GetToken(0))
			fieldData = &token.FieldData{
				MinVal: minVal,
			}
			tokenTable[block.field] = fieldData
			tokenTableEntry.MinVal = minVal
		}
		fieldData.Entries = append(fieldData.Entries, tokenTableEntry)

		former.Buf = block.payload.Pack(former.Buf)
		startIndex += uint32(block.payload.Len())

		if flushed, err := former.FlushIfNeeded(opts...); err != nil {
			return err
		} else if flushed {
			startIndex = 0
		}
		return nil
	}

	if err := generateBlocks(push); err != nil {
		return nil, err
	}

	if err := former.FlushForced(opts...); err != nil {
		return nil, err
	}

	w.writer.WriteEmptyBlock()

	w.stats = append(w.stats, former.GetStats())

	return tokenTable, nil
}

func (w *DiskBlocksWriter) writeTokenTableBlocks(zstdCompressLevel int, generateBlocks func(func(token.TableBlock) error) error) error {
	former := w.NewBlockFormer("token_table", consts.RegularBlockSize)

	opts := []disk.FlushOption{disk.WithZstdCompressLevel(zstdCompressLevel)}

	push := func(block token.TableBlock) error {
		former.Buf = block.Pack(former.Buf)
		if _, err := former.FlushIfNeeded(opts...); err != nil {
			return err
		}
		return nil
	}

	if err := generateBlocks(push); err != nil {
		return err
	}

	if err := former.FlushForced(opts...); err != nil {
		return err
	}

	w.writer.WriteEmptyBlock()

	w.stats = append(w.stats, former.GetStats())

	return nil
}

func (w *DiskBlocksWriter) writeLIDsBlocks(zstdCompressLevel int, generateBlocks func(func(*lidsBlock) error) error) (*lids.Table, error) {
	lidsTable := lids.NewTable(w.writer.GetBlockIndex(), nil, nil, nil)

	former := w.NewBlockFormer("lids", consts.RegularBlockSize)

	levelOpt := disk.WithZstdCompressLevel(zstdCompressLevel)

	push := func(block *lidsBlock) error {
		former.Buf = block.payload.Pack(former.Buf)
		if err := former.FlushForced(disk.WithExt(block.getExtForRegistry()), levelOpt); err != nil {
			return err
		}

		lidsTable.MinTIDs = append(lidsTable.MinTIDs, block.minTID)
		lidsTable.MaxTIDs = append(lidsTable.MaxTIDs, block.maxTID)
		lidsTable.IsContinued = append(lidsTable.IsContinued, block.isContinued)

		return nil
	}

	if err := generateBlocks(push); err != nil {
		return nil, err
	}

	w.writer.WriteEmptyBlock()

	w.stats = append(w.stats, former.GetStats())

	return lidsTable, nil
}

func (w *DiskBlocksWriter) WriteRegistryBlock() error {
	return w.writer.WriteBlocksRegistry()
}
