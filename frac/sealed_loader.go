package frac

import (
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
)

// LegacyLoader reads the old single .index file format by scanning blocks sequentially.
// Block indices stored in lids.Table and seqids.Table are absolute within the .index file,
// so the same IndexReader can be passed to all sub-loaders unchanged.
type LegacyLoader struct {
	reader     storage.IndexReader
	blockIndex uint32
}

// Load populates blocksData from a single legacy .index file.
// It starts at block 1 (block 0 is the Info block, already read by loadHeader).
func (l *LegacyLoader) Load(blocksData *sealed.BlocksData, info *common.Info, reader storage.IndexReader) {
	t := time.Now()

	l.reader = reader
	l.blockIndex = 1 // skip Info block at index 0

	l.skipSection() // skip token blocks
	l.skipSection() // skip token table blocks

	var err error
	blocksData.IDsTable, blocksData.BlocksOffsets, err = l.loadIDs(info)
	if err != nil {
		logger.Fatal("legacy load ids error", zap.Error(err))
	}

	blocksData.LIDsTable, err = l.loadLIDsTable()
	if err != nil {
		logger.Fatal("legacy load lids error", zap.Error(err))
	}

	took := time.Since(t)
	docsTotalK := float64(info.DocsTotal) / 1000
	indexOnDiskMb := util.SizeToUnit(info.IndexOnDisk, "mb")
	throughput := indexOnDiskMb / util.DurationToUnit(took, "s")
	logger.Info("sealed fraction loaded (legacy format)",
		zap.String("fraction", info.Path),
		util.ZapMsTsAsESTimeStr("creation_time", info.CreationTime),
		zap.String("from", info.From.String()),
		zap.String("to", info.To.String()),
		util.ZapFloat64WithPrec("docs_k", docsTotalK, 1),
		util.ZapDurationWithPrec("took_ms", took, "ms", 1),
		util.ZapFloat64WithPrec("throughput_mb_sec", throughput, 1),
	)
}

// skipSection advances past one separator-delimited section (reads headers until Len() == 0).
func (l *LegacyLoader) skipSection() {
	for {
		h, err := l.reader.GetBlockHeader(l.blockIndex)
		if err != nil {
			logger.Panic("error reading block header", zap.Error(err))
		}

		l.blockIndex++
		if h.Len() == 0 {
			return
		}
	}
}

// loadIDs reads the BlockOffsets block and then scans MID/RID/Pos triplets.
func (l *LegacyLoader) loadIDs(info *common.Info) (seqids.Table, []uint64, error) {
	var buf []byte

	data, _, err := l.reader.ReadIndexBlock(l.blockIndex, buf)
	if err != nil {
		return seqids.Table{}, nil, err
	}

	var offsets sealed.BlockOffsets
	if err := offsets.Unpack(data, info.BinaryDataVer); err != nil {
		return seqids.Table{}, nil, err
	}

	// Move to the first block of ID section.
	l.blockIndex++

	table := seqids.Table{
		StartBlockIndex: l.blockIndex,       // absolute index of first MID block in .index
		IDsTotal:        info.DocsTotal + 1, // Increment by one for [seq.SystemID]
	}

	for {
		h, err := l.reader.GetBlockHeader(l.blockIndex)
		if err != nil {
			logger.Fatal("error reading id block header", zap.Error(err))
		}

		l.blockIndex++
		if h.Len() == 0 {
			break
		}

		mid := seq.MID(h.GetExt1())
		if info.BinaryDataVer < config.BinaryDataV2 {
			mid = seq.MillisToMID(h.GetExt1())
		}

		table.MinBlockIDs = append(table.MinBlockIDs, seq.ID{
			MID: mid,
			RID: seq.RID(h.GetExt2()),
		})

		l.blockIndex += 2 // skip RIDs and Pos blocks
	}

	return table, offsets.Offsets, nil
}

// loadLIDsTable scans LID block headers, recording the absolute start index for lids.Table.
func (l *LegacyLoader) loadLIDsTable() (*lids.Table, error) {
	startIndex := l.blockIndex // absolute index of first LID block in .index

	var (
		maxTIDs     []uint32
		minTIDs     []uint32
		isContinued []bool
	)

	for {
		h, err := l.reader.GetBlockHeader(l.blockIndex)
		if err != nil {
			return nil, err
		}

		l.blockIndex++
		if h.Len() == 0 {
			break
		}

		maxTIDs = append(maxTIDs, uint32(h.GetExt2()>>32))
		minTIDs = append(minTIDs, uint32(h.GetExt2()&0xFFFFFFFF))

		isContinued = append(isContinued, h.GetExt1() == 1)
	}

	return lids.NewTable(startIndex, minTIDs, maxTIDs, isContinued), nil
}

// IndexReaders holds one IndexReader per split index file.
type IndexReaders struct {
	Token   storage.IndexReader
	Offsets storage.IndexReader
	ID      storage.IndexReader
	LID     storage.IndexReader
}

// Loader reads the per-section index files to populate BlocksData.
// Token data is loaded lazily (BlockLoader / TableLoader use the Token reader directly).
// Info is loaded separately via loadHeader before Load is called.
type Loader struct {
	buf []byte
}

// Load populates blocksData from the .offsets, .id, and .lid files.
func (l *Loader) Load(blocksData *sealed.BlocksData, info *common.Info, readers IndexReaders) {
	t := time.Now()

	var (
		err          error
		blockOffsets sealed.BlockOffsets
	)

	blockOffsets, err = l.loadBlocksOffsets(readers.Offsets, info.BinaryDataVer)
	if err != nil {
		logger.Fatal("load offsets error", zap.Error(err))
	}
	blocksData.BlocksOffsets = blockOffsets.Offsets

	blocksData.IDsTable = l.loadIDsTable(readers.ID, info)
	blocksData.LIDsTable, err = l.loadLIDsTable(readers.LID)
	if err != nil {
		logger.Fatal("load lids error", zap.Error(err))
	}

	took := time.Since(t)
	docsTotalK := float64(info.DocsTotal) / 1000
	indexOnDiskMb := util.SizeToUnit(info.IndexOnDisk, "mb")
	throughput := indexOnDiskMb / util.DurationToUnit(took, "s")
	logger.Info("sealed fraction loaded",
		zap.String("fraction", info.Path),
		util.ZapMsTsAsESTimeStr("creation_time", info.CreationTime),
		zap.String("from", info.From.String()),
		zap.String("to", info.To.String()),
		util.ZapFloat64WithPrec("docs_k", docsTotalK, 1),
		util.ZapDurationWithPrec("took_ms", took, "ms", 1),
		util.ZapFloat64WithPrec("throughput_mb_sec", throughput, 1),
	)
}

// loadBlocksOffsets reads block 0 from the .offsets file.
func (l *Loader) loadBlocksOffsets(
	r storage.IndexReader,
	fracVer config.BinaryDataVersion,
) (sealed.BlockOffsets, error) {
	data, _, err := r.ReadIndexBlock(0, l.buf)
	l.buf = data

	if err != nil {
		return sealed.BlockOffsets{}, err
	}

	var b sealed.BlockOffsets
	if err := b.Unpack(data, fracVer); err != nil {
		return sealed.BlockOffsets{}, err
	}

	return b, nil
}

// loadIDsTable scans block headers in the .id file to build seqids.Table.
// Blocks are stored as (MIDs, RIDs, Pos) triplets; we only need MIDs headers.
func (l *Loader) loadIDsTable(r storage.IndexReader, info *common.Info) seqids.Table {
	table := seqids.Table{
		StartBlockIndex: 0,
		IDsTotal:        info.DocsTotal + 1, // Increment by one for [seq.SystemID]
	}

	blocksCount, err := r.BlocksCount()
	if err != nil {
		logger.Fatal(
			"cannot get block count",
			zap.Error(err),
		)
	}

	for blockIdx := 0; blockIdx < blocksCount; blockIdx += 3 {
		header, err := r.GetBlockHeader(uint32(blockIdx))
		if err != nil {
			logger.Fatal("error reading id block header", zap.Error(err))
		}

		var mid seq.MID
		if info.BinaryDataVer < config.BinaryDataV2 {
			mid = seq.MillisToMID(header.GetExt1())
		} else {
			mid = seq.MID(header.GetExt1())
		}

		table.MinBlockIDs = append(table.MinBlockIDs, seq.ID{
			MID: mid,
			RID: seq.RID(header.GetExt2()),
		})
	}

	return table
}

// loadLIDsTable scans block headers in the .lid file to build lids.Table.
func (l *Loader) loadLIDsTable(r storage.IndexReader) (*lids.Table, error) {
	var (
		maxTIDs     []uint32
		minTIDs     []uint32
		isContinued []bool
	)

	blocksCount, err := r.BlocksCount()
	if err != nil {
		logger.Fatal(
			"cannot get block count",
			zap.Error(err),
		)
	}

	for blockIdx := 0; blockIdx < blocksCount; blockIdx++ {
		header, err := r.GetBlockHeader(uint32(blockIdx))
		if err != nil {
			return nil, err
		}

		ext2 := header.GetExt2()
		maxTIDs = append(maxTIDs, uint32(ext2>>32))
		minTIDs = append(minTIDs, uint32(ext2&0xFFFFFFFF))

		isContinued = append(isContinued, header.GetExt1() == 1)
	}

	return lids.NewTable(0, minTIDs, maxTIDs, isContinued), nil
}
