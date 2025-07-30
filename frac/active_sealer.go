package frac

import (
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type SealParams struct {
	IDsZstdLevel           int
	LIDsZstdLevel          int
	TokenListZstdLevel     int
	DocsPositionsZstdLevel int
	TokenTableZstdLevel    int

	DocBlocksZstdLevel int // DocBlocksZstdLevel is the zstd compress level of each document block.
	DocBlockSize       int // DocBlockSize is decompressed payload size of document block.
}

func Seal(f *Active, params SealParams) (*PreloadedData, error) {
	logger.Info("sealing fraction", zap.String("fraction", f.BaseFileName))

	start := time.Now()
	info := *f.info // copy
	if info.To == 0 {
		logger.Panic("sealing of an empty active fraction is not supported")
	}
	info.SealingTime = uint64(start.UnixMilli())

	indexFile, err := os.Create(f.BaseFileName + consts.IndexTmpFileSuffix)
	if err != nil {
		return nil, err
	}

	if _, err = indexFile.Seek(16, io.SeekStart); err != nil { // skip 16 bytes for pos and length of registry
		return nil, err
	}

	preloaded, err := writeSealedFraction(f, &info, indexFile, params)
	if err != nil {
		return nil, err
	}

	if indexFile, err = syncRename(indexFile, f.BaseFileName+consts.IndexFileSuffix); err != nil {
		return nil, err
	}

	parentDirPath := filepath.Dir(f.BaseFileName)
	util.MustSyncPath(parentDirPath)

	stat, err := indexFile.Stat()
	if err != nil {
		return nil, err
	}
	info.IndexOnDisk = uint64(stat.Size())

	preloaded.info = &info
	preloaded.indexFile = indexFile

	logger.Info(
		"fraction sealed",
		zap.String("fraction", f.BaseFileName),
		zap.Float64("time_spent_s", util.DurationToUnit(time.Since(start), "s")),
	)

	return preloaded, nil
}

func syncRename(f *os.File, newName string) (*os.File, error) {
	if err := f.Sync(); err != nil {
		return nil, err
	}
	if err := os.Rename(f.Name(), newName); err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	return os.OpenFile(newName, os.O_RDONLY, 0o776) // reopen with new name
}

func writeSortedDocs(f *Active, params SealParams, sortedIDs []seq.ID) (*os.File, []uint64, map[seq.ID]seq.DocPos, error) {
	sdocsFile, err := os.Create(f.BaseFileName + consts.SdocsTmpFileSuffix)
	if err != nil {
		return nil, nil, nil, err
	}

	logger.Info("sorting docs...")
	bw := getDocBlocksWriter(sdocsFile, params.DocBlockSize, params.DocBlocksZstdLevel)
	defer putDocBlocksWriter(bw)

	if err := writeDocsInOrder(f.DocsPositions, f.DocBlocks.GetVals(), f.sortReader, sortedIDs, bw); err != nil {
		return nil, nil, nil, err
	}

	if sdocsFile, err = syncRename(sdocsFile, f.BaseFileName+consts.SdocsFileSuffix); err != nil {
		return nil, nil, nil, err
	}

	return sdocsFile, slices.Clone(bw.BlockOffsets), maps.Clone(bw.Positions), nil
}

func writeSealedFraction(f *Active, info *Info, indexFile io.WriteSeeker, params SealParams) (*PreloadedData, error) {
	var err error

	docsFile := f.docsFile
	blocksOffsets := f.DocBlocks.GetVals()
	positions := f.DocsPositions.positions

	sortedIDs, oldToNewLIDsIndex := sortSeqIDs(f, f.MIDs.GetVals(), f.RIDs.GetVals())

	if !f.Config.SkipSortDocs {
		if docsFile, blocksOffsets, positions, err = writeSortedDocs(f, params, sortedIDs); err != nil {
			return nil, err
		}
		stat, err := docsFile.Stat()
		if err != nil {
			return nil, err
		}
		info.DocsOnDisk = uint64(stat.Size())
	}

	producer := NewDiskBlocksProducer()
	writer := NewSealedBlockWriter(indexFile)

	logger.Info("sealing frac stats...")
	info.BuildDistribution(sortedIDs)
	if err := writer.writeInfoBlock(producer.getInfoBlock(info)); err != nil {
		return nil, fmt.Errorf("seal info error: %w", err)
	}

	var tokenTable token.Table
	{
		logger.Info("sealing tokens...")
		generator := producer.getTokensBlocksGenerator(f.TokenList)
		tokenTable, err = writer.writeTokensBlocks(params.TokenListZstdLevel, generator)
		if err != nil {
			return nil, fmt.Errorf("sealing tokens error: %w", err)
		}
	}

	{
		logger.Info("sealing tokens table...")
		generator := producer.getTokenTableBlocksGenerator(f.TokenList, tokenTable)
		if err := writer.writeTokenTableBlocks(params.TokenTableZstdLevel, generator); err != nil {
			return nil, fmt.Errorf("sealing tokens table error: %w", err)
		}
	}

	{
		logger.Info("writing document positions block...")
		idsLen := f.MIDs.Len()
		generator := producer.getPositionBlock(idsLen, blocksOffsets)
		if err := writer.writePositionsBlock(params.DocsPositionsZstdLevel, generator); err != nil {
			return nil, fmt.Errorf("document positions block error: %w", err)
		}
	}

	var minBlockIDs []seq.ID
	{
		logger.Info("sealing ids...")
		ds := DocsPositions{positions: positions}
		generator := producer.getIDsBlocksGenerator(sortedIDs, &ds, consts.IDsBlockSize)
		minBlockIDs, err = writer.writeIDsBlocks(params.IDsZstdLevel, generator)
		if err != nil {
			return nil, fmt.Errorf("seal ids error: %w", err)
		}
	}

	var lidsTable *lids.Table
	{
		logger.Info("sealing lids...")
		generator := producer.getLIDsBlockGenerator(f.TokenList, oldToNewLIDsIndex, f.MIDs, f.RIDs, consts.LIDBlockCap)
		lidsTable, err = writer.writeLIDsBlocks(params.LIDsZstdLevel, generator)
		if err != nil {
			return nil, fmt.Errorf("seal lids error: %w", err)
		}
	}

	logger.Info("write registry...")
	if err = writer.WriteRegistryBlock(); err != nil {
		return nil, fmt.Errorf("write registry error: %w", err)
	}

	writer.stats.WriteLogs()

	return &PreloadedData{
		docsFile:      docsFile,
		lidsTable:     lidsTable,
		tokenTable:    tokenTable,
		blocksOffsets: blocksOffsets,
		idsTable: seqids.Table{
			MinBlockIDs:     minBlockIDs,
			IDsTotal:        f.MIDs.Len(),
			IDBlocksTotal:   f.DocBlocks.Len(),
			StartBlockIndex: writer.startOfIDsBlockIndex,
		},
	}, nil
}

func writeDocsInOrder(pos *DocsPositions, blocks []uint64, docsReader disk.DocsReader, ids []seq.ID, bw *docBlocksWriter) error {
	// Skip system seq.ID.
	if len(ids) == 0 {
		panic(fmt.Errorf("BUG: ids is empty"))
	}
	if ids[0] != systemSeqID {
		panic(fmt.Errorf("BUG: system ID expected"))
	}
	ids = ids[1:]

	if err := writeDocBlocksInOrder(pos, blocks, docsReader, ids, bw); err != nil {
		return err
	}
	return nil
}

func writeDocBlocksInOrder(pos *DocsPositions, blocks []uint64, docsReader disk.DocsReader, ids []seq.ID, bw *docBlocksWriter) error {
	var prevID seq.ID
	for _, id := range ids {
		if id == prevID {
			// IDs have duplicates in case of nested index.
			// In this case we need to store the original document once.
			continue
		}
		prevID = id

		oldPos := pos.Get(id)
		if oldPos == seq.DocPosNotFound {
			panic(fmt.Errorf("BUG: can't find doc position"))
		}

		blockOffsetIndex, docOffset := oldPos.Unpack()
		blockOffset := blocks[blockOffsetIndex]
		err := docsReader.ReadDocsFunc(blockOffset, []uint64{docOffset}, func(doc []byte) error {
			return bw.WriteDoc(id, doc)
		})
		if err != nil {
			return fmt.Errorf("writing document to block: %s", err)
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	return nil
}

type docBlocksWriter struct {
	w             *bytespool.Writer
	compressLevel int
	minBlockSize  int

	curBlockIndex      int
	currentBlockOffset uint64

	docs     []byte
	blockBuf []byte

	BlockOffsets []uint64
	Positions    map[seq.ID]seq.DocPos
}

var docBlocksWriterPool = sync.Pool{
	New: func() any {
		return &docBlocksWriter{Positions: make(map[seq.ID]seq.DocPos)}
	},
}

func getDocBlocksWriter(w io.Writer, blockSize, compressLevel int) *docBlocksWriter {
	bw := docBlocksWriterPool.Get().(*docBlocksWriter)

	if blockSize <= 0 {
		blockSize = consts.MB * 4
	}

	bufSize := consts.MB * 32
	if bufSize < blockSize {
		bufSize = blockSize
	}

	*bw = docBlocksWriter{
		w:             bytespool.AcquireWriterSize(w, bufSize),
		compressLevel: compressLevel,
		minBlockSize:  blockSize,

		curBlockIndex:      0,
		currentBlockOffset: 0,

		docs:     bw.docs[:0],
		blockBuf: bw.blockBuf[:0],

		BlockOffsets: bw.BlockOffsets[:0],
		Positions:    bw.Positions,
	}
	clear(bw.Positions)

	return bw
}

func putDocBlocksWriter(bw *docBlocksWriter) {
	err := bytespool.FlushReleaseWriter(bw.w)
	if err != nil {
		panic(fmt.Errorf("BUG: writer must be flushed before releasing blocks writer: %s", err))
	}
	bw.w = nil
	docBlocksWriterPool.Put(bw)
}

func (w *docBlocksWriter) WriteDoc(id seq.ID, doc []byte) error {
	pos := seq.PackDocPos(uint32(w.curBlockIndex), uint64(len(w.docs)))
	w.Positions[id] = pos

	w.docs = binary.LittleEndian.AppendUint32(w.docs, uint32(len(doc)))
	w.docs = append(w.docs, doc...)

	if len(w.docs) > w.minBlockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	return nil
}

func (w *docBlocksWriter) flushBlock() error {
	blockLen, err := w.compressWriteBlock()
	if err != nil {
		return err
	}

	w.docs = w.docs[:0]
	w.BlockOffsets = append(w.BlockOffsets, w.currentBlockOffset)
	w.curBlockIndex++
	w.currentBlockOffset += uint64(blockLen)

	return nil
}

func (w *docBlocksWriter) compressWriteBlock() (int, error) {
	w.blockBuf = w.blockBuf[:0]
	w.blockBuf = disk.CompressDocBlock(w.docs, w.blockBuf, w.compressLevel)

	if _, err := w.w.Write(w.blockBuf); err != nil {
		return 0, err
	}

	blockLen := len(w.blockBuf)
	return blockLen, nil
}

func (w *docBlocksWriter) Flush() error {
	if len(w.docs) > 0 {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	return nil
}
