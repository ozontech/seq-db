package frac

import (
	"io"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"
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
}

func Seal(f *Active, params SealParams) *PreloadedData {
	logger.Info("sealing fraction", zap.String("fraction", f.BaseFileName))

	start := time.Now()
	info := f.Info()
	if info.To == 0 {
		logger.Panic("sealing of an empty active fraction is not supported")
	}

	f.setInfoSealingTime(uint64(time.Now().UnixMilli()))

	tmpFileName := f.BaseFileName + consts.IndexTmpFileSuffix
	indexFile, err := os.OpenFile(tmpFileName, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0o776)
	if err != nil {
		logger.Fatal("can't open file", zap.String("file", tmpFileName), zap.Error(err))
	}

	_, err = indexFile.Seek(16, io.SeekStart) // skip 16 bytes for pos and length of registry
	if err != nil {
		logger.Fatal("can't seek file", zap.String("file", indexFile.Name()), zap.Error(err))
	}

	preloaded, err := writeAllBlocks(f, indexFile, params)
	if err != nil {
		logger.Fatal("can't seek file", zap.String("file", indexFile.Name()), zap.Error(err))
	}

	stat, err := indexFile.Stat() // refresh f.info.IndexOnDisk - it will be used later
	if err != nil {
		logger.Fatal("can't stat index file", zap.String("file", indexFile.Name()), zap.Error(err))
	}
	f.setInfoIndexOnDisk(uint64(stat.Size()))

	if err := indexFile.Sync(); err != nil {
		logger.Fatal("can't sync tmp index file", zap.String("file", indexFile.Name()), zap.Error(err))
	}

	newFileName := f.BaseFileName + consts.IndexFileSuffix
	err = os.Rename(tmpFileName, newFileName)
	if err != nil {
		logger.Error("can't rename index file",
			zap.String("old_path", tmpFileName),
			zap.String("new_path", newFileName),
			zap.Error(err),
		)
	}

	logger.Info(
		"fraction sealed",
		zap.String("fraction", newFileName),
		zap.Float64("time_spent_s", util.DurationToUnit(time.Since(start), "s")),
	)

	return preloaded
}

func writeAllBlocks(f *Active, indexFile *os.File, params SealParams) (*PreloadedData, error) {
	var err error

	writer := NewSealedBlockWriter(indexFile)
	producer := NewDiskBlocksProducer(f)

	logger.Info("sealing frac stats...")
	if err = writer.writeInfoBlock(producer.getInfoBlock()); err != nil {
		logger.Error("seal info error", zap.Error(err))
		return nil, err
	}

	logger.Info("sealing tokens...")
	tokenTable, err := writer.writeTokensBlocks(params.TokenListZstdLevel, producer.getTokensBlocksGenerator())
	if err != nil {
		logger.Error("sealing tokens error", zap.Error(err))
		return nil, err
	}

	logger.Info("sealing tokens table...")
	if err = writer.writeTokenTableBlocks(params.TokenTableZstdLevel, producer.getTokenTableBlocksGenerator(tokenTable)); err != nil {
		logger.Error("sealing tokens table error", zap.Error(err))
		return nil, err
	}

	logger.Info("writing document positions block...")
	if err = writer.writePositionsBlock(params.DocsPositionsZstdLevel, producer.getPositionBlock()); err != nil {
		logger.Error("document positions block error", zap.Error(err))
		return nil, err
	}

	logger.Info("sealing ids...")
	minBlockIDs, err := writer.writeIDsBlocks(params.IDsZstdLevel, producer.getIDsBlocksGenerator(consts.IDsBlockSize))
	if err != nil {
		logger.Error("seal ids error", zap.Error(err))
		return nil, err
	}

	logger.Info("sealing lids...")
	lidsTable, err := writer.writeLIDsBlocks(params.LIDsZstdLevel, producer.getLIDsBlockGenerator(consts.LIDBlockCap))
	if err != nil {
		logger.Error("seal lids error", zap.Error(err))
		return nil, err
	}

	logger.Info("write registry...")
	if err = writer.WriteRegistryBlock(); err != nil {
		logger.Error("write registry error", zap.Error(err))
		return nil, err
	}

	preloaded := PreloadedData{
		info:          &(*f.info),
		idsTable:      createIDsTable(f, writer.startOfIDsBlockIndex, minBlockIDs),
		lidsTable:     lidsTable,
		tokenTable:    tokenTable,
		BlocksOffsets: f.DocBlocks.GetVals(),
		docsFile:      f.docsFile,
		docsCache:     f.docsCache,
		docsReader:    f.docsReader,
		indexFile:     indexFile,
	}

	writer.stats.WriteLogs()

	return &preloaded, nil
}

func createIDsTable(f *Active, startOfIDsBlockIndex uint32, minBlockIDs []seq.ID) IDsTable {
	return IDsTable{
		MinBlockIDs:         minBlockIDs,
		IDsTotal:            f.MIDs.Len(),
		IDBlocksTotal:       f.DocBlocks.Len(),
		DiskStartBlockIndex: startOfIDsBlockIndex,
	}
}
