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

func seal(f *Active, params SealParams) {
	logger.Info("sealing fraction", zap.String("fraction", f.BaseFileName))

	start := time.Now()
	info := f.Info()
	if info.To == 0 {
		logger.Panic("sealing of an empty active fraction is not supported")
	}

	f.setInfoSealingTime(uint64(time.Now().UnixMilli()))

	tmpFileName := f.BaseFileName + consts.IndexTmpFileSuffix
	file, err := os.OpenFile(tmpFileName, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0o776)
	if err != nil {
		logger.Fatal("can't open file", zap.String("file", tmpFileName), zap.Error(err))
	}

	_, err = file.Seek(16, io.SeekStart) // skip 16 bytes for pos and length of registry
	if err != nil {
		logger.Fatal("can't seek file", zap.String("file", file.Name()), zap.Error(err))
	}

	if err = writeAllBlocks(f, file, params); err != nil {
		logger.Fatal("can't seek file", zap.String("file", file.Name()), zap.Error(err))
	}

	stat, err := file.Stat() // refresh f.info.IndexOnDisk - it will be used later
	if err != nil {
		logger.Fatal("can't stat index file", zap.String("file", file.Name()), zap.Error(err))
	}
	f.setInfoIndexOnDisk(uint64(stat.Size()))

	if err = file.Close(); err != nil {
		logger.Fatal("can't close file", zap.String("file", file.Name()), zap.Error(err))
	}

	f.close(false, "seal")

	newFileName := f.BaseFileName + consts.IndexFileSuffix
	err = os.Rename(tmpFileName, newFileName)
	if err != nil {
		logger.Error("can't rename index file",
			zap.String("old_path", tmpFileName),
			zap.String("new_path", newFileName),
			zap.Error(err),
		)
	}

	if f.shouldRemoveMeta {
		rmFileName := f.BaseFileName + consts.MetaFileSuffix
		err = os.Remove(rmFileName)
		if err != nil {
			logger.Error("can't delete metas file", zap.String("file", rmFileName), zap.Error(err))
		}
	}

	logger.Info(
		"fraction sealed",
		zap.String("fraction", newFileName),
		zap.Float64("time_spent_s", util.DurationToUnit(time.Since(start), "s")),
	)
}

func writeAllBlocks(f *Active, ws io.WriteSeeker, params SealParams) error {
	var err error

	writer := NewSealedBlockWriter(ws)
	producer := NewDiskBlocksProducer(f)

	logger.Info("sealing frac stats...")
	if err = writer.writeInfoBlock(producer.getInfoBlock()); err != nil {
		logger.Error("seal info error", zap.Error(err))
		return err
	}

	logger.Info("sealing tokens...")
	tokenTable, err := writer.writeTokensBlocks(params.TokenListZstdLevel, producer.getTokensBlocksGenerator())
	if err != nil {
		logger.Error("sealing tokens error", zap.Error(err))
		return err
	}

	logger.Info("sealing tokens table...")
	if err = writer.writeTokenTableBlocks(params.TokenTableZstdLevel, producer.getTokenTableBlocksGenerator(tokenTable)); err != nil {
		logger.Error("sealing tokens table error", zap.Error(err))
		return err
	}

	logger.Info("writing document positions block...")
	if err = writer.writePositionsBlock(params.DocsPositionsZstdLevel, producer.getPositionBlock()); err != nil {
		logger.Error("document positions block error", zap.Error(err))
		return err
	}

	logger.Info("sealing ids...")
	minBlockIDs, err := writer.writeIDsBlocks(params.IDsZstdLevel, producer.getIDsBlocksGenerator(consts.IDsBlockSize))
	if err != nil {
		logger.Error("seal ids error", zap.Error(err))
		return err
	}

	logger.Info("sealing lids...")
	lidsTable, err := writer.writeLIDsBlocks(params.LIDsZstdLevel, producer.getLIDsBlockGenerator(consts.LIDBlockCap))
	if err != nil {
		logger.Error("seal lids error", zap.Error(err))
		return err
	}

	logger.Info("write registry...")
	if err = writer.WriteRegistryBlock(); err != nil {
		logger.Error("write registry error", zap.Error(err))
		return err
	}

	// these fields actually aren't not used as intended: the data of these three fields will actually be read
	// from disk again in the future on the first attempt to search in fraction (see method Sealed.loadAndRLock())
	// TODO: we need to either remove this data preparation in active fraction sealing or avoid re-reading the data from disk
	f.sealedIDs = createSealedIDs(f, writer.startOfIDsBlockIndex, minBlockIDs)
	f.lidsTable = lidsTable
	f.tokenTable = tokenTable

	writer.stats.WriteLogs()

	return nil
}

func createSealedIDs(f *Active, startOfIDsBlockIndex uint32, minBlockIDs []seq.ID) *SealedIDs {
	sealedIDs := NewSealedIDs(nil, nil, nil)
	sealedIDs.DiskStartBlockIndex = startOfIDsBlockIndex
	sealedIDs.IDBlocksTotal = f.DocBlocks.Len()
	sealedIDs.IDsTotal = f.MIDs.Len()
	sealedIDs.MinBlockIDs = minBlockIDs
	return sealedIDs
}
