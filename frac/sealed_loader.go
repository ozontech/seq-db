package frac

import (
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
)

type Loader struct {
	reader     *storage.IndexReader
	blockIndex uint32
	blockBuf   []byte
}

func (l *Loader) Load(frac *Sealed) {
	t := time.Now()

	l.reader = &frac.indexReader

	l.blockIndex = 1 // skipping info block that's already read

	l.skipTokens()

	var err error

	if frac.idsTable, frac.BlocksOffsets, err = l.loadIDs(); err != nil {
		logger.Fatal("load ids error", zap.Error(err))
	}

	if frac.lidsTable, err = l.loadLIDsBlocksTable(); err != nil {
		logger.Fatal("load lids error", zap.Error(err))
	}

	took := time.Since(t)

	docsTotalK := float64(frac.info.DocsTotal) / 1000
	indexOnDiskMb := util.SizeToUnit(frac.info.IndexOnDisk, "mb")
	throughput := indexOnDiskMb / util.DurationToUnit(took, "s")
	logger.Info("sealed fraction loaded",
		zap.String("fraction", frac.BaseFileName),
		util.ZapMsTsAsESTimeStr("creation_time", frac.info.CreationTime),
		zap.String("from", frac.info.From.String()),
		zap.String("to", frac.info.To.String()),
		util.ZapFloat64WithPrec("docs_k", docsTotalK, 1),
		util.ZapDurationWithPrec("took_ms", took, "ms", 1),
		util.ZapFloat64WithPrec("throughput_mb_sec", throughput, 1),
	)
}

func (l *Loader) nextIndexBlock() ([]byte, error) {
	data, _, err := l.reader.ReadIndexBlock(l.blockIndex, l.blockBuf)
	l.blockBuf = data
	l.blockIndex++
	return data, err
}

func (l *Loader) skipBlock() storage.IndexBlockHeader {
	header, err := l.reader.GetBlockHeader(l.blockIndex)
	if err != nil {
		logger.Panic("error reading block header", zap.Error(err))
	}
	l.blockIndex++
	return header
}

func (l *Loader) loadIDs() (idsTable seqids.Table, blocksOffsets []uint64, err error) {
	var result []byte

	if result, err = l.nextIndexBlock(); err != nil {
		return idsTable, nil, err
	}

	b := BlockOffsets{}
	if err := b.Unpack(result); err != nil {
		return idsTable, nil, err
	}

	blocksOffsets = b.Offsets
	idsTable.IDsTotal = b.IDsTotal
	idsTable.IDBlocksTotal = uint32(len(b.Offsets))
	idsTable.StartBlockIndex = l.blockIndex

	for {
		// get MIDs block header
		header := l.skipBlock()
		if header.Len() == 0 {
			break
		}
		idsTable.MinBlockIDs = append(idsTable.MinBlockIDs, seq.ID{
			MID: seq.MID(header.GetExt1()),
			RID: seq.RID(header.GetExt2()),
		})

		// skipping RIDs and Pos blocks
		l.skipBlock()
		l.skipBlock()
	}

	return idsTable, blocksOffsets, nil
}

func (l *Loader) skipTokens() {
	for {
		// skip actual token blocks
		header := l.skipBlock()
		if header.Len() == 0 {
			break
		}
	}

	for {
		// skip token table
		header := l.skipBlock()
		if header.Len() == 0 {
			break
		}
	}
}

func (l *Loader) loadLIDsBlocksTable() (*lids.Table, error) {
	maxTIDs := make([]uint32, 0)
	minTIDs := make([]uint32, 0)
	isContinued := make([]bool, 0)

	startIndex := l.blockIndex
	for {
		header := l.skipBlock()
		if header.Len() == 0 {
			break
		}

		ext1 := header.GetExt1()
		ext2 := header.GetExt2()

		maxTIDs = append(maxTIDs, uint32(ext2>>32))
		minTIDs = append(minTIDs, uint32(ext2&0xFFFFFFFF))

		isContinued = append(isContinued, ext1 == 1)
	}

	return lids.NewTable(startIndex, minTIDs, maxTIDs, isContinued), nil
}
