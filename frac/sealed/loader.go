package sealed

import (
	"encoding/binary"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type Loader struct {
	frac         *Sealed
	reader       *disk.Reader
	blocksReader *disk.RegistryReader
	blockIndex   uint32
	blockBuf     []byte
}

func (l *Loader) Load(frac *Sealed) {
	t := time.Now()

	l.frac = frac
	l.reader = l.frac.reader

	l.blocksReader = l.frac.blocksReader
	l.blockIndex = 1 // skipping info block that's already read

	l.skipTokens()

	err := l.loadIDs()
	if err != nil {
		logger.Fatal("load ids error", zap.Error(err))
	}

	if l.frac.lidsTable, err = l.loadLIDsBlocksTable(); err != nil {
		logger.Fatal("load lids error", zap.Error(err))
	}

	took := time.Since(t)

	docsTotalK := float64(frac.Base.Info.DocsTotal) / 1000
	indexOnDiskMb := util.SizeToUnit(frac.Base.Info.IndexOnDisk, "mb")
	throughput := indexOnDiskMb / util.DurationToUnit(took, "s")
	logger.Info("sealed fraction loaded",
		zap.String("fraction", frac.BaseFileName),
		util.ZapMsTsAsESTimeStr("creation_time", frac.Base.Info.CreationTime),
		zap.String("from", frac.Base.Info.From.String()),
		zap.String("to", frac.Base.Info.To.String()),
		util.ZapFloat64WithPrec("docs_k", docsTotalK, 1),
		util.ZapDurationWithPrec("took_ms", took, "ms", 1),
		util.ZapFloat64WithPrec("throughput_mb_sec", throughput, 1),
	)
}

func (l *Loader) nextIndexBlock() ([]byte, error) {
	data, _, err := l.reader.ReadIndexBlock(l.blocksReader, l.blockIndex, l.blockBuf)
	l.blockBuf = data
	l.blockIndex++
	return data, err
}

func (l *Loader) skipBlock() disk.RegistryEntry {
	header := l.blocksReader.GetBlockHeader(l.blockIndex)
	l.blockIndex++
	return header
}

func (l *Loader) loadIDs() error {
	frac := l.frac
	ids := frac.ids

	// read positions block
	result, err := l.nextIndexBlock()
	if util.IsRecoveredPanicError(err) {
		logger.Panic("todo: handle read err", zap.Error(err))
	}

	ids.IDBlocksTotal = binary.LittleEndian.Uint32(result)
	result = result[4:]

	// total ids
	ids.IDsTotal = binary.LittleEndian.Uint32(result)
	result = result[4:]

	offset := uint64(0)
	for len(result) != 0 {
		delta, n := binary.Varint(result)
		if n == 0 {
			panic("varint returned 0")
		}
		result = result[n:]
		offset += uint64(delta)

		frac.blocksOffsets = append(frac.blocksOffsets, offset)
	}

	ids.DiskStartBlockIndex = l.blockIndex

	for {
		// get MIDs block header
		header := l.skipBlock()
		if header.Len() == 0 {
			break
		}
		ids.MinBlockIDs = append(ids.MinBlockIDs, seq.ID{
			MID: seq.MID(header.GetExt1()),
			RID: seq.RID(header.GetExt2()),
		})

		// skipping RIDs and Pos blocks
		l.skipBlock()
		l.skipBlock()
	}

	return nil
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
