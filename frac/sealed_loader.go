package frac

import (
	"encoding/binary"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/packer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type Loader struct {
	frac         *Sealed
	reader       *disk.Reader
	blocksReader *disk.BlocksReader
	blockIndex   uint32
	outBuf       []byte
}

func (l *Loader) Load(frac *Sealed) {
	t := time.Now()

	l.frac = frac
	l.reader = l.frac.reader

	l.blocksReader = l.frac.blocksReader
	l.blockIndex = 1 // skipping info block that's already read

	tokenTable, err := l.loadTokenList()
	if err != nil {
		logger.Fatal("load token list error", zap.Error(err))
	}
	frac.tokenTable = tokenTable

	err = l.loadIDs()
	if err != nil {
		logger.Fatal("load ids error", zap.Error(err))
	}

	if l.frac.lidsTable, err = l.loadLIDsBlocksTable(); err != nil {
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

func (l *Loader) processReadTask() *disk.ReadIndexTask {
	task := l.reader.ReadIndexBlock(l.blocksReader, l.blockIndex, l.outBuf)
	l.outBuf = task.Buf
	l.blockIndex++
	return task
}

func (l *Loader) skipBlock() disk.BlocksRegistryEntry {
	header := l.blocksReader.GetBlockHeader(l.blockIndex)
	l.blockIndex++
	return header
}

func (l *Loader) loadIDs() error {
	frac := l.frac
	ids := frac.ids

	// read positions block
	task := l.processReadTask()
	if util.IsRecoveredPanicError(task.Err) {
		logger.Panic("todo: handle read err", zap.Error(task.Err))
	}

	result := task.Buf
	ids.IDBlocksTotal = binary.LittleEndian.Uint32(result)
	result = result[4:]

	// total ids
	ids.IDsTotal = binary.LittleEndian.Uint32(result)
	result = result[4:]

	docBlock := uint64(0)
	for len(result) != 0 {
		delta, n := binary.Varint(result)
		if n == 0 {
			panic("varint returned 0")
		}
		result = result[n:]
		docBlock += uint64(delta)

		frac.DocBlocks.Append(docBlock)
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

func (l *Loader) loadTokenList() (token.Table, error) {
	for {
		// skip actual token blocks, go for token table
		header := l.skipBlock()
		if header.Len() == 0 {
			break
		}
	}

	tokenTable := make(token.Table)
	for task := l.processReadTask(); len(task.Buf) > 0; task = l.processReadTask() {
		if task.Err != nil {
			return nil, task.Err
		}

		unpacker := packer.NewBytesUnpacker(task.Buf)
		for unpacker.Len() > 0 {
			fieldName := string(unpacker.GetBinary())
			field := token.FieldData{Entries: make([]*token.TableEntry, unpacker.GetUint32())}
			entries := make([]token.TableEntry, len(field.Entries))
			for i := range field.Entries {
				e := &entries[i]
				e.StartTID = unpacker.GetUint32()
				e.ValCount = unpacker.GetUint32()
				e.StartIndex = unpacker.GetUint32() // todo: it seems we can calculate this field using valCount but not store startIndex on disk
				e.BlockIndex = unpacker.GetUint32()
				minVal := unpacker.GetBinary()
				if i == 0 {
					field.MinVal = string(minVal)
				}
				e.MaxVal = string(unpacker.GetBinary())
				field.Entries[i] = e
			}
			tokenTable[fieldName] = &field
		}
	}

	return tokenTable, nil
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
