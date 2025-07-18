package sealing

import (
	"errors"
	"iter"
	"os"
	"path/filepath"
	"time"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
	"go.uber.org/zap"
)

type Source interface {
	Info() *common.Info                                   // fraction Info
	IDsBlocks(size int) iter.Seq2[[]seq.ID, []seq.DocPos] // ordered sequence of document IDs and their positions, divided into blocks
	TokenBlocks(size int) iter.Seq[[][]byte]              // ordered sequence of tokens divided into blocks
	Fields() iter.Seq2[string, uint32]                    // ordered sequence of fields with their max field's TID value
	TokenLIDs() iter.Seq[[]uint32]                        // ordered sequence of sorted tokenLIDs
	BlocksOffsets() []uint64                              // offsets of DocBlock's
	LastError() error
}

func Seal(src Source, params common.SealParams) (*sealed.PreloadedData, error) {
	start := time.Now()
	info := src.Info()

	if info.To == 0 {
		return nil, errors.New("sealing of an empty active fraction is not supported")
	}

	indexFile, err := os.Create(info.Path + consts.IndexTmpFileSuffix)
	if err != nil {
		return nil, err
	}

	indexSealer := NewIndexSealer(params)
	if err = indexSealer.WriteIndex(indexFile, src); err != nil {
		return nil, err
	}

	if err := indexFile.Sync(); err != nil {
		return nil, err
	}
	if err := os.Rename(indexFile.Name(), info.Path+consts.IndexFileSuffix); err != nil {
		return nil, err
	}

	util.MustSyncPath(filepath.Dir(info.Path))

	stat, err := indexFile.Stat()
	if err != nil {
		return nil, err
	}
	info.IndexOnDisk = uint64(stat.Size())

	if err := indexFile.Close(); err != nil {
		return nil, err
	}

	preloaded := sealed.PreloadedData{
		Info:          info,
		BlocksOffsets: src.BlocksOffsets(),
		IDsTable:      indexSealer.IDsTable(),
		LIDsTable:     indexSealer.LIDsTable(),
		TokenTable:    indexSealer.TokenTable(),
	}

	logger.Info(
		"fraction sealed",
		zap.String("fraction", filepath.Dir(info.Path)),
		zap.Float64("time_spent_s", util.DurationToUnit(time.Since(start), "s")),
	)
	return &preloaded, nil
}
