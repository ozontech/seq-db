package sealer

import (
	"errors"
	"iter"
	"os"
	"path/filepath"
	"time"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
	"go.uber.org/zap"
)

type Source interface {
	Info() *frac.Info
	Tokens() iter.Seq[[]byte]              // ordered sequence of tokens
	Fields() iter.Seq2[string, uint32]     // ordered sequence of fields with max field's TID value
	IDs() iter.Seq[seq.ID]                 // ordered sequence of seq.ID's
	Pos() iter.Seq[seq.DocPos]             // ordered sequence of seq.DocPos
	TokenLIDs() iter.Seq[iter.Seq[uint32]] // ordered sequence of sorted tokenLIDs
	BlocksOffsets() []uint64
	DocFile() *os.File
}

func Seal(src Source, params frac.SealParams) (*frac.PreloadedData, error) {
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

	if indexFile, err = frac.SyncRename(indexFile, info.Path+consts.IndexFileSuffix); err != nil {
		return nil, err
	}

	util.MustSyncPath(filepath.Dir(info.Path))

	stat, err := indexFile.Stat()
	if err != nil {
		return nil, err
	}
	info.IndexOnDisk = uint64(stat.Size())

	preloaded := frac.PreloadedData{
		Info:          info,
		IndexFile:     indexFile,
		DocsFile:      src.DocFile(),
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
