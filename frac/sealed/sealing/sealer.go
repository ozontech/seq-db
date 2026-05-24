package sealing

import (
	"errors"
	"iter"
	"os"
	"path/filepath"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type (
	DocLocation  = util.Pair[seq.ID, seq.DocPos]
	TokenPosting = util.Pair[[]byte, []uint32]
)

// Source interface defines the contract for data sources that can be sealed.
// Provides access to all necessary data components for index creation
type Source interface {
	// Info returns metadata describing this source.
	Info() *common.Info

	// ID returns an iterator over stored document identifiers paired with
	// their positions, in descending [seq.ID] order.
	ID() iter.Seq2[DocLocation, error]

	// BlockOffsets returns byte offsets to each document block
	// within this source's `.docs` file.
	BlockOffsets() []uint64

	// TokenTriplet iterates over fields in lexicographic order.
	// For each field, it yields tokens (lexicographically sorted)
	// paired with the local document ID list for that token.
	TokenTriplet() iter.Seq2[string, iter.Seq2[TokenPosting, error]]
}

// Seal writes five index files (.info, .token, .offsets, .id, .lid) for the fraction
// and returns PreloadedData for fast initialization of the sealed fraction.
func Seal(src Source, params common.SealParams) (*sealed.PreloadedData, error) {
	info := src.Info()

	if info.To == 0 {
		return nil, errors.New("sealing of an empty active fraction is not supported")
	}

	sealer := NewIndexSealer(params)

	if err := createAndWrite(
		info.Path+consts.OffsetsTmpFileSuffix,
		info.Path+consts.OffsetsFileSuffix,
		func(f *os.File) error { return sealer.WriteOffsetsFile(f, src) },
	); err != nil {
		return nil, err
	}

	if !params.SkipFsync {
		// Ensure data is flushed to disk
		if err := indexFile.Sync(); err != nil {
			return nil, err
		}
	}

	if err := createAndWriteBoth(
		info.Path+consts.TokenTmpFileSuffix, info.Path+consts.TokenFileSuffix,
		info.Path+consts.LIDTmpFileSuffix, info.Path+consts.LIDFileSuffix,
		func(tokenF, lidF *os.File) error { return sealer.WriteTokenTriplet(tokenF, lidF, src) },
	); err != nil {
		return nil, err
	}

	if err := createAndWrite(
		info.Path+consts.InfoTmpFileSuffix,
		info.Path+consts.InfoFileSuffix,
		func(f *os.File) error { return sealer.WriteInfoFile(f, src) },
	); err != nil {
		return nil, err
	}

	util.MustSyncPath(filepath.Dir(info.Path))

	// Compute total index size as sum of all 5 files.
	var totalSize uint64
	for _, suffix := range []string{
		consts.InfoFileSuffix,
		consts.TokenFileSuffix,
		consts.OffsetsFileSuffix,
		consts.IDFileSuffix,
		consts.LIDFileSuffix,
	} {
		st, err := os.Stat(info.Path + suffix)
		if err != nil {
			return nil, err
		}
		totalSize += uint64(st.Size())
	}

	if !params.SkipFsync {
		// Ensure directory metadata is synced to disk
		util.MustSyncPath(filepath.Dir(info.Path))
	}

	preloaded := &sealed.PreloadedData{
		Info:       info,
		TokenTable: sealer.TokenTable(),
		BlocksData: sealed.BlocksData{
			IDsTable:      sealer.IDsTable(),
			LIDsTable:     &lidsTable,
			BlocksOffsets: src.BlockOffsets(),
		},
	}

	return preloaded, nil
}

func syncAndClose(f *os.File) error {
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func createAndWrite(
	tmp, final string,
	write func(*os.File) error,
) error {
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	if err := errors.Join(write(f), syncAndClose(f)); err != nil {
		return err
	}

	return os.Rename(tmp, final)
}

func createAndWriteBoth(
	tmpa, finala,
	tmpb, finalb string,
	write func(*os.File, *os.File) error,
) error {
	a, err := os.Create(tmpa)
	if err != nil {
		return err
	}

	b, err := os.Create(tmpb)
	if err != nil {
		a.Close()
		return err
	}

	writeErr := write(a, b)
	if err := errors.Join(writeErr, syncAndClose(a), syncAndClose(b)); err != nil {
		return err
	}

	if err := os.Rename(tmpa, finala); err != nil {
		return err
	}

	return os.Rename(tmpb, finalb)
}
