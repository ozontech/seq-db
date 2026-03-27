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

// Source interface defines the contract for data sources that can be sealed.
// Provides access to all necessary data components for index creation.
type Source interface {
	// Info returns information about [sealing.Source].
	// For example, in one case it returns information about [frac.Active].
	Info() *common.Info

	// ID returns a view into [sealing.Source] stored ids.
	// Identificators are returned in sorted order starting with the biggest seq.ID.
	ID() iter.Seq2[seq.ID, seq.DocPos]

	// BlockOffsets returns all offsets to [storage.DocBlock]
	// stored nside `.docs` file that is owned by [sealing.Source].
	BlockOffsets() []uint64

	Iterator() iter.Seq2[
		string,                      // Field name
		iter.Seq2[[]byte, []uint32], // Token value and lids for this token
	]

	LastError() error // Last error encountered during data retrieval
}

// createAndWrite creates a tmp file, calls write, syncs, closes, then renames to finalPath.
func createAndWrite(tmpPath, finalPath string, write func(*os.File) error) error {
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	if err := write(f); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, finalPath)
}

// createAndWriteBoth creates two tmp files, calls write with both, syncs and closes them,
// then renames both to their final paths.
func createAndWriteBoth(tmpPath1, finalPath1, tmpPath2, finalPath2 string, write func(*os.File, *os.File) error) error {
	f1, err := os.Create(tmpPath1)
	if err != nil {
		return err
	}
	f2, err := os.Create(tmpPath2)
	if err != nil {
		f1.Close()
		return err
	}
	if err := write(f1, f2); err != nil {
		f1.Close()
		f2.Close()
		return err
	}
	if err := f1.Sync(); err != nil {
		f1.Close()
		f2.Close()
		return err
	}
	if err := f1.Close(); err != nil {
		f2.Close()
		return err
	}
	if err := f2.Sync(); err != nil {
		f2.Close()
		return err
	}
	if err := f2.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath1, finalPath1); err != nil {
		return err
	}
	return os.Rename(tmpPath2, finalPath2)
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
		info.Path+consts.InfoTmpFileSuffix,
		info.Path+consts.InfoFileSuffix,
		func(f *os.File) error { return sealer.WriteInfoFile(f, src) },
	); err != nil {
		return nil, err
	}

	if err := createAndWrite(
		info.Path+consts.OffsetsTmpFileSuffix,
		info.Path+consts.OffsetsFileSuffix,
		func(f *os.File) error { return sealer.WriteOffsetsFile(f, src) },
	); err != nil {
		return nil, err
	}

	if err := createAndWrite(
		info.Path+consts.IDTmpFileSuffix,
		info.Path+consts.IDFileSuffix,
		func(f *os.File) error { return sealer.WriteIDFile(f, src) },
	); err != nil {
		return nil, err
	}

	if err := createAndWriteBoth(
		info.Path+consts.TokenTmpFileSuffix, info.Path+consts.TokenFileSuffix,
		info.Path+consts.LIDTmpFileSuffix, info.Path+consts.LIDFileSuffix,
		func(tokenF, lidF *os.File) error { return sealer.WriteTokenAndLIDFiles(tokenF, lidF, src) },
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
	info.IndexOnDisk = totalSize

	lidsTable := sealer.LIDsTable()
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
