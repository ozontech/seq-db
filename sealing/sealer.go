package sealing

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/indexwriter"
	"github.com/ozontech/seq-db/util"
)

// Source defines the contract for data sources that can be sealed.
// Provides access to all necessary data components for index creation.
type Source = indexwriter.Source

func syncAndClose(f *os.File) error {
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func createAndWrite(tmpPath, finalPath string, write func(*os.File) error) error {
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	if err := errors.Join(write(f), syncAndClose(f)); err != nil {
		return err
	}

	return os.Rename(tmpPath, finalPath)
}

func createAndWriteBoth(
	tmpPath1, finalPath1,
	tmpPath2, finalPath2 string,
	write func(*os.File, *os.File) error,
) error {
	f1, err := os.Create(tmpPath1)
	if err != nil {
		return err
	}

	f2, err := os.Create(tmpPath2)
	if err != nil {
		f1.Close()
		return err
	}

	writeErr := write(f1, f2)
	if err := errors.Join(writeErr, syncAndClose(f1), syncAndClose(f2)); err != nil {
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

	sealer := indexwriter.New(params)

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

	info.IndexOnDisk = 0
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
		info.IndexOnDisk += uint64(st.Size())
	}

	lidsTable := sealer.LIDsTable()
	preloaded := &sealed.PreloadedData{
		Info:       info,
		TokenTable: sealer.TokenTable(),
		BlocksData: sealed.BlocksData{
			LIDsTable:     &lidsTable,
			IDsTable:      sealer.IDsTable(),
			BlocksOffsets: src.BlockOffsets(),
		},
	}

	return preloaded, nil
}
