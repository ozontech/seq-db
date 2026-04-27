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

// Seal writes five index files (.info, .token, .offsets, .id, .lid) for the fraction
// and returns PreloadedData for fast initialization of the sealed fraction.
func Seal(src Source, params common.SealParams) (*sealed.PreloadedData, error) {
	info := src.Info()

	if info.To == 0 {
		return nil, errors.New("sealing of an empty active fraction is not supported")
	}

	w := indexwriter.New(params)
	if err := createAndWrite(
		info.Path+consts.OffsetsTmpFileSuffix,
		info.Path+consts.OffsetsFileSuffix,
		func(f *os.File) error { return w.WriteOffsetsFile(f, src) },
	); err != nil {
		return nil, err
	}

	if err := createAndWrite(
		info.Path+consts.IDTmpFileSuffix,
		info.Path+consts.IDFileSuffix,
		func(f *os.File) error { return w.WriteIDFile(f, src) },
	); err != nil {
		return nil, err
	}

	if err := createAndWriteBoth(
		info.Path+consts.TokenTmpFileSuffix, info.Path+consts.TokenFileSuffix,
		info.Path+consts.LIDTmpFileSuffix, info.Path+consts.LIDFileSuffix,
		func(tokenF, lidF *os.File) error { return w.WriteTokenTriplet(tokenF, lidF, src) },
	); err != nil {
		return nil, err
	}

	if err := createAndWrite(
		info.Path+consts.InfoTmpFileSuffix,
		info.Path+consts.InfoFileSuffix,
		func(f *os.File) error { return w.WriteInfoFile(f, src) },
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
	lidsTable := w.LIDsTable()

	preloaded := &sealed.PreloadedData{
		Info:       info,
		TokenTable: w.TokenTable(),
		BlocksData: sealed.BlocksData{
			IDsTable:      w.IDsTable(),
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

func createAndWrite(tmp, final string, write func(*os.File) error) error {
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
	atmp, afinal,
	btmp, bfinal string,
	write func(*os.File, *os.File) error,
) error {
	a, err := os.Create(atmp)
	if err != nil {
		return err
	}

	b, err := os.Create(btmp)
	if err != nil {
		a.Close()
		return err
	}

	writeErr := write(a, b)
	if err := errors.Join(writeErr, syncAndClose(a), syncAndClose(b)); err != nil {
		return err
	}

	if err := os.Rename(atmp, afinal); err != nil {
		return err
	}

	return os.Rename(btmp, bfinal)
}
