package compaction

import (
	"errors"
	"os"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/indexwriter"
)

func Merge(filename string, srcs ...Source) (*sealed.PreloadedData, error) {
	src := NewMergeSource(filename, srcs)

	// FIXME(dkharms): [common.SealParams] must be passed into [Merge] function.
	writer := indexwriter.New(common.SealParams{
		IDsZstdLevel:           3,
		LIDsZstdLevel:          3,
		TokenListZstdLevel:     3,
		DocsPositionsZstdLevel: 3,
		TokenTableZstdLevel:    3,
		DocBlocksZstdLevel:     3,
		DocBlockSize:           3,
	})

	if err := createAndWrite(
		filename+consts.OffsetsTmpFileSuffix,
		filename+consts.OffsetsFileSuffix,
		func(f *os.File) error { return writer.WriteOffsetsFile(f, src) },
	); err != nil {
		return nil, err
	}

	if err := createAndWrite(
		filename+consts.IDTmpFileSuffix,
		filename+consts.IDFileSuffix,
		func(f *os.File) error { return writer.WriteIDFile(f, src) },
	); err != nil {
		return nil, err
	}

	if err := createAndWriteBoth(
		filename+consts.TokenTmpFileSuffix,
		filename+consts.TokenFileSuffix,
		filename+consts.LIDTmpFileSuffix,
		filename+consts.LIDFileSuffix,
		func(tf, lf *os.File) error { return writer.WriteTokenTriplet(tf, lf, src) },
	); err != nil {
		return nil, err
	}

	if err := createAndWrite(
		filename+consts.InfoTmpFileSuffix,
		filename+consts.InfoFileSuffix,
		func(f *os.File) error { return writer.WriteInfoFile(f, src) },
	); err != nil {
		return nil, err
	}

	if err := mergeDocs(filename, srcs...); err != nil {
		return nil, err
	}

	info := src.Info()
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

	lidsTable := writer.LIDsTable()
	preloaded := &sealed.PreloadedData{
		Info:       info,
		TokenTable: writer.TokenTable(),
		BlocksData: sealed.BlocksData{
			LIDsTable:     &lidsTable,
			IDsTable:      writer.IDsTable(),
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

// FIXME(dkharms): Create buffered writer for file.
func mergeDocs(filename string, srcs ...Source) error {
	return createAndWrite(
		filename+consts.DocsTmpFileSuffix,
		filename+consts.DocsFileSuffix,
		func(f *os.File) error {
			for _, src := range srcs {
				for block := range src.DocBlock() {
					if _, err := f.Write(block); err != nil {
						return err
					}
				}
			}
			return nil
		},
	)
}
