package compaction

import (
	"errors"
	"os"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/indexwriter"
)

func Merge(filename string, params common.SealParams, srcs ...Source) (*sealed.PreloadedData, error) {
	w := indexwriter.New(params)
	src := NewMergeSource(filename, srcs)

	if err := createAndWrite(
		filename+consts.OffsetsTmpFileSuffix,
		filename+consts.OffsetsFileSuffix,
		func(f *os.File) error { return w.WriteOffsetsFile(f, src) },
	); err != nil {
		return nil, err
	}

	if err := createAndWrite(
		filename+consts.IDTmpFileSuffix,
		filename+consts.IDFileSuffix,
		func(f *os.File) error { return w.WriteIDFile(f, src) },
	); err != nil {
		return nil, err
	}

	if err := createAndWriteBoth(
		filename+consts.TokenTmpFileSuffix,
		filename+consts.TokenFileSuffix,
		filename+consts.LIDTmpFileSuffix,
		filename+consts.LIDFileSuffix,
		func(tf, lf *os.File) error { return w.WriteTokenTriplet(tf, lf, src) },
	); err != nil {
		return nil, err
	}

	if err := createAndWrite(
		filename+consts.InfoTmpFileSuffix,
		filename+consts.InfoFileSuffix,
		func(f *os.File) error { return w.WriteInfoFile(f, src) },
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

	lidsTable := w.LIDsTable()
	preloaded := &sealed.PreloadedData{
		Info:       info,
		TokenTable: w.TokenTable(),
		BlocksData: sealed.BlocksData{
			LIDsTable:     &lidsTable,
			IDsTable:      w.IDsTable(),
			BlocksOffsets: src.BlockOffsets(),
		},
	}

	return preloaded, nil
}

func mergeDocs(filename string, srcs ...Source) error {
	return createAndWrite(
		filename+consts.DocsTmpFileSuffix,
		filename+consts.DocsFileSuffix,
		func(f *os.File) error {
			var docsSize uint64
			for _, src := range srcs {
				for loc, err := range src.DocBlocks() {
					if err != nil {
						return err
					}

					payload, offset := loc.First, loc.Second
					if _, err := f.WriteAt(payload, int64(offset+docsSize)); err != nil {
						return err
					}
				}

				docsSize += src.Info().DocsOnDisk
			}

			return nil
		},
	)
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
