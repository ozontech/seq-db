package compaction

import (
	"errors"
	"os"

	"github.com/alecthomas/units"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/indexwriter"
	"github.com/ozontech/seq-db/logger"
)

func Merge(filename string, params common.SealParams, srcs ...Source) (*sealed.PreloadedData, error) {
	writer := indexwriter.New(params)
	src := NewMergeSource(filename, srcs)

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

func mergeDocs(filename string, srcs ...Source) error {
	return createAndWrite(
		filename+consts.DocsTmpFileSuffix,
		filename+consts.DocsFileSuffix,
		func(f *os.File) error {
			w := bytespool.AcquireWriterSize(f, int(units.MiB))

			defer func() {
				if err := w.Flush(); err != nil {
					logger.Error(
						"cannot flush compacted .docs file",
						zap.Error(err),
						zap.String("fraction", filename),
					)
				}
				bytespool.ReleaseWriter(w)
			}()

			for _, src := range srcs {
				for block := range src.DocBlock() {
					if _, err := w.Write(block); err != nil {
						return err
					}
				}
			}

			return nil
		},
	)
}
