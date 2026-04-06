package frac

import (
	"os"

	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/storage"
)

type ActiveWriter struct {
	docs *storage.FileWriter
	meta MetaWriter
}

type MetaWriter interface {
	Write(data storage.WalBlock, sw *stopwatch.Stopwatch) (int64, error)
	Stop()
}

// NewActiveWriter creates a writer for *.wal files
func NewActiveWriter(docsFile, walFile *os.File, docsOffset, walOffset int64, skipFsync bool) *ActiveWriter {
	return &ActiveWriter{
		docs: storage.NewFileWriter(docsFile, docsOffset, skipFsync),
		meta: storage.NewWalWriter(walFile, walOffset, skipFsync),
	}
}

// NewActiveWriterLegacy creates a writer for *.meta files
func NewActiveWriterLegacy(docsFile, metaFile *os.File, docsOffset, metaOffset int64, skipFsync bool) *ActiveWriter {
	return &ActiveWriter{
		docs: storage.NewFileWriter(docsFile, docsOffset, skipFsync),
		meta: NewLegacyMetaWriter(storage.NewFileWriter(metaFile, metaOffset, skipFsync)),
	}
}

func (a *ActiveWriter) Write(docs storage.DocBlock, meta storage.WalBlock, sw *stopwatch.Stopwatch) error {
	m := sw.Start("write_docs")
	offset, err := a.docs.Write(docs, sw)
	m.Stop()

	if err != nil {
		return err
	}

	meta.SetDocsOffset(uint64(offset))

	m = sw.Start("write_meta")
	_, err = a.meta.Write(meta, sw)
	m.Stop()

	return err
}

func (a *ActiveWriter) Stop() {
	a.docs.Stop()
	a.meta.Stop()
}
