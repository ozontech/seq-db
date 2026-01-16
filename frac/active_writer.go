package frac

import (
	"os"

	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/storage"
)

type ActiveWriter struct {
	docs *FileWriter
	meta MetaWriter
}

type MetaWriter interface {
	Write(data []byte, sw *stopwatch.Stopwatch) (int64, error)
	Stop()
}

func NewActiveWriter(docsFile, walFile *os.File, docsOffset, walOffset int64, skipFsync bool) *ActiveWriter {
	return &ActiveWriter{
		docs: NewFileWriter(docsFile, docsOffset, skipFsync),
		meta: storage.NewWalWriter(walFile, walOffset, skipFsync),
	}
}

func NewActiveWriterLegacy(docsFile, metaFile *os.File, docsOffset, metaOffset int64, skipFsync bool) *ActiveWriter {
	return &ActiveWriter{
		docs: NewFileWriter(docsFile, docsOffset, skipFsync),
		meta: NewFileWriter(metaFile, metaOffset, skipFsync),
	}
}

func (a *ActiveWriter) Write(docs storage.DocBlock, meta storage.MetaBlock, sw *stopwatch.Stopwatch) error {
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
