package frac

import (
	"os"

	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/storage"
)

type ActiveWriter struct {
	docs *storage.FileWriter
	wal  *storage.WalWriter
}

// NewActiveWriter creates a writer for *.wal files
func NewActiveWriter(docsFile, walFile *os.File, docsOffset, walOffset int64, skipFsync bool) *ActiveWriter {
	return &ActiveWriter{
		docs: storage.NewFileWriter(docsFile, docsOffset, skipFsync),
		wal:  storage.NewWalWriter(walFile, walOffset, skipFsync),
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
	_, err = a.wal.Write(meta, sw)
	m.Stop()

	return err
}

func (a *ActiveWriter) Stop() {
	a.docs.Stop()
	a.wal.Stop()
}
