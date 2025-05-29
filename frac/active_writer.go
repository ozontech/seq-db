package frac

import (
	"os"
	"sync"

	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/metric/stopwatch"
)

type ActiveWriter struct {
	mu   sync.Mutex // todo: remove this mutex on next release
	docs *FileWriter
	meta *FileWriter
}

func NewActiveWriter(docsFile, metaFile *os.File, docsOffset, metaOffset int64, skipFsync bool) *ActiveWriter {
	return &ActiveWriter{
		docs: NewFileWriter(docsFile, docsOffset, skipFsync),
		meta: NewFileWriter(metaFile, metaOffset, skipFsync),
	}
}

func (a *ActiveWriter) Write(docs, meta []byte, sw *stopwatch.Stopwatch) error {
	w := sw.Start("wait_lock")
	a.mu.Lock()
	defer a.mu.Unlock()
	w.Stop()

	m := sw.Start("write_docs")
	offset, err := a.docs.Write(docs, sw)
	m.Stop()

	if err != nil {
		return err
	}

	disk.DocBlock(meta).SetExt1(uint64(len(docs)))
	disk.DocBlock(meta).SetExt2(uint64(offset))

	m = sw.Start("write_meta")
	_, err = a.meta.Write(meta, sw)
	m.Stop()

	return err
}

func (a *ActiveWriter) Stop() {
	a.docs.Stop()
	a.meta.Stop()
}
