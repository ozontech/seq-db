package active_old

import (
	"os"

	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/storage"
)

type Writer struct {
	docs *FileWriter
	meta *FileWriter
}

func NewWriter(docsFile, metaFile *os.File, docsOffset, metaOffset int64, skipFsync bool) *Writer {
	return &Writer{
		docs: NewFileWriter(docsFile, docsOffset, skipFsync),
		meta: NewFileWriter(metaFile, metaOffset, skipFsync),
	}
}

func (a *Writer) Write(docs, meta []byte, sw *stopwatch.Stopwatch) error {
	m := sw.Start("write_docs")
	offset, err := a.docs.Write(docs, sw)
	m.Stop()

	if err != nil {
		return err
	}

	storage.DocBlock(meta).SetExt2(uint64(offset))

	m = sw.Start("write_meta")
	_, err = a.meta.Write(meta, sw)
	m.Stop()

	return err
}

func (a *Writer) Stop() {
	a.docs.Stop()
	a.meta.Stop()
}
