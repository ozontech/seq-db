package frac

import (
	"os"
	"sync"

	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/metric/stopwatch"
)

type ActiveWriter struct {
	mu   sync.Mutex
	docs *FileWriter
	meta *FileWriter

	docsOffset int64
}

func NewActiveWriter(docsFile, metaFile *os.File, docsSize int64, skipFsync bool) *ActiveWriter {
	return &ActiveWriter{
		docs:       NewFileWriter(docsFile, skipFsync),
		meta:       NewFileWriter(metaFile, skipFsync),
		docsOffset: docsSize,
	}
}

func (a *ActiveWriter) Write(docs, meta []byte, sw *stopwatch.Stopwatch) error {
	m := sw.Start("wait_lock")
	a.mu.Lock()
	m.Stop()

	offset := a.docsOffset

	m = sw.Start("write_docs")
	err := a.docs.Write(docs, sw)
	m.Stop()

	if err != nil {
		return err
	}

	a.docsOffset += int64(len(docs))
	// a.mu.Unlock()

	disk.DocBlock(meta).SetExt1(uint64(len(docs)))
	disk.DocBlock(meta).SetExt2(uint64(offset))

	m = sw.Start("write_meta")
	err = a.meta.Write(meta, sw)
	m.Stop()

	if err != nil {
		return err
	}

	a.mu.Unlock() // todo: move this unlock to the line 52 (after transitional period)

	return nil
}

func (a *ActiveWriter) Stop() {
	a.docs.Stop()
	a.meta.Stop()
}

type ActiveIndexer struct {
	iw *IndexWorkers
}

func NewActiveIndexer(iw *IndexWorkers) *ActiveIndexer {
	return &ActiveIndexer{iw: iw}
}

func (a *ActiveIndexer) Index(frac *Active, metas []byte, wg *sync.WaitGroup, sw *stopwatch.Stopwatch) {
	m := sw.Start("send_index_chan")
	a.iw.In(&IndexTask{
		Pos:   disk.DocBlock(metas).GetExt2(),
		Metas: metas,
		Frac:  frac,
		Wg:    wg,
	})
	m.Stop()
}
