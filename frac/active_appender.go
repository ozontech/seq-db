package frac

import (
	"os"
	"sync"

	"go.uber.org/atomic"

	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
)

type ActiveAppender struct {
	inCh          chan<- writeTask
	docsToMetasCh chan<- writeTask
	iw            *IndexWorkers
}

type writeTaskBase struct {
	data disk.DocBlock
}

func (wt *writeTaskBase) fetchBlock() disk.DocBlock {
	data := wt.data
	wt.data = nil // release buffer immediately
	return data
}

type docsWriteTask struct {
	writeTaskBase
	outCh   chan<- writeTask
	payload writeTask
}

func (wt *docsWriteTask) done() {
	wt.outCh <- wt.payload
}

type metaWriteTask struct {
	writeTaskBase
	indexTask *IndexTask
	wg        sync.WaitGroup
}

func (wt *metaWriteTask) done() {
	setPos(wt.indexTask)
	wt.wg.Done()
}

func setPos(t *IndexTask) {
	t.Pos = t.Frac.UpdateDiskStats(t.GetDocsLen(), t.GetMetaLen())
}

func StartAppender(docsFile, metasFile *os.File, size int, skipFsync bool, iw *IndexWorkers) ActiveAppender {
	inCh := make(chan writeTask, size)          // closed externally by ActiveAppender.Stop
	docsToMetasCh := make(chan writeTask, size) // closed by us
	wgDocsWorker := &sync.WaitGroup{}           // to close chan
	wgDocsWorker.Add(1)
	if skipFsync {
		startWriteWorkerWithoutFsync(docsFile, inCh, wgDocsWorker, "docs")
		startWriteWorkerWithoutFsync(metasFile, docsToMetasCh, nil, "metas")
	} else {
		// we first need to commit docs file, before we start writing to meta
		// otherwise fraction may become corrupted and will fail during replay
		startWriteWorker(docsFile, inCh, wgDocsWorker, "docs")
		startWriteWorker(metasFile, docsToMetasCh, nil, "metas")
	}

	go func() {
		wgDocsWorker.Wait()
		close(docsToMetasCh)
	}()

	return ActiveAppender{
		inCh:          inCh,
		docsToMetasCh: docsToMetasCh,
		iw:            iw,
	}
}

func (a *ActiveAppender) In(frac *Active, docs, metas []byte, writeQueue *atomic.Uint64, appendQueue *atomic.Uint32) {
	task := &IndexTask{
		DocsLen:     uint64(len(docs)),
		Metas:       metas,
		Frac:        frac,
		AppendQueue: appendQueue,
	}

	sw := stopwatch.New()
	m := sw.Start("send_write_chan")
	metasTask := metaWriteTask{
		writeTaskBase: writeTaskBase{
			data: metas,
		},
		indexTask: task,
	}
	docsTask := docsWriteTask{
		writeTaskBase: writeTaskBase{
			data: docs,
		},
		outCh:   a.docsToMetasCh,
		payload: &metasTask,
	}
	metasTask.wg.Add(1)
	a.inCh <- &docsTask
	m.Stop()

	m = sw.Start("wait_write_worker")
	metasTask.wg.Wait()
	m.Stop()

	m = sw.Start("send_index_chan")
	a.iw.In(task)
	m.Stop()

	writeQueue.Dec()

	sw.Export(metric.BulkStagesSeconds)
}

func (a *ActiveAppender) InReplay(frac *Active, docsLen uint64, metas []byte, appendQueue *atomic.Uint32) {
	task := &IndexTask{
		DocsLen:     docsLen,
		Metas:       metas,
		Frac:        frac,
		AppendQueue: appendQueue,
	}

	setPos(task)
	a.iw.In(task)
}

func (a *ActiveAppender) Stop() {
	// we only need to close the input chan
	// all goroutines and intermediate and output channels will be finished and closed sequentially
	close(a.inCh)
}
