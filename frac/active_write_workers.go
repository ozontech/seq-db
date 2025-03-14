package frac

import (
	"os"
	"sync"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/util"
)

type writeTask interface {
	fetchBlock() disk.DocBlock
	done()
}

type writeWorker struct {
	file    *os.File
	batchan *util.Batchan[writeTask]
}

func newWriteWorker(file *os.File) *writeWorker {
	return &writeWorker{
		file:    file,
		batchan: util.NewBatchan[writeTask](),
	}
}

func (w *writeWorker) runWrite(inCh <-chan writeTask, name string) {
	sw := stopwatch.New()

	for t := range inCh {
		m := sw.Start(name + " >> write_duration")
		if _, err := w.file.Write(t.fetchBlock()); err != nil {
			logger.Fatal("can't write fraction file", zap.String("file", w.file.Name()), zap.Error(err))
		}
		m.Stop()
		m = sw.Start(name + " >> write_send_duration")
		w.batchan.Send(t)
		m.Stop()

		sw.Export(metric.BulkStagesSeconds)
	}
	w.batchan.Close()
}

func (w *writeWorker) runFsync(name string) {
	sw := stopwatch.New()

	var payload []writeTask
	for {
		payload = w.batchan.Fetch(payload)
		if len(payload) == 0 {
			break
		}

		metric.BulkDiskSyncTasksCount.Observe(float64(len(payload)))

		m := sw.Start(name + " >> fsync")
		if err := w.file.Sync(); err != nil {
			logger.Fatal("error syncing file",
				zap.String("file", w.file.Name()),
				zap.Error(err),
			)
		}
		m.Stop()

		m = sw.Start(name + " >> fsync_done")
		for _, v := range payload {
			v.done()
		}
		m.Stop()

		sw.Export(metric.BulkStagesSeconds)
	}
}

func startWriteWorker(file *os.File, inCh <-chan writeTask, wg *sync.WaitGroup, name string) {
	worker := newWriteWorker(file)
	go worker.runWrite(inCh, name)
	go func() {
		worker.runFsync(name)
		if wg != nil {
			wg.Done()
		}
	}()
}

func startWriteWorkerWithoutFsync(file *os.File, inCh <-chan writeTask, wg *sync.WaitGroup, name string) {
	go func() {
		sw := stopwatch.New()
		for t := range inCh {
			m := sw.Start(name + " >> write_duration")
			if _, err := file.Write(t.fetchBlock()); err != nil {
				logger.Fatal("can't write fraction file", zap.String("file", file.Name()), zap.Error(err))
			}
			m.Stop()

			m = sw.Start(name + " >> write_send_duration")
			t.done()
			m.Stop()

			sw.Export(metric.BulkStagesSeconds)
		}
		if wg != nil {
			wg.Done()
		}
	}()
}
