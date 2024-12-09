package fetch

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/tracer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type Task struct {
	Docs    [][]byte
	Err     error
	IDs     []seq.ID
	Out     chan *Task
	Frac    frac.Fraction
	wg      *sync.WaitGroup
	Context context.Context
}

type DocFetcherByFraction struct {
	fetchCh chan *Task
}

func NewDocumentFetcher(fetchWorkers int) *DocFetcherByFraction {
	f := &DocFetcherByFraction{
		fetchCh: make(chan *Task, fetchWorkers*2),
	}

	for i := 0; i < fetchWorkers; i++ {
		go f.launchWorker()
	}

	return f
}

func (df *DocFetcherByFraction) launchWorker() {
	docsBuf := make([]byte, 0)

	midCache := frac.NewUnpackCache()
	ridCache := frac.NewUnpackCache()
	t := tracer.New()
	for fetchTask := range df.fetchCh {
		m := t.Start("fetch_worker")
		if fetchTask.Context.Err() == nil {
			fetchTask.Docs, docsBuf, fetchTask.Err = df.fetchMany(fetchTask, docsBuf, midCache, ridCache)
		}
		m.Stop()

		fetchTask.Out <- fetchTask
		fetchTask.wg.Done()

		t.UpdateMetric(metric.FetchWorkerStagesSeconds)
	}
}

func (df *DocFetcherByFraction) fetchMany(fetchTask *Task, docsBuf []byte, midCache, ridCache *frac.UnpackCache) ([][]byte, []byte, error) {
	docs := make([][]byte, 0, len(fetchTask.IDs))
	for _, id := range fetchTask.IDs {
		var err error
		var doc []byte

		doc, docsBuf, err = fetchWithRecover(fetchTask.Frac, id, docsBuf, midCache, ridCache)
		if err != nil {
			return docs, docsBuf, err
		}
		docs = append(docs, doc)
	}

	return docs, docsBuf, nil
}

func splitToBatches(ids []seq.ID) [][]seq.ID {
	batches := make([][]seq.ID, 0, conf.FetchWorkers*2)
	if len(ids) < conf.FetchWorkers*4 {
		batches = append(batches, ids)
	} else {
		partLen := len(ids) / conf.FetchWorkers
		x := 0
		for x+partLen < len(ids) {
			batches = append(batches, ids[x:x+partLen])
			x += partLen
		}
		batches = append(batches, ids[x:])

	}

	return batches
}

func (df *DocFetcherByFraction) submitFetch(ctx context.Context, fracs fracmanager.FracsList, ids []seq.IDSource) chan *Task {
	ch := make(chan *Task)

	go func() {
		wg := &sync.WaitGroup{}
		t := tracer.New()
		m := t.Start("submit_fetch")

		selectedIDs := []seq.ID{}
		for _, currentFrac := range fracs {
			selectedIDs = selectedIDs[:0]
			m1 := t.Start("ids_by_fracs")
			for _, id := range ids {
				if currentFrac.Contains(id.ID.MID) {
					metric.FetchWithoutHint.Inc()
					selectedIDs = append(selectedIDs, id.ID)
				}
			}
			m1.Stop()
			// no ids in current fraction
			if len(selectedIDs) == 0 {
				continue
			}

			m2 := t.Start("ids_batching")
			idsCopy := make([]seq.ID, len(selectedIDs))
			copy(idsCopy, selectedIDs)
			batches := splitToBatches(idsCopy)
			m2.Stop()

			wg.Add(len(batches))

			m3 := t.Start("batches_to_workers")
			for _, batch := range batches {
				df.fetchCh <- &Task{
					IDs:     batch,
					Out:     ch,
					Frac:    currentFrac,
					wg:      wg,
					Context: ctx,
				}
			}
			m3.Stop()
		}
		m.Stop()

		t.UpdateMetric(metric.SubmitFetchStagesSeconds)

		wg.Wait()
		close(ch)
	}()

	return ch
}

func (df *DocFetcherByFraction) FetchDocs(ctx context.Context, fracs fracmanager.FracsList, ids []seq.IDSource) (map[string][]byte, error) {
	docs := make(map[string][]byte, len(ids))

	ch := df.submitFetch(ctx, fracs, ids)
	var errors []error

	for task := range ch {
		if task.Err != nil {
			errors = append(errors, task.Err)
			logger.Error("fetch task finished with error", zap.Error(task.Err))
			continue
		}
		for i, id := range task.IDs {
			if i >= len(task.Docs) {
				logger.Error("something went wrong while fetching")
				break
			}
			docs[id.String()] = task.Docs[i]
		}
	}

	return docs, util.CollapseErrors(errors)
}
