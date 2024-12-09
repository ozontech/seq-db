package fetch

import (
	"context"
	"fmt"
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

type DocFetcherOld struct {
	fetchCh chan *TaskOld
}

type TaskOld struct {
	Docs    [][]byte
	Err     error
	IDs     []seq.IDSource
	Out     chan *TaskOld
	Fracs   fracmanager.FracsList
	wg      *sync.WaitGroup
	Context context.Context
}

func NewDocumentFetcherOld(fetchWorkers int) *DocFetcherOld {
	f := &DocFetcherOld{
		fetchCh: make(chan *TaskOld, fetchWorkers*2),
	}

	for i := 0; i < fetchWorkers; i++ {
		go f.launchWorker()
	}

	return f
}

func (df *DocFetcherOld) launchWorker() {
	docsBuf := make([]byte, 0)

	midCache := frac.NewUnpackCache()
	ridCache := frac.NewUnpackCache()
	for fetchTask := range df.fetchCh {
		metric.FetchWorkerIDsPerTask.Observe(float64(len(fetchTask.IDs)))
		if fetchTask.Context.Err() == nil {
			fetchTask.Docs, docsBuf, fetchTask.Err = df.fetchMany(fetchTask, docsBuf, midCache, ridCache)
		}

		fetchTask.Out <- fetchTask
		fetchTask.wg.Done()
	}
}

func (df *DocFetcherOld) submitFetch(ctx context.Context, fracs fracmanager.FracsList, ids []seq.IDSource) chan *TaskOld {
	ch := make(chan *TaskOld, len(ids))
	go func() {
		parts := make([][]seq.IDSource, 0, conf.FetchWorkers*4)
		if len(ids) < conf.FetchWorkers*4 {
			parts = append(parts, ids)
		} else {
			partLen := len(ids) / conf.FetchWorkers
			x := 0
			for x+partLen < len(ids) {
				parts = append(parts, ids[x:x+partLen])
				x += partLen
			}
			parts = append(parts, ids[x:])
		}

		wg := &sync.WaitGroup{}
		wg.Add(len(parts))
		for _, part := range parts {
			df.fetchCh <- &TaskOld{
				IDs:     part,
				Out:     ch,
				Fracs:   fracs,
				wg:      wg,
				Context: ctx,
			}
		}
		wg.Wait()
		close(ch)
	}()

	return ch
}

func (df *DocFetcherOld) FetchDocs(ctx context.Context, fracs fracmanager.FracsList, ids []seq.IDSource) (map[string][]byte, error) {
	docs := make(map[string][]byte, len(ids))
	ch := df.submitFetch(ctx, fracs, ids)

	var errors []error
	for task := range ch {
		if task.Err != nil {
			errors = append(errors, task.Err)
			continue
		}
		for i, id := range task.IDs {
			if i >= len(task.Docs) {
				err := fmt.Errorf("more task ids than docs, docs_len=%d, task_ids_len=%d", len(task.Docs), len(task.IDs))
				logger.Error("something went wrong while fetching", zap.Error(err))
				errors = append(errors, err)
				break
			}
			docs[id.ID.String()] = task.Docs[i]
		}
	}

	return docs, util.CollapseErrors(errors)
}

func (df *DocFetcherOld) fetchMany(fetchTask *TaskOld, docsBuf []byte, midCache, ridCache *frac.UnpackCache) ([][]byte, []byte, error) {
	docs := make([][]byte, 0, len(fetchTask.IDs))
	for _, id := range fetchTask.IDs {
		var err error
		var doc []byte

		fetchFunc := df.fetchOne
		if id.Hint != "" {
			fetchFunc = df.fetchOneWithHint
		}

		doc, docsBuf, err = fetchFunc(id, fetchTask.Fracs, docsBuf, midCache, ridCache)
		if err != nil {
			return docs, docsBuf, err
		}
		docs = append(docs, doc)
	}

	return docs, docsBuf, nil
}

func (df *DocFetcherOld) fetchOneWithHint(
	id seq.IDSource,
	fracs fracmanager.FracsList,
	docsBuf []byte,
	midCache, ridCache *frac.UnpackCache,
) ([]byte, []byte, error) {
	metric.FetchWithHints.Inc()
	idTime := seq.ExtractMID(id.ID)

	t := tracer.New()
	m1 := t.Start("fetch_worker")
	defer func() {
		m1.Stop()
		t.UpdateMetric(metric.FetchWorkerStagesSeconds)
	}()

	// has hint
	fracInstance := fracs.FindByName(id.Hint)
	if fracInstance == nil {
		logger.Error("fraction not found by hint",
			zap.String("hint", id.Hint))
		metric.FetchHintMisses.Inc()
		return nil, docsBuf, nil
	}
	if !fracInstance.Contains(idTime) {
		logger.Error("fraction from hint does not contain MID",
			zap.String("hint", id.Hint),
			zap.Uint64("mid", uint64(idTime)))
		metric.FetchHintMisses.Inc()

		return nil, docsBuf, nil
	}

	var err error
	var doc []byte
	m2 := t.Start("fetch_fraction")
	doc, docsBuf, err = fetchWithRecover(fracInstance, id.ID, docsBuf, midCache, ridCache)
	if err != nil {
		return doc, docsBuf, err
	}
	m2.Stop()
	if doc != nil {
		metric.FetchWorkerFracsPerTask.Observe(float64(1))
		return doc, docsBuf, nil
	}

	return nil, docsBuf, nil
}

func (df *DocFetcherOld) fetchOne(
	id seq.IDSource,
	fracs fracmanager.FracsList,
	docsBuf []byte,
	midCache, ridCache *frac.UnpackCache,
) ([]byte, []byte, error) {
	metric.FetchWithoutHint.Inc()
	idTime := seq.ExtractMID(id.ID)

	t := tracer.New()
	m1 := t.Start("fetch_worker")
	fracsTouched := 0
	defer func() {
		m1.Stop()
		metric.FetchWorkerFracsPerTask.Observe(float64(fracsTouched))
		t.UpdateMetric(metric.FetchWorkerStagesSeconds)
	}()

	for i := len(fracs) - 1; i >= 0; i-- {
		if !fracs[i].Contains(idTime) {
			continue
		}
		fracsTouched++
		var err error
		var doc []byte
		m2 := t.Start("fetch_fraction")
		if doc, docsBuf, err = fetchWithRecover(fracs[i], id.ID, docsBuf, midCache, ridCache); err != nil {
			return doc, docsBuf, err
		}
		m2.Stop()
		if doc != nil {
			return doc, docsBuf, nil
		}
	}

	return nil, docsBuf, nil
}
