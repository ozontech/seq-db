package fetch

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type Fetcher struct {
	fetchCh chan *task
}

type task struct {
	docs [][]byte
	err  error
	ids  []seq.ID
	out  chan *task
	frac frac.Fraction
	wg   *sync.WaitGroup
	ctx  context.Context
}

func New(fetchWorkers int) *Fetcher {
	f := &Fetcher{
		fetchCh: make(chan *task, fetchWorkers*2),
	}

	for i := 0; i < fetchWorkers; i++ {
		go f.launchWorker()
	}

	return f
}

func (df *Fetcher) launchWorker() {
	for fetchTask := range df.fetchCh {
		metric.FetchWorkerIDsPerTask.Observe(float64(len(fetchTask.ids)))
		if fetchTask.ctx.Err() == nil {
			fetchTask.docs, fetchTask.err = fetchMultiWithRecover(fetchTask.ctx, fetchTask.frac, fetchTask.ids)
		}

		fetchTask.out <- fetchTask
		fetchTask.wg.Done()
	}
}

func fetchMultiWithRecover(ctx context.Context, f frac.Fraction, ids []seq.ID) (_ [][]byte, err error) {
	defer func() {
		if panicData := recover(); panicData != nil {
			err = fmt.Errorf("internal error: fetch panicked on fraction %s: %s", f.Info().Name(), panicData)
			util.Recover(metric.StorePanics, err)
		}
	}()

	dp, release, ok := f.DataProvider(ctx)
	if !ok {
		return nil, nil
	}
	defer release()

	return dp.Fetch(ids)
}

func filterFracs(fracs fracmanager.FracsList, minMID, maxMID seq.MID) fracmanager.FracsList {
	subset := make(fracmanager.FracsList, 0, len(fracs))
	for _, f := range fracs {
		if f.IsIntersecting(minMID, maxMID) {
			subset = append(subset, f)
		}
	}
	return subset
}

func groupIDsByFraction(idsOrig seq.IDSources, fracs fracmanager.FracsList) map[frac.Fraction][]seq.ID {
	// sort idsOrig to get sorted ids for each fraction due to optimize loading of ids-blocks
	ids := append(seq.IDSources{}, idsOrig...)
	sort.Sort(ids)

	// reduce candidate fractions
	fracSubset := filterFracs(fracs, ids[0].ID.MID, ids[len(ids)-1].ID.MID)

	// group by fraction
	fracIDs := []seq.ID{}
	groups := map[frac.Fraction][]seq.ID{}

	for _, f := range fracSubset {
		i := 0
		fracIDs := fracIDs[:0]
		fracName := f.Info().Name()

		for _, id := range ids {
			if id.Hint == "" {
				ids[i], i = id, i+1 // always check ids with empty hint for all fractions
				if f.Contains(id.ID.MID) {
					fracIDs = append(fracIDs, id.ID)
				}
				continue
			}

			if id.Hint != fracName {
				ids[i], i = id, i+1 // check this id for others fraction next time
				continue
			}

			if !f.Contains(id.ID.MID) {
				logger.Error("fraction from hint does not contain MID",
					zap.String("hint", id.Hint),
					zap.Uint64("mid", uint64(id.ID.MID)))
				metric.FetchHintMisses.Inc()
				continue
			}
			fracIDs = append(fracIDs, id.ID)
		}
		groups[f] = append([]seq.ID{}, fracIDs...)
		ids = ids[:i]
	}

	for _, id := range ids {
		if id.Hint == "" {
			continue
		}
		logger.Error("fraction not found by hint", zap.String("hint", id.Hint))
		metric.FetchHintMisses.Inc()
	}

	return groups
}

func (df *Fetcher) submitFetch(ctx context.Context, fracs fracmanager.FracsList, ids []seq.IDSource) chan *task {
	ch := make(chan *task, len(fracs))

	idsGroups := groupIDsByFraction(ids, fracs)

	go func() {
		wg := &sync.WaitGroup{}
		wg.Add(len(idsGroups))

		for frac, fracIDs := range idsGroups {
			df.fetchCh <- &task{
				ids:  fracIDs,
				out:  ch,
				frac: frac,
				wg:   wg,
				ctx:  ctx,
			}
		}
		wg.Wait()
		close(ch)
	}()

	return ch
}

func (df *Fetcher) FetchDocs(ctx context.Context, fracs fracmanager.FracsList, ids []seq.IDSource) ([][]byte, error) {
	reversPos := map[seq.ID]int{}
	for i, id := range ids {
		reversPos[id.ID] = i
	}

	ch := df.submitFetch(ctx, fracs, ids)

	var errors []error
	docs := make([][]byte, len(ids))
	for task := range ch {
		if task.err != nil {
			errors = append(errors, task.err)
			continue
		}
		for i, id := range task.ids {
			if task.docs[i] != nil {
				docs[reversPos[id]] = task.docs[i]
			}
		}
	}

	return docs, util.CollapseErrors(errors)
}
