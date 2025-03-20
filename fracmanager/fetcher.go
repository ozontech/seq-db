package fracmanager

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

var (
	fetcherStagesSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "stages_seconds",
		Buckets:   metric.SecondsBuckets,
	}, []string{"stage"})
	fetcherIDsPerFraction = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "ids_per_fraction",
	})
	fetcherWithHints = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "requests_with_hints",
	})
	fetcherWithoutHint = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "requests_without_hints",
	})
	fetcherHintMisses = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "fetcher",
		Name:      "hint_misses",
	})
)

type Fetcher struct {
	sem chan struct{}
}

func NewFetcher(maxWorkersNum int) *Fetcher {
	if maxWorkersNum <= 0 {
		logger.Panic("invalid workers value")
	}
	return &Fetcher{
		sem: make(chan struct{}, maxWorkersNum),
	}
}

func (f *Fetcher) FetchDocs(ctx context.Context, fracs List, ids []seq.IDSource) ([][]byte, error) {
	sw := stopwatch.New()

	m := sw.Start("fill_revers_pos")
	reversPos := map[seq.ID]int{}
	for i, id := range ids {
		reversPos[id.ID] = i
	}
	m.Stop()

	m = sw.Start("group_ids_by_frac")
	fracs, idsByFrac := groupIDsByFraction(ids, fracs)
	m.Stop()

	m = sw.Start("fetch_async")
	docsByFracs, err := f.fetchDocsAsync(ctx, fracs, idsByFrac)
	m.Stop()

	// arrange the result in the original order of ids
	m = sw.Start("arrange_order")
	result := make([][]byte, len(ids))
	for i := range docsByFracs {
		for j := range docsByFracs[i] {
			if docsByFracs[i][j] != nil { // doc can be nil if we don't find corresponding id in corresponding fraction
				result[reversPos[idsByFrac[i][j]]] = docsByFracs[i][j]
			}
		}
	}
	m.Stop()

	sw.Export(fetcherStagesSeconds)

	return result, err
}

func (f *Fetcher) fetchDocsAsync(ctx context.Context, fracs []frac.Fraction, idsByFrac [][]seq.ID) ([][][]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var err error

	once := sync.Once{}
	wg := sync.WaitGroup{}
	docs := make([][][]byte, len(fracs))

loop:
	for i, frac := range fracs {
		select {
		case <-ctx.Done():
			once.Do(func() { err = ctx.Err() })
			break loop
		case f.sem <- struct{}{}: // acquire semaphore
			wg.Add(1)
			go func() {
				var fracErr error
				if docs[i], fracErr = fracFetch(ctx, frac, idsByFrac[i]); fracErr != nil {
					once.Do(func() {
						err = fracErr
						cancel()
					})
				}
				<-f.sem // release semaphore
				wg.Done()
			}()
		}
	}

	wg.Wait()

	if err != nil {
		return nil, err
	}

	return docs, nil
}

func fracFetch(ctx context.Context, f frac.Fraction, ids []seq.ID) (_ [][]byte, err error) {
	defer func() {
		if panicData := util.RecoverToError(recover(), metric.StorePanics); panicData != nil {
			err = fmt.Errorf("internal error: fetch panicked on fraction %s, error=%w", f.Info().Name(), panicData)
		}
	}()

	dp, release := f.DataProvider(ctx)
	defer release()

	return dp.Fetch(ids)
}

func sortIDs(idsOrig seq.IDSources) (seq.IDSources, seq.MID, seq.MID) {
	// we expect that idsOrig may already be sorted.
	// both direction either asc or desc suit us.
	// so we try to guess the sort order to minimize permutations.
	last := len(idsOrig) - 1
	ids := append(seq.IDSources{}, idsOrig...)

	if seq.Less(ids[0].ID, ids[last].ID) {
		sort.Sort(ids)
		return ids, ids[0].ID.MID, ids[last].ID.MID
	}

	sort.Sort(sort.Reverse(ids))
	return ids, ids[last].ID.MID, ids[0].ID.MID
}

func groupIDsByFraction(idsOrig seq.IDSources, fracsIn List) (List, [][]seq.ID) {
	// sort idsOrig to get sorted ids for each faction to optimize loading of ids-blocks
	ids, minMID, maxMID := sortIDs(idsOrig)

	idsBuf := []seq.ID{}
	fracsOut := fracsIn.FilterInRange(minMID, maxMID) // reduce candidate fractions
	idsByFracs := make([][]seq.ID, 0, len(fracsOut))

	// stats
	withHintsCnt := 0
	hintMissesCnt := 0

	l := 0
	// Here we group `IDs` by factions. Each ID can have a `Hint` - a hint in which faction it should be found.
	// In this case, such an ID falls into only one single group of this faction.
	// If the ID has no hint, then it can potentially end up in any faction for which `From < ID.MID < To`
	for _, f := range fracsOut {
		i := 0
		idsBuf = idsBuf[:0]
		fracName := f.Info().Name()

		for _, id := range ids {
			if id.Hint == "" {
				ids[i], i = id, i+1 // always check ids with empty hint for all fractions
				if f.Contains(id.ID.MID) {
					idsBuf = append(idsBuf, id.ID)
				}
				continue
			}

			if id.Hint != fracName {
				ids[i], i = id, i+1 // check this id for others fraction next time
				continue
			}

			withHintsCnt++
			if !f.Contains(id.ID.MID) {
				logger.Error("fraction from hint does not contain MID",
					zap.String("hint", id.Hint),
					zap.Uint64("mid", uint64(id.ID.MID)))
				hintMissesCnt++
				continue
			}
			idsBuf = append(idsBuf, id.ID)
		}
		if len(idsBuf) > 0 {
			fracsOut[l] = f
			idsByFracs = append(idsByFracs, append([]seq.ID{}, idsBuf...))
			fetcherIDsPerFraction.Observe(float64(len(idsBuf)))
			l++
		}
		ids = ids[:i]
	}

	// By this point, we should have no IDs with `Hints` left in our list.
	// Otherwise, we either don't have such a faction or the condition `From < ID.MID < To` was not met.
	for _, id := range ids {
		if id.Hint == "" {
			continue
		}
		logger.Error("fraction not found by hint", zap.String("hint", id.Hint))
		withHintsCnt++
		hintMissesCnt++
	}

	fetcherHintMisses.Add(float64(hintMissesCnt))
	fetcherWithHints.Add(float64(withHintsCnt))
	fetcherWithoutHint.Add(float64(len(idsOrig) - withHintsCnt))

	return fracsOut[:l], idsByFracs
}
