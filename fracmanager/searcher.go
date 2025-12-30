package fracmanager

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type SearcherCfg struct {
	MaxFractionHits       int // the maximum number of fractions used in the search
	FractionsPerIteration int
}

type Searcher struct {
	sem chan struct{}
	cfg SearcherCfg
}

func NewSearcher(maxWorkersNum int, cfg SearcherCfg) *Searcher {
	if maxWorkersNum <= 0 {
		logger.Panic("invalid workers value")
	}
	return &Searcher{
		sem: make(chan struct{}, maxWorkersNum),
		cfg: cfg,
	}
}

func (s *Searcher) SearchDocs(ctx context.Context, fracs []frac.Fraction, params processor.SearchParams, tr *querytracer.Tracer) (*seq.QPR, error) {
	remainingFracs, err := s.prepareFracs(fracs, params)
	if err != nil {
		return nil, err
	}

	subSearchesCnt := 0
	origLimit := params.Limit
	scanAll := params.IsScanAllRequest()

	total := &seq.QPR{
		Histogram: make(map[seq.MID]uint64),
		Aggs:      make([]seq.AggregatableSamples, len(params.AggQ)),
	}

	fracsChunkSize := s.cfg.FractionsPerIteration
	if fracsChunkSize == 0 {
		fracsChunkSize = len(remainingFracs)
	}

	var totalSearchTimeNanos int64
	var totalWaitTimeNanos int64

	for len(remainingFracs) > 0 && (scanAll || params.Limit > 0) {
		subQPRs, searchTimeNanos, waitTimeNanos, err := s.searchDocsAsync(ctx, remainingFracs.Shift(fracsChunkSize), params)
		if err != nil {
			return nil, err
		}

		totalSearchTimeNanos += searchTimeNanos
		totalWaitTimeNanos += waitTimeNanos

		seq.MergeQPRs(total, subQPRs, origLimit, seq.MillisToMID(params.HistInterval), params.Order)

		// reduce the limit on the number of ensured docs in response
		params.Limit = origLimit - calcEnsuredIDsCount(total.IDs, remainingFracs, params.Order)

		subSearchesCnt++
	}

	if tr != nil && tr.Enabled() {
		searchSpan := &querytracer.Span{
			Message:  "search iteratively (cpu time)",
			Duration: time.Duration(totalSearchTimeNanos),
		}
		tr.AddChildWithSpan(searchSpan)
		waitSpan := &querytracer.Span{
			Message:  "waiting goroutines (all cores)",
			Duration: time.Duration(totalWaitTimeNanos),
		}
		tr.AddChildWithSpan(waitSpan)
	}

	searchSubSearches.Observe(float64(subSearchesCnt))
	return total, nil

}

func (s *Searcher) prepareFracs(fracs List, params processor.SearchParams) (List, error) {
	fracs = fracs.FilterInRange(params.From, params.To)
	if s.cfg.MaxFractionHits > 0 && len(fracs) > s.cfg.MaxFractionHits {
		return nil, fmt.Errorf(
			"%w (%d > %d), try decreasing query time range",
			consts.ErrTooManyFractionsHit,
			len(fracs),
			s.cfg.MaxFractionHits,
		)
	}
	fracs.Sort(params.Order)
	return fracs, nil
}

// calcEnsuredIDsCount calculates the number of IDs that are guaranteed to be included in the response
// (they will never be displaced and cut off in the next iterations)
func calcEnsuredIDsCount(ids seq.IDSources, remainingFracs List, order seq.DocsOrder) int {
	if len(remainingFracs) == 0 {
		return len(ids)
	}

	nextFracInfo := remainingFracs[0].Info()

	if order.IsReverse() {
		// ids here are in ASCENDING ORDER
		// we will never get new IDs from the remaining fractions that are less than nextFracInfo.From,
		// so any IDs we have that are less than nextFracInfo.From are guaranteed to be included in the response
		return sort.Search(len(ids), func(i int) bool { return ids[i].ID.MID >= nextFracInfo.From })
	}

	// ids here are in DESCENDING ORDER
	// we will never get new IDs from the remaining fractions that are greater than nextFracInfo.To,
	// so any IDs we have that are greater than nextFracInfo.To are guaranteed to be included in the response
	return sort.Search(len(ids), func(i int) bool { return ids[i].ID.MID <= nextFracInfo.To })
}

func (s *Searcher) searchDocsAsync(ctx context.Context, fracs []frac.Fraction, params processor.SearchParams) ([]*seq.QPR, int64, int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var err error

	once := sync.Once{}
	wg := sync.WaitGroup{}
	qprs := make([]*seq.QPR, len(fracs))

	var totalSearchTimeNanos atomic.Int64
	var totalWaitTimeNanos atomic.Int64

	wgDoneTimes := make([]time.Time, len(fracs))

loop:
	for i, frac := range fracs {
		select {
		case <-ctx.Done():
			once.Do(func() { err = ctx.Err() })
			break loop
		case s.sem <- struct{}{}: // acquire semaphore
			wg.Add(1)
			go func() {
				searchStart := time.Now()
				var fracErr error
				if qprs[i], fracErr = s.fracSearch(ctx, params, frac); fracErr != nil {
					once.Do(func() {
						err = fracErr
						cancel()
					})
				}
				searchDuration := time.Since(searchStart)
				totalSearchTimeNanos.Add(searchDuration.Nanoseconds())
				<-s.sem // release semaphore

				wgDoneTimes[i] = time.Now()
				wg.Done()
			}()
		}
	}

	wg.Wait()
	waitEndTime := time.Now()

	for _, doneTime := range wgDoneTimes {
		waitDuration := waitEndTime.Sub(doneTime)
		if waitDuration > 0 {
			totalWaitTimeNanos.Add(waitDuration.Nanoseconds())
		}
	}

	if err != nil {
		return nil, 0, 0, err
	}

	return qprs, totalSearchTimeNanos.Load(), totalWaitTimeNanos.Load(), nil
}

func (s *Searcher) fracSearch(ctx context.Context, params processor.SearchParams, f frac.Fraction) (_ *seq.QPR, err error) {
	defer func() {
		if panicData := util.RecoverToError(recover(), metric.StorePanics); panicData != nil {
			err = fmt.Errorf("internal error: search panicked on fraction %s, error=%w", f.Info().Name(), panicData)
		}
	}()
	return f.Search(ctx, params)
}
