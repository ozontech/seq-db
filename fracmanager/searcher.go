package fracmanager

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

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
	SlowLogThreshold      time.Duration
	MaxQprMemory          int // max heap memory a single QPR can use. 0 if no limit set
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
	start := time.Now()
	remainingFracs, err := s.prepareFracs(fracs, params)
	if err != nil {
		return nil, err
	}

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

	var (
		stats       searchStats
		qprMemUsage int
	)

	for len(remainingFracs) > 0 && (scanAll || params.Limit > 0) {
		chunk := remainingFracs.Shift(fracsChunkSize)

		subQPRs, timings, err := s.searchDocsAsync(ctx, chunk, params)
		if err != nil {
			return nil, err
		}
		stats.merge(chunk, subQPRs, timings)

		seq.MergeQPRs(total, subQPRs, origLimit, seq.MillisToMID(params.HistInterval), params.Order)

		qprMemUsage = total.MemUsage()
		if s.cfg.MaxQprMemory > 0 && qprMemUsage > s.cfg.MaxQprMemory {
			return nil, fmt.Errorf(
				"%w: used %d bytes, limit %d",
				consts.ErrMemoryLimitExceeded, qprMemUsage, s.cfg.MaxQprMemory,
			)
		}

		// reduce the limit on the number of ensured docs in response
		params.Limit = origLimit - calcEnsuredIDsCount(total.IDs, remainingFracs, params.Order)
	}

	stats.addToTracer(tr)
	searchSubSearches.Observe(float64(stats.iterations))

	took := time.Since(start)
	if s.cfg.SlowLogThreshold != 0 && took >= s.cfg.SlowLogThreshold {
		fields := []zap.Field{
			zap.Object("params", params),
			zap.Uint64("total", total.Total),
			zap.Int64("took_ms", took.Milliseconds()),
			util.ZapUint64AsSizeStr("qpr_size", uint64(qprMemUsage)),
		}

		logger.Warn(
			"slow search",
			append(fields, stats.zapFields()...)...,
		)
	}

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

func (s *Searcher) searchDocsAsync(
	ctx context.Context,
	fracs []frac.Fraction,
	params processor.SearchParams,
) ([]*seq.QPR, fracTimings, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		err  error
		once sync.Once
		wg   sync.WaitGroup
	)

	qprs := make([]*seq.QPR, len(fracs))
	timings := fracTimings{elapsed: make([]time.Duration, len(fracs))}

loop:
	for i, f := range fracs {
		acquireStart := time.Now()

		select {
		case <-ctx.Done():
			once.Do(func() { err = ctx.Err() })
			break loop
		case s.sem <- struct{}{}: // acquire semaphore
		}

		timings.semaphoreWait += time.Since(acquireStart)

		wg.Go(func() {
			defer func() { <-s.sem }() // release semaphore

			searchStart := time.Now()
			qpr, fracErr := s.fracSearch(ctx, params, f)
			timings.elapsed[i] = time.Since(searchStart)

			if fracErr != nil {
				once.Do(func() {
					err = fracErr
					cancel()
				})
				return
			}
			qprs[i] = qpr
		})
	}

	wg.Wait()
	return qprs, timings, err
}

func (s *Searcher) fracSearch(ctx context.Context, params processor.SearchParams, f frac.Fraction) (_ *seq.QPR, err error) {
	defer func() {
		if panicData := util.RecoverToError(recover(), metric.StorePanics); panicData != nil {
			err = fmt.Errorf("internal error: search panicked on fraction %s, error=%w", f.Info().Name(), panicData)
		}
	}()
	return f.Search(ctx, params)
}
