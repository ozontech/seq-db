package searcher

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
)

type Config struct {
	AggLimits             AggLimits
	MaxFractionHits       int // the maximum number of fractions used in the search
	FractionsPerIteration int
}

type Searcher struct {
	sem chan struct{}
	cfg Config
}

func New(maxWorkersNum int, config Config) *Searcher {
	if maxWorkersNum <= 0 {
		logger.Panic("invalid workers value")
	}

	return &Searcher{
		sem: make(chan struct{}, maxWorkersNum),
		cfg: config,
	}
}

func (s *Searcher) SearchDocs(ctx context.Context, params Params, fracs frac.List) (*seq.QPR, error) {
	start := time.Now()

	fracs, err := s.prepareFracs(params, fracs)
	if err != nil {
		return nil, err
	}

	origLimit := params.Limit
	scanAll := params.NeedScanAllRange()

	subQueriesCount := 0

	total := &seq.QPR{
		Histogram: make(map[seq.MID]uint64),
		Aggs:      make([]seq.QPRHistogram, len(params.AggQ)),
	}

	for len(fracs) > 0 && (scanAll || params.Limit > 0) {
		qprs, err := s.searchAsync(ctx, fracs.Pop(s.cfg.FractionsPerIteration), params)
		if err != nil {
			return nil, err
		}

		seq.MergeQPRs(total, flatten(qprs), origLimit, seq.MID(params.HistInterval), params.Order)

		// reduce the limit on the number of ensured ids in response
		params.Limit = origLimit - calcEnsuredIDsCount(total.IDs, fracs, params.Order)

		subQueriesCount++
	}

	searchSubSearches.Observe(float64(subQueriesCount))
	searchEvalDurationSeconds.Observe(time.Since(start).Seconds())

	return total, nil
}

func flatten(in [][]*seq.QPR) []*seq.QPR {
	size := 0
	for _, q := range in {
		size += len(q)
	}
	out := make([]*seq.QPR, 0, size)
	for _, q := range in {
		out = append(out, q...)
	}
	return out
}

// calcEnsuredIDsCount calculates the number of IDs that are guaranteed to be included in the response
// (they will never be displaced and cut off in the next iterations)
func calcEnsuredIDsCount(ids seq.IDSources, remainingFracs frac.List, order seq.DocsOrder) int {
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

func (s *Searcher) prepareFracs(params Params, fracs frac.List) (frac.List, error) {
	fracs = fracs.FilterInRange(params.From, params.To)
	if s.cfg.MaxFractionHits > 0 && len(fracs) > s.cfg.MaxFractionHits {
		metric.RejectedRequests.WithLabelValues("search", "fracs_exceeding").Inc()
		return nil, fmt.Errorf(
			"%w: %d > %d, try decreasing query time range",
			consts.ErrTooManyFracHit,
			len(fracs),
			s.cfg.MaxFractionHits,
		)
	}
	fracs.Sort(params.Order)
	return fracs, nil
}

func (s *Searcher) searchAsync(ctx context.Context, fracs []frac.Fraction, params Params) ([][]*seq.QPR, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var err error

	once := sync.Once{}
	wg := sync.WaitGroup{}
	qprs := make([][]*seq.QPR, len(fracs))
	fracSearcher := newFracSearcher(ctx, params, s.cfg.AggLimits)

loop:
	for i, frac := range fracs {
		select {
		case <-ctx.Done():
			once.Do(func() { err = ctx.Err() })
			break loop
		case s.sem <- struct{}{}: // acquire semaphore
			wg.Add(1)
			go func() {
				var fracErr error
				if qprs[i], fracErr = fracSearcher.Search(frac); fracErr != nil {
					once.Do(func() {
						err = fracErr
						cancel()
					})
				}

				<-s.sem // release semaphore
				wg.Done()
			}()
		}
	}

	wg.Wait()

	if err != nil {
		return nil, err
	}

	return qprs, nil
}
