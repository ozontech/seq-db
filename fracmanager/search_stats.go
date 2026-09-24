package fracmanager

import (
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
)

const maxFracsSlowSearchLog = 10

type fracTimings struct {
	semaphoreWait time.Duration
	elapsed       []time.Duration
}

type searchStats struct {
	iterations int

	total         time.Duration
	semaphoreWait time.Duration

	min     time.Duration
	fastest string

	max     time.Duration
	slowest string

	matched        int
	unmatched      int
	matchedNames   []string
	unmatchedNames []string
}

func (s *searchStats) merge(fracs []frac.Fraction, qprs []*seq.QPR, timings fracTimings) {
	s.iterations++
	s.semaphoreWait += timings.semaphoreWait

	for i, elapsed := range timings.elapsed {
		if s.fracs() == 0 || elapsed < s.min {
			s.min = elapsed
			s.fastest = fracs[i].Info().Name()
		}

		if elapsed > s.max {
			s.max = elapsed
			s.slowest = fracs[i].Info().Name()
		}

		s.total += elapsed

		if qprs[i].Empty() {
			s.unmatched++
			if len(s.unmatchedNames) < maxFracsSlowSearchLog {
				s.unmatchedNames = append(s.unmatchedNames, fracs[i].Info().Name())
			}
			continue
		}

		s.matched++
		if len(s.matchedNames) < maxFracsSlowSearchLog {
			s.matchedNames = append(s.matchedNames, fracs[i].Info().Name())
		}
	}
}

func (s *searchStats) fracs() int {
	return s.matched + s.unmatched
}

func (s *searchStats) mean() time.Duration {
	if s.fracs() == 0 {
		return 0
	}
	return s.total / time.Duration(s.fracs())
}

func (s *searchStats) addToTracer(tr *querytracer.Tracer) {
	if !tr.Enabled() {
		return
	}

	tr.AddChildWithSpan(&querytracer.Span{
		Message:  "waiting on the search worker semaphore",
		Duration: s.semaphoreWait,
	})

	tr.AddChildWithSpan(&querytracer.Span{
		Message: fmt.Sprintf(
			"fraction search time: %d fractions (%d matched) in %d iterations",
			s.fracs(), s.matched, s.iterations,
		),
		Duration: s.total,
		Children: []*querytracer.Span{
			{Message: "mean", Duration: s.mean()},
			{Message: fmt.Sprintf("min (%s)", s.fastest), Duration: s.min},
			{Message: fmt.Sprintf("max (%s)", s.slowest), Duration: s.max},
		},
	})
}

func (s *searchStats) zapFields() []zap.Field {
	return []zap.Field{
		zap.Int("search_iterations", s.iterations),
		zap.Int("fracs_searched", s.fracs()),
		zap.Int("fracs_matched", s.matched),
		zap.Strings("fracs_matched_sample", s.matchedNames),
		zap.Int("fracs_unmatched", s.unmatched),
		zap.Strings("fracs_unmatched_sample", s.unmatchedNames),
		zap.Int64("semaphore_wait_ms", s.semaphoreWait.Milliseconds()),
		zap.Int64("frac_search_total_ms", s.total.Milliseconds()),
		zap.Int64("frac_search_min_ms", s.min.Milliseconds()),
		zap.String("frac_search_min_frac", s.fastest),
		zap.Int64("frac_search_mean_ms", s.mean().Milliseconds()),
		zap.Int64("frac_search_max_ms", s.max.Milliseconds()),
		zap.String("frac_search_max_frac", s.slowest),
	}
}
