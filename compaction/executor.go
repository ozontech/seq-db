package compaction

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/logger"
)

type Executor struct {
	params common.SealParams

	workers int
	wg      sync.WaitGroup

	p *planner
}

func NewExecutor(workers int, params common.SealParams, p *planner) *Executor {
	e := Executor{workers: workers, p: p, params: params}
	e.init()
	return &e
}

func (e *Executor) Stop() {
	e.p.stop()
	e.wg.Wait()
}

func (e *Executor) init() {
	for range e.workers {
		e.wg.Go(func() {
			for t := range e.p.tasks {
				compactionInflight.Inc()

				start := time.Now()
				result, err := e.compact(t)
				compactionDurationSeconds.Observe(time.Since(start).Seconds())

				t.onComplete(result, err)
				compactionInflight.Dec()
			}
		})
	}
}

func (e *Executor) compact(t task) (*sealed.PreloadedData, error) {
	var (
		names []string
		srcs  []Source
	)

	for _, f := range t.snapshot.Fractions() {
		names = append(names, f.Info().Name())
		srcs = append(srcs, frac.NewSealedSource(f))
		compactionBytesTotal.Add(float64(f.Info().IndexOnDisk))
	}

	logger.Info(
		"compacting fractions",
		zap.Time("bin", t.bin),
		zap.Strings("names", names),
	)

	preloaded, err := Merge(t.filename, e.params, srcs...)
	return preloaded, err
}
