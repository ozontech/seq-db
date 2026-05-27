package compaction

import (
	"sync"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/logger"
)

type Executor struct {
	workers int
	wg      sync.WaitGroup
	p       *planner
}

// FIXME(dkharms): I need to pass here [common.SealParams].
func NewExecutor(workers int, p *planner) *Executor {
	e := Executor{workers: workers, p: p}
	e.init()
	return &e
}

func (e *Executor) Close() {
	e.p.close()
	e.wg.Wait()
}

func (e *Executor) init() {
	for range e.workers {
		e.wg.Go(func() {
			for t := range e.p.tasks {
				logger.Info(
					"got new compaction task",
					zap.Time("bin", t.bin),
					zap.Any("snapshot", t.snapshot),
				)
				t.onComplete(e.compact(t))
			}
		})
	}
}

func (e *Executor) compact(t task) (result, error) {
	var (
		names []string
		srcs  []Source
	)

	for _, f := range t.snapshot.Fractions() {
		names = append(names, f.Info().Name())
		srcs = append(srcs, frac.NewSealedSource(f))
	}

	logger.Info(
		"compacting fractions",
		zap.Strings("names", names),
	)

	preloaded, err := Merge(t.filename, common.SealParams{}, srcs...)
	return result{filename: t.filename, consumed: t.snapshot, produced: preloaded}, err
}

type noopexecutor struct{}

func (noopexecutor) Compact(t task) (result, error) {
	var (
		sum   int
		cnt   int
		names []string
	)

	for _, f := range t.snapshot.Fractions() {
		cnt += 1
		sum += int(f.Info().IndexOnDisk)
		names = append(names, f.Info().Name())
	}

	logger.Info(
		"picked fractions",
		zap.Any("names", names),
		zap.Int("size", sum),
		zap.Int("count", cnt),
	)

	return result{}, nil
}
