package fracmanager

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
	"go.uber.org/zap"
)

/**
 *   Possible states (only 4):
 *  -----------------------------------------------------------------
 *  |            | Active & Writable | Sealing | Sealed  | Suicided |
 *  -----------------------------------------------------------------
 *  | f.active   | value             | value   | nil     | nil      |
 *  -----------------------------------------------------------------
 *  | f.sealed   | nil               | nil     | value   | nil      |
 *  -----------------------------------------------------------------
 *  | f.readonly | false             | true    |   --    |  --      |
 *  -----------------------------------------------------------------
 *  All other states are impossible.
 */

type transitionFrac struct {
	builder *builder

	useMu    sync.RWMutex
	active   *frac.Active
	sealed   *frac.Sealed
	readonly bool

	indexWg sync.WaitGroup
	sealWg  sync.WaitGroup
}

func (f *transitionFrac) cur() frac.Fraction {
	f.useMu.RLock()
	defer f.useMu.RUnlock()

	if f.sealed == nil {
		return f.active
	}
	return f.sealed
}

func (f *transitionFrac) IsIntersecting(from seq.MID, to seq.MID) bool {
	return f.cur().IsIntersecting(from, to)
}

func (f *transitionFrac) Contains(mid seq.MID) bool {
	return f.cur().Contains(mid)
}

func (f *transitionFrac) Info() *frac.Info {
	return f.cur().Info()
}

func (f *transitionFrac) DataProvider(ctx context.Context) (frac.DataProvider, func()) {
	f.useMu.RLock()
	defer f.useMu.RUnlock()

	if f.active != nil {
		return f.active.DataProvider(ctx)
	}

	if f.sealed != nil {
		metric.CountersTotal.WithLabelValues("use_sealed_from_active").Inc()
		return f.sealed.DataProvider(ctx)
	}

	return frac.EmptyDataProvider{}, func() {}
}

func (f *transitionFrac) canAppend() bool {
	f.useMu.RLock()
	defer f.useMu.RUnlock()

	if !f.isActiveState() {
		return false
	}
	f.indexWg.Add(1) // It is important to place it inside the lock
	return true
}

func (f *transitionFrac) Append(docs, meta []byte) error {
	if !f.canAppend() {
		return errors.New("fraction is not writable")
	}

	f.active.Append(docs, meta, &f.indexWg)

	return nil
}

func (f *transitionFrac) WaitWriteIdle() {
	start := time.Now()
	logger.Info("waiting fraction to stop write...", zap.String("name", f.active.BaseFileName))
	f.indexWg.Wait()
	waitTime := util.DurationToUnit(time.Since(start), "s")
	logger.Info("write is stopped", zap.String("name", f.active.BaseFileName), zap.Float64("time_wait_s", waitTime))
}

func (f *transitionFrac) setSealing() (*frac.Active, error) {
	f.useMu.Lock()
	defer f.useMu.Unlock()

	if !f.isActiveState() {
		return nil, errors.New("fraction is not active")
	}
	f.readonly = true
	f.sealWg.Add(1)
	return f.active, nil
}

func (f *transitionFrac) Seal(params frac.SealParams) (*frac.Sealed, error) {
	active, err := f.setSealing()
	if err != nil {
		return nil, err
	}

	f.WaitWriteIdle()

	preloaded := frac.Seal(active, params)
	sealed := f.builder.NewSealedPreloaded(active.BaseFileName, preloaded)

	f.useMu.Lock()
	f.sealed = sealed
	f.active = nil
	f.useMu.Unlock()

	f.sealWg.Done()

	active.Release(false)

	return sealed, nil
}

// trySetSuicided set suicided state if possible (if not sealing right now)
func (f *transitionFrac) trySetSuicided() (*frac.Active, *frac.Sealed, bool) {
	f.useMu.Lock()
	defer f.useMu.Unlock()

	sealed := f.sealed
	active := f.active

	sealing := f.isSealingState()

	if !sealing {
		f.sealed = nil
		f.active = nil
	}

	return active, sealed, sealing
}

func (f *transitionFrac) Suicide() {
	active, sealed, sealing := f.trySetSuicided()
	if sealing {
		f.sealWg.Wait()
		// we can get `sealing` == true only once here
		// next attempt after Wait() should be successful
		active, sealed, _ = f.trySetSuicided()
	}

	if active != nil {
		f.active.Suicide()
	}

	if sealed != nil {
		sealed.Suicide()
	}
}

func (f *transitionFrac) isActiveState() bool {
	return f.active != nil && f.sealed == nil && !f.readonly
}

func (f *transitionFrac) isSealingState() bool {
	return f.active != nil && f.sealed == nil && f.readonly
}

func (f *transitionFrac) isSuicidedState() bool {
	return f.active == nil && f.sealed == nil
}
