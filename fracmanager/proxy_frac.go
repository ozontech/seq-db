package fracmanager

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

var ErrSealingFractionSuicided = errors.New("sealing fraction is suicided")

/**
 *   Possible states (only 4):
 *  --------------------------------------------------------
 *  |            		| f.active | f.sealed | f.readonly |
 *  --------------------------------------------------------
 *  | Active & Writable |  value   |    nil   |  false     |
 *  --------------------------------------------------------
 *  | Sealing   		|  value   |    nil   |  true      |
 *  --------------------------------------------------------
 *  | Sealed 			|   nil    |  value   |  true      |
 *  --------------------------------------------------------
 *  | Suicided 			|   nil    |   nil    |  true      |
 *  --------------------------------------------------------
 *  All other states are impossible.
 */

type proxyFrac struct {
	fp *fractionProvider

	useMu    sync.RWMutex
	active   *frac.Active
	sealed   *frac.Sealed
	readonly bool

	indexWg sync.WaitGroup
	sealWg  sync.WaitGroup
}

func (f *proxyFrac) cur() frac.Fraction {
	f.useMu.RLock()
	defer f.useMu.RUnlock()

	if f.sealed == nil {
		return f.active
	}
	return f.sealed
}

func (f *proxyFrac) IsIntersecting(from, to seq.MID) bool {
	return f.cur().IsIntersecting(from, to)
}

func (f *proxyFrac) Contains(mid seq.MID) bool {
	return f.cur().Contains(mid)
}

func (f *proxyFrac) Info() *frac.Info {
	return f.cur().Info()
}

func (f *proxyFrac) DataProvider(ctx context.Context) (frac.DataProvider, func()) {
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

func (f *proxyFrac) Append(docs, meta []byte) error {
	f.useMu.RLock()
	if !f.isActiveState() {
		f.useMu.RUnlock()
		return errors.New("fraction is not writable")
	}
	active := f.active
	f.indexWg.Add(1) // It's important to put wg.Add() inside a lock, otherwise we might call WaitWriteIdle() before it
	f.useMu.RUnlock()

	return active.Append(docs, meta, &f.indexWg)
}

func (f *proxyFrac) WaitWriteIdle() {
	start := time.Now()
	logger.Info("waiting fraction to stop write...", zap.String("name", f.active.BaseFileName))
	f.indexWg.Wait()
	waitTime := util.DurationToUnit(time.Since(start), "s")
	logger.Info("write is stopped", zap.String("name", f.active.BaseFileName), zap.Float64("time_wait_s", waitTime))
}

func (f *proxyFrac) Seal(params frac.SealParams) (*frac.Sealed, error) {
	f.useMu.Lock()
	if f.isSuicidedState() {
		f.useMu.Unlock()
		return nil, ErrSealingFractionSuicided
	}
	if !f.isActiveState() {
		f.useMu.Unlock()
		return nil, errors.New("sealing fraction is not active")
	}
	f.readonly = true
	active := f.active
	f.sealWg.Add(1) // It's important to put wg.Add() inside a lock, otherwise we might call wg.Wait() before it
	f.useMu.Unlock()

	f.WaitWriteIdle()

	preloaded, err := frac.Seal(active, params)
	if err != nil {
		return nil, err
	}

	sealed := f.fp.NewSealedPreloaded(active.BaseFileName, preloaded)

	f.useMu.Lock()
	f.sealed = sealed
	f.active = nil
	f.useMu.Unlock()

	f.sealWg.Done()

	active.Release()

	return sealed, nil
}

// trySetSuicided set suicided state if possible (if not sealing right now)
func (f *proxyFrac) trySetSuicided() (*frac.Active, *frac.Sealed, bool) {
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

func (f *proxyFrac) Suicide() {
	active, sealed, sealing := f.trySetSuicided()
	if sealing {
		f.sealWg.Wait()
		// we can get `sealing` == true only once here
		// next attempt after Wait() should be successful
		active, sealed, _ = f.trySetSuicided()
	}

	if active != nil {
		active.Suicide()
	}

	if sealed != nil {
		sealed.Suicide()
	}
}

func (f *proxyFrac) isActiveState() bool {
	return f.active != nil && f.sealed == nil && !f.readonly
}

func (f *proxyFrac) isSealingState() bool {
	return f.active != nil && f.sealed == nil && f.readonly
}

func (f *proxyFrac) isSuicidedState() bool {
	return f.active == nil && f.sealed == nil
}
