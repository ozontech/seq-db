package fracmanager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
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

	name string

	indexWg sync.WaitGroup
	sealWg  sync.WaitGroup
}

func newProxyFrac(active *frac.Active, fp *fractionProvider) *proxyFrac {
	return &proxyFrac{
		fp:     fp,
		active: active,
		name:   active.BaseFileName,
	}
}

func (f *proxyFrac) cur() frac.Fraction {
	f.useMu.RLock()
	defer f.useMu.RUnlock()

	if f.active != nil {
		return f.active
	}

	if f.sealed != nil {
		metric.CountersTotal.WithLabelValues("use_sealed_from_active").Inc()
		return f.sealed
	}

	metric.CountersTotal.WithLabelValues("use_empty_from_active").Inc()
	return frac.EmptyFraction
}

func (f *proxyFrac) IsIntersecting(from, to seq.MID) bool {
	return f.cur().IsIntersecting(from, to)
}

func (f *proxyFrac) Contains(mid seq.MID) bool {
	return f.cur().Contains(mid)
}

func (f *proxyFrac) Info() *common.Info {
	return f.cur().Info()
}

func (f *proxyFrac) Fetch(ctx context.Context, ids []seq.ID) ([][]byte, error) {
	return f.cur().Fetch(ctx, ids)
}

func (f *proxyFrac) Search(ctx context.Context, params processor.SearchParams) (*seq.QPR, error) {
	return f.cur().Search(ctx, params)
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
	logger.Info("waiting fraction to stop write...", zap.String("name", f.name))
	f.indexWg.Wait()
	waitTime := util.DurationToUnit(time.Since(start), "s")
	logger.Info("write is stopped", zap.String("name", f.name), zap.Float64("time_wait_s", waitTime))
}

func (f *proxyFrac) Seal() (*frac.Sealed, error) {
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

	sealed, err := f.fp.Seal(active)
	if err != nil {
		return nil, err
	}

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

	// We must compute `isSealing` before
	// we change fraction to read-only.
	isSealing := f.isSealingState()

	// If the object is in active state, switch to read-only mode
	if f.isActiveState() {
		f.readonly = true
	}

	// If sealing is not in progress, we can safely clear the state
	if !isSealing {
		f.sealed = nil
		f.active = nil
	}

	return active, sealed, isSealing
}

func (f *proxyFrac) Offload(ctx context.Context, u storage.Uploader) (bool, error) {
	f.useMu.RLock()

	if f.isSealingState() {
		f.useMu.RUnlock()
		f.sealWg.Wait()

		if c := f.cur(); c != nil {
			return c.Offload(ctx, u)
		}

		return false, nil
	}

	f.useMu.RUnlock()
	return f.cur().Offload(ctx, u)
}

func (f *proxyFrac) Suicide() {
	active, sealed, isSealing := f.trySetSuicided()

	if isSealing {
		f.sealWg.Wait()
		// we can get `sealing` == true only once here
		// next attempt after Wait() should be successful
		active, sealed, _ = f.trySetSuicided()
	}

	if active != nil {
		// Wait for write operations to complete before suiciding
		f.WaitWriteIdle()
		active.Suicide()
	}

	if sealed != nil {
		sealed.Suicide()
	}
}

func (f *proxyFrac) String() string {
	return fmt.Sprintf("%s", f.cur())
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
