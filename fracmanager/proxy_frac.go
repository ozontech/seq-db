package fracmanager

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

var (
	_ frac.Fraction = (*fractionProxy)(nil)
	_ frac.Fraction = (*emptyFraction)(nil)
)

// fractionProxy provides thread-safe access to a fraction with atomic replacement
// Used to switch fraction implementations (active → sealed → remote) without blocking readers.
// Lifecycle: Created for each fraction, persists through state transitions.
type fractionProxy struct {
	mu   sync.RWMutex
	impl frac.Fraction // Current fraction implementation
}

func (p *fractionProxy) Redirect(frac frac.Fraction) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.impl = frac
}

func (p *fractionProxy) Info() *common.Info {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.impl.Info()
}

func (p *fractionProxy) IsIntersecting(from seq.MID, to seq.MID) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.impl.IsIntersecting(from, to)
}

func (p *fractionProxy) Contains(mid seq.MID) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.impl.Contains(mid)
}

func (p *fractionProxy) Fetch(ctx context.Context, ids []seq.ID) ([][]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.impl.Fetch(ctx, ids)
}

func (p *fractionProxy) Search(ctx context.Context, params processor.SearchParams) (*seq.QPR, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.impl.Search(ctx, params)
}

// activeProxy manages an active (writable) fraction
// Tracks pending write operations and provides freeze capability.
// Lifecycle: Created when fraction becomes active, destroyed after sealing.
type activeProxy struct {
	proxy    *fractionProxy // Thread-safe fraction access
	instance *frac.Active   // Actual active fraction instance
	sealed   *frac.Sealed   // Sealed version (set after sealing)

	mu sync.RWMutex   // Protects readonly state
	wg sync.WaitGroup // Tracks pending write operations

	frozen bool // Whether fraction is frozen for writes
}

func newActiveProxy(active *frac.Active) *activeProxy {
	return &activeProxy{
		proxy:    &fractionProxy{impl: active},
		instance: active,
	}
}

// Append adds documents to the active fraction
func (p *activeProxy) Append(docs, meta []byte) error {
	p.mu.RLock()
	if p.frozen {
		p.mu.RUnlock()
		return errors.New("fraction is not writable")
	}
	p.wg.Add(1) // Important: wg.Add() inside lock to prevent race with WaitWriteIdle()
	p.mu.RUnlock()

	return p.instance.Append(docs, meta, &p.wg)
}

// WaitWriteIdle waits for all pending write operations to complete
// Used before sealing to ensure data consistency.
func (p *activeProxy) WaitWriteIdle() {
	start := time.Now()
	logger.Info("waiting fraction to stop write...", zap.String("name", p.instance.BaseFileName))
	p.wg.Wait()
	waitTime := util.DurationToUnit(time.Since(start), "s")
	logger.Info("write is stopped",
		zap.String("name", p.instance.BaseFileName),
		zap.Float64("time_wait_s", waitTime))
}

// Freeze marks the fraction as read-only and prevents new writes from starting after freeze.
func (p *activeProxy) Freeze() error {
	p.mu.Lock()
	if p.frozen {
		p.mu.Unlock()
		return errors.New("fraction is already freezed")
	}
	p.frozen = true
	p.mu.Unlock()

	p.WaitWriteIdle()

	return nil
}

// sealedProxy represents a sealed fraction that may be offloaded
// Tracks both local sealed instance and remote version if offloaded.
type sealedProxy struct {
	proxy    *fractionProxy // Thread-safe fraction access
	instance *frac.Sealed   // Local sealed fraction
	remote   *frac.Remote   // Remote version (if offloaded)
}

// remoteProxy represents an offloaded fraction
type remoteProxy struct {
	proxy    *fractionProxy // Thread-safe fraction access
	instance *frac.Remote   // Remote fraction instance
}

// emptyFraction represents a missing or deleted fraction
// Returns empty results for all operations.
// Used as placeholder when fraction is removed but references still exist.
type emptyFraction struct {
}

func (emptyFraction) Info() *common.Info {
	return &common.Info{
		Path: "empty",
		From: math.MaxUint64,
		To:   0,
	}
}

func (emptyFraction) IsIntersecting(from seq.MID, to seq.MID) bool {
	return false
}

func (emptyFraction) Contains(mid seq.MID) bool {
	return false
}

func (emptyFraction) Fetch(ctx context.Context, ids []seq.ID) ([][]byte, error) {
	return nil, nil
}

func (emptyFraction) Search(_ context.Context, params processor.SearchParams) (*seq.QPR, error) {
	metric.CountersTotal.WithLabelValues("empty_data_provider").Inc()
	return &seq.QPR{Aggs: make([]seq.AggregatableSamples, len(params.AggQ))}, nil
}
