package fracmanager

import (
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
)

var (
	ErrFractionNotWritable = errors.New("fraction is not writable")
	ErrFractionSuspended   = errors.New("write operations temporarily suspended - database capacity exceeded")
)

type syncAppender struct {
	refCountedActive // Actual active fraction instance

	mu sync.RWMutex   // Protects readonly state
	wg sync.WaitGroup // Tracks pending write operations

	finalized bool // Whether fraction is frozen for writes
	suspended bool // Temporarily suspended for writes
}

// append adds documents to the active fraction
func (a *syncAppender) append(docs, meta []byte) error {
	a.mu.RLock()
	if a.finalized {
		a.mu.RUnlock()
		return ErrFractionNotWritable
	}
	if a.suspended {
		a.mu.RUnlock()
		return ErrFractionSuspended
	}
	a.wg.Add(1) // Important: wg.Add() inside lock to prevent race with WaitWriteIdle()
	a.mu.RUnlock()

	return a.refCountedActive.Append(docs, meta, &a.wg)
}

func (a *syncAppender) isSuspended() bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.suspended
}

func (a *syncAppender) suspend(value bool) {
	a.mu.Lock()
	a.suspended = value
	a.mu.Unlock()
}

// waitWriteIdle waits for all pending write operations to complete
// Used before sealing to ensure data consistency.
func (a *syncAppender) waitWriteIdle() {
	start := time.Now()
	logger.Info("waiting fraction to stop write...", zap.String("name", a.BaseFileName))
	a.wg.Wait()
	waitTime := util.DurationToUnit(time.Since(start), "s")
	logger.Info("write is stopped",
		zap.String("name", a.BaseFileName),
		zap.Float64("time_wait_s", waitTime))
}

// finalize marks the fraction as read-only and prevents new writes from starting after finalize.
func (a *syncAppender) finalize() error {
	a.mu.Lock()
	if a.finalized {
		a.mu.Unlock()
		return errors.New("fraction is already finalized")
	}
	a.finalized = true
	a.mu.Unlock()

	return nil
}
