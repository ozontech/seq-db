package fracmanager

import (
	"errors"
	"sync"
	"time"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
	"go.uber.org/zap"
)

type syncAppender struct {
	instance *frac.Active // Actual active fraction instance

	mu sync.RWMutex   // Protects readonly state
	wg sync.WaitGroup // Tracks pending write operations

	finalized bool // Whether fraction is frozen for writes
}

// Append adds documents to the active fraction
func (p *syncAppender) Append(docs, meta []byte) error {
	p.mu.RLock()
	if p.finalized {
		p.mu.RUnlock()
		return ErrFractionNotWritable
	}
	p.wg.Add(1) // Important: wg.Add() inside lock to prevent race with WaitWriteIdle()
	p.mu.RUnlock()

	return p.instance.Append(docs, meta, &p.wg)
}

// WaitWriteIdle waits for all pending write operations to complete
// Used before sealing to ensure data consistency.
func (p *syncAppender) WaitWriteIdle() {
	start := time.Now()
	logger.Info("waiting fraction to stop write...", zap.String("name", p.instance.BaseFileName))
	p.wg.Wait()
	waitTime := util.DurationToUnit(time.Since(start), "s")
	logger.Info("write is stopped",
		zap.String("name", p.instance.BaseFileName),
		zap.Float64("time_wait_s", waitTime))
}

// Finalize marks the fraction as read-only and prevents new writes from starting after finalize.
func (p *syncAppender) Finalize() error {
	p.mu.Lock()
	if p.finalized {
		p.mu.Unlock()
		return errors.New("fraction is already finalized")
	}
	p.finalized = true
	p.mu.Unlock()

	return nil
}
