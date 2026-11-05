package fracmanager

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

const defaultRetryDelay = 2 * time.Second

type remoteCleaner struct {
	ctx       context.Context
	infoCache *fracInfoCache
	pending   sync.Map
}

func newRemoteCleaner(infoCache *fracInfoCache) *remoteCleaner {
	return &remoteCleaner{
		infoCache: infoCache,
	}
}

func (c *remoteCleaner) Start(ctx context.Context, retryDelay time.Duration, wg *sync.WaitGroup) {
	c.ctx = ctx
	if retryDelay <= 0 {
		retryDelay = defaultRetryDelay
	}
	wg.Go(func() { c.run(retryDelay) })
}

func (c *remoteCleaner) Add(remote *refCountedRemote) {
	if c.ctx.Err() != nil {
		return
	}
	c.pending.LoadOrStore(remote.BaseFileName, remote)
}

func (c *remoteCleaner) run(retryDelay time.Duration) {
	ticker := time.NewTicker(retryDelay)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.clean()
		}
	}
}

func (c *remoteCleaner) clean() {
	c.pending.Range(func(name, value any) bool {
		if c.ctx.Err() != nil {
			return false
		}
		remote := value.(*refCountedRemote)
		removed, err := remote.TryDestroy()
		if err != nil {
			logger.Warn("failed to remove remote fraction",
				zap.String("name", remote.BaseFileName),
				zap.Error(err),
			)
			return true
		}
		if !removed {
			return true
		}
		c.infoCache.Remove(remote.Info().Name())
		c.pending.Delete(name)
		logger.Info("removed remote fraction", zap.String("name", remote.BaseFileName))
		return true
	})
}
