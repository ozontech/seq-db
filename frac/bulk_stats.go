package frac

import (
	"sync"
	"time"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
	"go.uber.org/zap"
)

// BulkStatsCollector collects and periodically logs bulk operation statistics
type BulkStatsCollector struct {
	in chan BulkStats // channel for receiving stats
	wg sync.WaitGroup // wait group for graceful shutdown
	bs BulkStats      // accumulated statistics
	p  time.Duration  // logging interval
}

// NewBulkStatsCollector creates a new stats collector with specified parameters
func NewBulkStatsCollector(period time.Duration, queueSize int) *BulkStatsCollector {
	c := BulkStatsCollector{
		in: make(chan BulkStats, queueSize),
		p:  period,
	}

	c.wg.Add(1)
	go func() {
		t := time.NewTicker(period)
		defer t.Stop()
		defer c.wg.Done()

		for {
			select {
			case s, ok := <-c.in:
				if !ok {
					return
				}
				c.bs.add(s)
			case <-t.C:
				c.log()
			}
		}
	}()

	return &c
}

// BulkStats holds statistics for bulk operations
type BulkStats struct {
	bulks int
	docs  int
	size  int
}

func (s *BulkStats) add(stats BulkStats) {
	s.bulks += stats.bulks
	s.size += stats.size
	s.docs += stats.docs
}

func (c *BulkStatsCollector) Add(bs BulkStats) {
	c.in <- bs
}

func (c *BulkStatsCollector) log() {
	logger.Info("bulk stats",
		zap.String("period", c.p.String()),
		zap.Int("bulks", c.bs.bulks),
		zap.Int("docs", c.bs.docs),
		zap.Float64("size_mb", util.Float64ToPrec(util.SizeToUnit(uint64(c.bs.size), "mb"), 2)),
	)
	c.bs.bulks = 0
	c.bs.docs = 0
	c.bs.size = 0
}

func (c *BulkStatsCollector) Stop() {
	close(c.in)
	c.wg.Wait()
}
