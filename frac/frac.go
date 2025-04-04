package frac

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/seq"
)

type Config struct {
	Search SearchConfig
}

type SearchConfig struct {
	AggLimits AggLimits
}

type AggLimits struct {
	MaxFieldTokens     int // MaxFieldTokens max AggQuery.Field uniq values to parse.
	MaxGroupTokens     int // MaxGroupTokens max AggQuery.GroupBy unique values.
	MaxTIDsPerFraction int // MaxTIDsPerFraction max number of tokens per fraction.
}

type frac struct {
	Config Config

	statsMu sync.Mutex

	info *Info

	BaseFileName string

	useLock  sync.RWMutex
	suicided bool
}

func (f *frac) Contains(id seq.MID) bool {
	return f.IsIntersecting(id, id)
}

func (f *frac) IsIntersecting(from, to seq.MID) bool {
	info := f.Info()
	if info.DocsTotal == 0 { // don't include fresh active fraction
		return false
	}

	if to < info.From || info.To < from {
		return false
	}

	if info.Distribution == nil { // can't check distribution
		return true
	}

	// check with distribution
	return info.Distribution.IsIntersecting(from, to)
}

func (f *frac) Info() *Info {
	f.statsMu.Lock()
	defer f.statsMu.Unlock()
	info := *f.info

	return &info
}

func (f *frac) setInfoSealingTime(newTime uint64) {
	f.statsMu.Lock()
	defer f.statsMu.Unlock()

	f.info.SealingTime = newTime
}

func (f *frac) setInfoIndexOnDisk(newSize uint64) {
	f.statsMu.Lock()
	defer f.statsMu.Unlock()

	f.info.IndexOnDisk = newSize
}

func (f *frac) toString(fracType string) string {
	stats := f.Info()
	s := fmt.Sprintf(
		"%s fraction name=%s, creation time=%s, from=%s, to=%s, %s",
		fracType,
		stats.Name(),
		time.UnixMilli(int64(stats.CreationTime)).Format(consts.ESTimeFormat),
		stats.From,
		stats.To,
		stats.String(),
	)
	if fracType == "" {
		return s[1:]
	}
	return s
}

// logArgs returns slice of zap.Field for frac close log.
func (f *frac) closeLogArgs(fracType, hint string, err error) []zap.Field {
	return []zap.Field{
		zap.String("frac", f.BaseFileName),
		zap.String("type", fracType),
		zap.String("hint", hint),
		zap.Error(err),
	}
}
