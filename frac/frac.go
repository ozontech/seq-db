package frac

import (
	"fmt"
	"sync"
	"time"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/searcher"
)

type SearchCfg struct {
	AggLimits searcher.AggLimits
}

type frac struct {
	searchCfg SearchCfg

	statsMu sync.Mutex

	info *Info

	BaseFileName string

	useLock  sync.RWMutex
	suicided bool
}

func toString(f Fraction, fracType string) string {
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
