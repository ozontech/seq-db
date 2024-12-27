package frac

import (
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
)

type IndexCache struct {
	Registry   *cache.Cache[[]byte]
	MIDs       *cache.Cache[[]byte]
	RIDs       *cache.Cache[[]byte]
	Params     *cache.Cache[[]uint64]
	Tokens     *cache.Cache[*token.CacheEntry]
	TokenTable *cache.Cache[token.Table]
	LIDs       *cache.Cache[*lids.Chunks]
}

func (s *IndexCache) Release() {
	s.LIDs.Release()
	s.MIDs.Release()
	s.RIDs.Release()
	s.Params.Release()
	s.Registry.Release()
	s.Tokens.Release()
	s.TokenTable.Release()
}
