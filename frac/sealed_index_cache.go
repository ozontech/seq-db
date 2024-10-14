package frac

import (
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
)

type SealedIndexCache struct {
	Registry *cache.Cache[[]byte]
	MIDs     *cache.Cache[[]byte]
	RIDs     *cache.Cache[[]byte]
	Params   *cache.Cache[[]uint64]
	Tokens   *cache.Cache[*token.CacheEntry]
	LIDs     *cache.Cache[*lids.Chunks]
}
