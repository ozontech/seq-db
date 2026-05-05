package frac

import (
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
)

func newIndexCache() *IndexCache {
	return &IndexCache{
		LegacyRegistry: cache.NewCache[[]byte](nil, nil),

		TokenRegistry:   cache.NewCache[[]byte](nil, nil),
		OffsetsRegistry: cache.NewCache[[]byte](nil, nil),
		IDRegistry:      cache.NewCache[[]byte](nil, nil),
		LIDRegistry:     cache.NewCache[[]byte](nil, nil),

		MIDs:   cache.NewCache[[]byte](nil, nil),
		RIDs:   cache.NewCache[seqids.BlockRIDs](nil, nil),
		Params: cache.NewCache[seqids.BlockParams](nil, nil),

		Tokens:     cache.NewCache[*token.Block](nil, nil),
		TokenTable: cache.NewCache[token.Table](nil, nil),
		LIDs:       cache.NewCache[*lids.Block](nil, nil),
	}
}

type IndexCache struct {
	// Registry cache for legacy sealed fractions.
	LegacyRegistry *cache.Cache[[]byte]

	// Per-file registry caches (each IndexReader needs its own).
	TokenRegistry   *cache.Cache[[]byte]
	OffsetsRegistry *cache.Cache[[]byte]
	IDRegistry      *cache.Cache[[]byte]
	LIDRegistry     *cache.Cache[[]byte]

	// Block-level data caches shared across all readers.
	MIDs   *cache.Cache[[]byte]
	RIDs   *cache.Cache[seqids.BlockRIDs]
	Params *cache.Cache[seqids.BlockParams]

	Tokens     *cache.Cache[*token.Block]
	TokenTable *cache.Cache[token.Table]

	LIDs *cache.Cache[*lids.Block]
}

func (s *IndexCache) Release() {
	s.LegacyRegistry.Release()

	s.TokenRegistry.Release()
	s.OffsetsRegistry.Release()
	s.IDRegistry.Release()
	s.LIDRegistry.Release()

	s.MIDs.Release()
	s.RIDs.Release()
	s.Params.Release()

	s.Tokens.Release()
	s.TokenTable.Release()

	s.LIDs.Release()
}
