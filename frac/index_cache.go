package frac

import (
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
)

type IndexCache struct {
	// Per-file registry caches (each IndexReader needs its own).
	InfoRegistry    *cache.Cache[[]byte]
	TokenRegistry   *cache.Cache[[]byte]
	OffsetsRegistry *cache.Cache[[]byte]
	IDRegistry      *cache.Cache[[]byte]
	LIDRegistry     *cache.Cache[[]byte]

	// Block-level data caches shared across all readers.
	MIDs       *cache.Cache[[]byte]
	RIDs       *cache.Cache[seqids.BlockRIDs]
	Params     *cache.Cache[seqids.BlockParams]
	Tokens     *cache.Cache[*token.Block]
	TokenTable *cache.Cache[token.Table]
	LIDs       *cache.Cache[*lids.Block]
}

func (s *IndexCache) Release() {
	s.InfoRegistry.Release()
	s.TokenRegistry.Release()
	s.OffsetsRegistry.Release()
	s.IDRegistry.Release()
	s.LIDRegistry.Release()
	s.LIDs.Release()
	s.MIDs.Release()
	s.RIDs.Release()
	s.Params.Release()
	s.Tokens.Release()
	s.TokenTable.Release()
}
