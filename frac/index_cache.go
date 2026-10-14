package frac

import (
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
)

// ExprTIDs is a value of the ExprTIDs cache layer: TIDs matched by a
// scan-heavy token expression (wildcard, regex, range) of the immutable
// fraction. Key keeps the original parser.TokenKey string to detect
// uint32 hash collisions. Exact-match literals are resolved by binary
// search alone and never enter this cache.
type ExprTIDs struct {
	Key  string
	TIDs []uint32
}

func NewIndexCache() *IndexCache {
	return &IndexCache{
		LegacyRegistry: cache.NewConcurrentCache[[]byte](nil, nil),

		TokenRegistry:   cache.NewConcurrentCache[[]byte](nil, nil),
		OffsetsRegistry: cache.NewConcurrentCache[[]byte](nil, nil),
		IDRegistry:      cache.NewConcurrentCache[[]byte](nil, nil),
		LIDRegistry:     cache.NewConcurrentCache[[]byte](nil, nil),

		MIDs:   cache.NewConcurrentCache[[]byte](nil, nil),
		RIDs:   cache.NewConcurrentCache[seqids.BlockRIDs](nil, nil),
		Params: cache.NewConcurrentCache[seqids.BlockParams](nil, nil),

		Tokens:     cache.NewConcurrentCache[*token.Block](nil, nil),
		TokenTable: cache.NewConcurrentCache[token.Table](nil, nil),
		LIDs:       cache.NewConcurrentCache[*lids.Block](nil, nil),

		ExprTIDs: cache.NewConcurrentCache[ExprTIDs](nil, nil),
	}
}

type IndexCache struct {
	// Registry cache for legacy sealed fractions.
	LegacyRegistry *cache.ConcurrentCache[[]byte]

	// Per-file registry caches (each IndexReader needs its own).
	TokenRegistry   *cache.ConcurrentCache[[]byte]
	OffsetsRegistry *cache.ConcurrentCache[[]byte]
	IDRegistry      *cache.ConcurrentCache[[]byte]
	LIDRegistry     *cache.ConcurrentCache[[]byte]

	// Block-level data caches shared across all readers.
	MIDs   *cache.ConcurrentCache[[]byte]
	RIDs   *cache.ConcurrentCache[seqids.BlockRIDs]
	Params *cache.ConcurrentCache[seqids.BlockParams]

	Tokens     *cache.ConcurrentCache[*token.Block]
	TokenTable *cache.ConcurrentCache[token.Table]

	LIDs *cache.ConcurrentCache[*lids.Block]

	// ExprTIDs caches GetTIDsByTokenExpr results of the immutable
	// fraction for scan-heavy token expressions (see ExprTIDs);
	// keyed by the hash of parser.TokenKey, evicted by the
	// shared cleaner.
	ExprTIDs *cache.ConcurrentCache[ExprTIDs]
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

	s.ExprTIDs.Release()
}
