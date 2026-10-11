package processor

import (
	"sync"

	"github.com/ozontech/seq-db/parser"
)

// TokenExprCache caches results of GetTIDsByTokenExpr per fraction.
// It exists for async searches, where the same fraction is searched once per
// interval it covers, repeating the same token lookups (e.g. a suffix scan
// over all tokens of a field) that are identical for a sealed fraction.
//
// Tokens are compared by identity of the AST node, so the cache is only
// valid within a single parsed query. The fraction must be immutable
// (sealed), otherwise cached TIDs may miss newly added tokens.
//
// Cached slices are returned by reference and must be treated as read-only.
type TokenExprCache struct {
	mu       sync.Mutex
	entries  map[string]map[parser.Token][]uint32
	maxBytes int
	bytes    int
}

// NewTokenExprCache returns a cache limited by maxBytes of stored TIDs.
// Returns nil when maxBytes is not positive; all methods of a nil cache
// are no-ops, so callers don't need to check whether caching is enabled.
func NewTokenExprCache(maxBytes int) *TokenExprCache {
	if maxBytes <= 0 {
		return nil
	}
	return &TokenExprCache{
		entries:  make(map[string]map[parser.Token][]uint32),
		maxBytes: maxBytes,
	}
}

func (c *TokenExprCache) Get(frac string, token parser.Token) ([]uint32, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	tids, ok := c.entries[frac][token]
	return tids, ok
}

// Put stores tids for the given fraction and token. When the cache is full,
// the entry is dropped: recomputing it is cheaper than unbounded growth.
// Empty results are cached too, since an absent token is exactly the case
// worth remembering — its lookup scans the whole field and finds nothing.
func (c *TokenExprCache) Put(frac string, token parser.Token, tids []uint32) {
	if c == nil {
		return
	}
	size := len(tids) * 4 // each TID is a uint32
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bytes+size > c.maxBytes {
		return
	}
	fracEntries, ok := c.entries[frac]
	if !ok {
		fracEntries = make(map[parser.Token][]uint32)
		c.entries[frac] = fracEntries
	}
	fracEntries[token] = tids
	c.bytes += size
}
