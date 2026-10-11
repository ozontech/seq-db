package processor

import "context"

// SearchContext carries request-scoped state of a search execution.
// Unlike SearchParams, which describes what to search (and is persisted),
// SearchContext describes how the search is executed and lives only for
// the duration of a request — including async searches, where one request
// spawns many searches over the same fractions.
//
// It embeds context.Context so cancellation and deadlines keep working
// through it; an embedded zero Context must not be created directly,
// use NewSearchContext.
type SearchContext struct {
	context.Context

	// TokenCache caches token TID lookups across searches of the same
	// sealed fraction within one request; nil means caching is disabled.
	TokenCache *TokenExprCache
}

// NewSearchContext creates a search context without any request-scoped state.
func NewSearchContext(ctx context.Context) SearchContext {
	return SearchContext{Context: ctx}
}
