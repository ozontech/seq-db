package fetch

import (
	"context"

	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/seq"
)

type Fetcher interface {
	FetchDocs(ctx context.Context, fracs fracmanager.FracsList, ids []seq.IDSource) (map[string][]byte, error)
}
