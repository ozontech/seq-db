package fracmanager

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/mappingprovider"
	"github.com/ozontech/seq-db/seq"
)

type fakeFrac struct {
	frac.Fraction
	info frac.Info
	dp   fakeDP
}

func (f *fakeFrac) Info() *frac.Info {
	return &f.info
}

func (f *fakeFrac) DataProvider(_ context.Context) (frac.DataProvider, func()) {
	return &f.dp, func() {}
}

type fakeDP struct {
	frac.DataProvider
	qpr seq.QPR
}

func (f *fakeDP) Search(processor.SearchParams) (*seq.QPR, error) {
	return &f.qpr, nil
}

func TestAsyncSearcherMaintain(t *testing.T) {
	r := require.New(t)

	cfg := AsyncSearcherConfig{
		DataDir: t.TempDir(),
	}
	mp, err := mappingprovider.New("", mappingprovider.WithMapping(seq.Mapping{}))
	r.NoError(err)

	as := MustStartAsync(cfg, mp, nil)

	req := AsyncSearchRequest{
		ID:        uuid.New().String(),
		Params:    processor.SearchParams{},
		Query:     "*",
		Retention: time.Hour,
	}
	fracs := []frac.Fraction{
		&fakeFrac{info: frac.Info{Path: "1"}},
	}
	r.NoError(as.StartSearch(req, fracs))

	as.processWg.Wait()
}
