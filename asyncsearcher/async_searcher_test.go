package asyncsearcher

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/mappingprovider"
	"github.com/ozontech/seq-db/seq"
)

type fakeFrac struct {
	frac.Fraction
	info common.Info
	dp   fakeDP
}

func (f *fakeFrac) Info() *common.Info {
	return &f.info
}

func (f *fakeFrac) IsIntersecting(from, to seq.MID) bool {
	return true
}

func (f *fakeFrac) Search(context.Context, processor.SearchParams) (*seq.QPR, error) {
	return &f.dp.qpr, nil
}

type fakeDP struct {
	qpr seq.QPR
}

type fakeFractionProvider fracmanager.List

func (fp fakeFractionProvider) AcquireFraction(name string) (frac.Fraction, func(), bool) {
	for _, f := range fp {
		if f.Info().Name() == name {
			return f, func() {}, true
		}
	}
	return nil, func() {}, false
}

func (fp fakeFractionProvider) AcquireFractions() (fracmanager.List, func()) {
	return fracmanager.List(fp), func() {}
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

	fracs := fakeFractionProvider{
		&fakeFrac{info: common.Info{Path: "1"}},
	}
	r.NoError(as.StartSearch(req, fracs))

	as.processWg.Wait()
}

// partialProvider lists both fractions (so info.Fractions gets two entries),
// but only "present" is acquirable. "missing" simulates a fraction that was
// removed by the time the search reached it: it stays listed in info.Fractions
// yet never produces a .qpr file.
type partialProvider struct {
	list fracmanager.List
}

func (p *partialProvider) AcquireFractions() (fracmanager.List, func()) {
	return p.list, func() {}
}

func (p *partialProvider) AcquireFraction(name string) (frac.Fraction, func(), bool) {
	for _, f := range p.list {
		if f.Info().Name() == name && name == "present" {
			return f, func() {}, true
		}
	}
	return nil, func() {}, false
}

// TestMergeSkipsMissingFrac is a regression test: when a fraction listed in
// info.Fractions was skipped (already removed) and produced no .qpr, merge used
// to build its path from info.Fractions, hit a missing file in loadSearchResult,
// discard the whole accumulated result, write an empty .mqpr and delete the
// real .qpr — losing the only matching document.
func TestMergeSkipsMissingFrac(t *testing.T) {
	r := require.New(t)

	cfg := AsyncSearcherConfig{DataDir: t.TempDir()}
	mp, err := mappingprovider.New("", mappingprovider.WithMapping(seq.Mapping{}))
	r.NoError(err)

	as := MustStartAsync(cfg, mp, nil)
	t.Cleanup(func() { as.readOnly.Store(false) })

	presentFrac := &fakeFrac{
		info: common.Info{Path: "present"},
		dp:   fakeDP{qpr: seq.QPR{IDs: []seq.IDSource{{ID: seq.ID{MID: 42}}}, Total: 1}},
	}
	missingFrac := &fakeFrac{info: common.Info{Path: "missing"}}
	provider := &partialProvider{list: fracmanager.List{presentFrac, missingFrac}}

	req := AsyncSearchRequest{
		ID:        uuid.New().String(),
		Params:    processor.SearchParams{Limit: 1000, Order: seq.DocsOrderDesc},
		Query:     "*",
		Retention: time.Hour,
	}
	r.NoError(as.StartSearch(req, provider))
	as.processWg.Wait()

	// "missing" produced no .qpr; "present" did. Merge must not drop the
	// present result while collapsing the request into a single .mqpr.
	as.merge()

	resp, ok := as.FetchSearchResult(FetchSearchResultRequest{ID: req.ID, Limit: 1000, Order: seq.DocsOrderDesc})
	r.True(ok)
	r.Equal(AsyncSearchStatusDone, resp.Status)
	r.Len(resp.QPR.IDs, 1)
	r.Equal(seq.MID(42), resp.QPR.IDs[0].ID.MID)
}
