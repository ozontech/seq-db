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

	fracs := fracmanager.List{
		&fakeFrac{info: common.Info{Path: "1"}},
	}
	r.NoError(as.StartSearch(req, fracs.Names(), fakeFractionProvider(fracs)))

	as.processWg.Wait()
}
