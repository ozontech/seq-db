package fracmanager

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
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
		&fakeFrac{info: common.Info{Path: "1"}},
	}
	r.NoError(as.StartSearch(req, fracs))

	as.processWg.Wait()
}
