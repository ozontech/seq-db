package fracmanager

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

type testFakeFrac struct {
	frac.Fraction
}

func (t *testFakeFrac) IsIntersecting(_, _ seq.MID) bool {
	return true
}

func (f *testFakeFrac) Fetch(ctx context.Context, ids []seq.ID) ([][]byte, error) {
	return nil, nil
}

func (f *testFakeFrac) Search(ctx context.Context, params processor.SearchParams) (*seq.QPR, error) {
	return &seq.QPR{Aggs: make([]seq.AggregatableSamples, len(params.AggQ))}, nil
}

func (t *testFakeFrac) Info() *common.Info {
	return common.NewInfo("test", 0, 0)
}

func TestFracsLimit(t *testing.T) {
	maxFractionHits := 10
	fracsCount := maxFractionHits + 10

	testFracs := make(List, 0, fracsCount)
	for i := 0; i < fracsCount; i++ {
		testFracs = append(testFracs, &testFakeFrac{})
	}

	s := NewSearcher(1, SearcherCfg{MaxFractionHits: maxFractionHits})
	_, err := s.prepareFracs(testFracs, processor.SearchParams{})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, consts.ErrTooManyFractionsHit))
}

func TestEmptyFracs(t *testing.T) {
	searcher := NewSearcher(1, SearcherCfg{})

	ctx := context.Background()

	seqql, err := parser.ParseSeqQL("level:7", seq.TestMapping)
	assert.NoError(t, err)

	params := processor.SearchParams{
		AST:   seqql.Root,
		From:  seq.MID(0),
		To:    seq.MID(math.MaxUint64),
		Limit: 100,
	}

	qpr, err := searcher.SearchDocs(ctx, List{&testFakeFrac{}}, params)
	assert.NoError(t, err)

	assert.Empty(t, qpr.IDs)
}
