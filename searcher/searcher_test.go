package searcher

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

type testFakeFrac struct {
	frac.Fraction
}

func (t *testFakeFrac) IsIntersecting(_, _ seq.MID) bool {
	return true
}

func (t *testFakeFrac) DataProvider(_ context.Context) (frac.DataProvider, func(), bool) {
	return nil, nil, false
}

func (t *testFakeFrac) Info() *frac.Info {
	return frac.NewInfo("test", 0, 0)
}

func TestFracsLimit(t *testing.T) {
	maxFractionHits := 10
	fracsCount := maxFractionHits + 10

	testFracs := make(frac.List, 0, fracsCount)
	for i := 0; i < fracsCount; i++ {
		testFracs = append(testFracs, &testFakeFrac{})
	}

	searcher := New(1, Conf{MaxFractionHits: maxFractionHits})

	_, err := searcher.prepareFracs(testFracs, Params{})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, consts.ErrTooManyFractionsHit))
}

func TestEmptyFracs(t *testing.T) {
	searcher := New(1, Conf{})

	ctx := context.Background()

	seqql, err := parser.ParseSeqQL("level:7", seq.TestMapping)
	assert.NoError(t, err)

	params := Params{
		AST:   seqql.Root,
		From:  seq.MID(0),
		To:    seq.MID(math.MaxUint64),
		Limit: 100,
	}

	qpr, err := searcher.SearchDocs(ctx, frac.List{&testFakeFrac{}}, params)
	assert.NoError(t, err)

	assert.Empty(t, qpr.IDs)
}
