package searcher

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/seq"
)

type testFakeFrac struct {
	frac.Fraction
}

func (t *testFakeFrac) IsIntersecting(_, _ seq.MID) bool {
	return true
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
