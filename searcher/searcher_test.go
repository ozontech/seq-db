package searcher

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/seq"
)

type testFakeFrac struct {
	frac.Fraction
	counter atomic.Int64
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

	searcher := New(1, Config{MaxFractionHits: maxFractionHits})

	_, err := searcher.prepareFracs(Params{}, testFracs)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, consts.ErrTooManyFracHit))
}
