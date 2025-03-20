package fracmanager

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ozontech/seq-db/frac/searcher"
)

func TestFracsLimit(t *testing.T) {
	maxFractionHits := 10
	fracsCount := maxFractionHits + 10

	testFracs := make(List, 0, fracsCount)
	for i := 0; i < fracsCount; i++ {
		testFracs = append(testFracs, &testFakeFrac{})
	}

	s := NewSearcher(1, SearcherCfg{MaxFractionHits: maxFractionHits})
	_, err := s.prepareFracs(testFracs, searcher.Params{})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrTooManyFractionsHit))
}
