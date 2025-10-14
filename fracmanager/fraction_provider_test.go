package fracmanager

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFractionID(t *testing.T) {
	fp := newFractionProvider(nil, nil, nil, nil, nil)
	ulid1 := fp.nextFractionID()
	ulid2 := fp.nextFractionID()
	assert.NotEqual(t, ulid1, ulid2, "ULIDs should be different")
	assert.Equal(t, 26, len(ulid1), "ULID should have length 26")
	assert.Greater(t, ulid2, ulid1)
}
