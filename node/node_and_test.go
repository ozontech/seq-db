package node

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeAnd_NextGeqAscending(t *testing.T) {
	left := NewStatic([]uint32{1, 2, 7, 10, 20, 25, 26, 30, 50, 80, 90, 100}, false)
	right := NewStatic([]uint32{1, 3, 4, 7, 9, 30, 40, 45, 60, 80, 110}, false)

	node := NewAnd(left, right)

	// Currently, nodes instantiate their state on creation, which will be fixed later.
	// Thus, the first LID returned is the first from left and right
	id := node.NextGeq(NewCmpLIDOrderDesc(7))
	assert.Equal(t, uint32(1), id.Unpack())

	id = node.NextGeq(NewCmpLIDOrderDesc(7))
	assert.Equal(t, uint32(7), id.Unpack())

	id = node.NextGeq(NewCmpLIDOrderDesc(50))
	assert.Equal(t, uint32(80), id.Unpack())

	id = node.NextGeq(NewCmpLIDOrderDesc(50))
	assert.True(t, id.IsNull())
}

// TestNodeAnd_NextGeqCompatibility tests that just calling NextGeq with 0 passed as argument is equivalent to
// calling Next
func TestNodeAnd_NextGeqCompatibility(t *testing.T) {
	for _, rev := range []bool{true, false} {
		left := []uint32{rand.Uint32N(10)}
		right := []uint32{rand.Uint32N(10)}

		for i := 1; i < 1000; i++ {
			left = append(left, left[i-1]+rand.Uint32N(10))
			right = append(right, right[i-1]+rand.Uint32N(10))
		}

		node := NewAnd(NewStatic(left, rev), NewStatic(right, rev))
		nodeGeq := NewAnd(NewStatic(left, rev), NewStatic(right, rev))

		var zero uint32
		if rev {
			zero = math.MaxUint32
		} else {
			zero = 0
		}

		for {
			lid := node.Next()
			lidGeq := nodeGeq.NextGeq(NewCmpLID(zero, rev))

			assert.Equal(t, lid, lidGeq)

			if lid.IsNull() {
				break
			}
		}
	}
}
