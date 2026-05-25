package compaction

import (
	"context"
	"math"
	"testing"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/seq"
	"github.com/stretchr/testify/require"
)

type mockFraction struct {
	indexOnDisk uint64
}

func (m *mockFraction) Info() *common.Info {
	return &common.Info{IndexOnDisk: m.indexOnDisk}
}

func (m *mockFraction) IsIntersecting(seq.MID, seq.MID) bool {
	return false
}

func (m *mockFraction) Contains(seq.MID) bool {
	return false
}

func (m *mockFraction) Fetch(context.Context, []seq.ID) ([][]byte, error) {
	return nil, nil
}

func (m *mockFraction) Search(context.Context, processor.SearchParams) (*seq.QPR, error) {
	return nil, nil
}

func (m *mockFraction) FindLIDs(context.Context, []seq.ID) ([]seq.LID, error) {
	return nil, nil
}

func makeFracs(sizes ...uint64) []frac.Fraction {
	out := make([]frac.Fraction, len(sizes))
	for i, s := range sizes {
		out[i] = &mockFraction{indexOnDisk: s}
	}
	return out
}

func TestSTCS_Pick(t *testing.T) {
	s := strategySTCS{
		mergeTrigger:     4,
		mergeFanIn:       32,
		mergeFanOutSize:  math.MaxUint64,
		bucketLowerbound: 0.5,
		bucketUpperbound: 1.5,
	}

	t.Run("not-enough-candidates", func(t *testing.T) {
		for n := range s.mergeTrigger {
			require.Nil(t, s.Pick(makeFracs(make([]uint64, n)...)))
		}
	})

	t.Run("requirement-not-met", func(t *testing.T) {
		// Each fraction size is 10x the previous.
		// They land in different buckets and no bucket with [mergeTrigger] fractions exists.
		require.Nil(t, s.Pick(makeFracs(100, 1000, 10000, 100000)))
	})

	t.Run("one-bucket", func(t *testing.T) {
		require.Len(t, s.Pick(makeFracs(1000, 1000, 1000, 1000)), 4)
	})

	t.Run("largest-bucket", func(t *testing.T) {
		fracs := s.Pick(makeFracs(
			1000, 1000,
			100000, 100000, 100000, 100000, 100000, // Will take this bucket.
		))

		require.Len(t, fracs, 5)
		for _, f := range fracs {
			require.Equal(t, uint64(100000), f.Info().IndexOnDisk)
		}
	})

	t.Run("cap-at-fan-in", func(t *testing.T) {
		sizes := make([]uint64, s.mergeFanIn+10)

		for i := range sizes {
			sizes[i] = 5000
		}

		require.Len(t, s.Pick(makeFracs(sizes...)), s.mergeFanIn)
	})
}
