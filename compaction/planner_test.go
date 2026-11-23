package compaction

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPlanner_Distribute(t *testing.T) {
	start := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) uint64 {
		return uint64(start.Add(d).UnixMilli())
	}

	bins := (&planner{}).distribute(time.Hour, []fraction{
		&mockFraction{epoch: 0, creationTime: at(0)},
		&mockFraction{epoch: 1, creationTime: at(0)},
		&mockFraction{epoch: 1, creationTime: at(30 * time.Minute)},
		&mockFraction{epoch: 2, creationTime: at(45 * time.Minute)},
		&mockFraction{epoch: 2, creationTime: at(time.Hour)},
	})

	sizes := make(map[uint64][]int)
	for b, fracs := range bins {
		for _, f := range fracs {
			require.Equal(t, b.epoch, f.Info().Epoch)
		}
		sizes[b.epoch] = append(sizes[b.epoch], len(fracs))
	}

	require.Equal(t, map[uint64][]int{0: {1}, 1: {2}, 2: {1, 1}}, sizes)
}
