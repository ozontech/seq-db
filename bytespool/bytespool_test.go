//go:build !race

package bytespool

import (
	"runtime/debug"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufferPoolSimpleAcquire(t *testing.T) {
	buf := Acquire(235)
	defer Release(buf)
	require.Equal(t, 256, cap(buf.B))
	require.Equal(t, 235, len(buf.B))
}

func TestBufferPoolReleaseByIndex(t *testing.T) {
	type TestCase struct {
		RequestedLength      int
		ExpectedLength       int
		ExpectedCapacity     int
		ExpectedReleaseIndex int
	}

	tcs := []TestCase{
		{
			RequestedLength:      0,
			ExpectedLength:       0,
			ExpectedCapacity:     0,
			ExpectedReleaseIndex: -1,
		},
		{
			RequestedLength:      1,
			ExpectedLength:       1,
			ExpectedCapacity:     1,
			ExpectedReleaseIndex: 0,
		},
		{
			RequestedLength:      16,
			ExpectedLength:       16,
			ExpectedCapacity:     16,
			ExpectedReleaseIndex: 4,
		},
		{
			RequestedLength:      17,
			ExpectedLength:       17,
			ExpectedCapacity:     32,
			ExpectedReleaseIndex: 5,
		},
		{
			RequestedLength:      31,
			ExpectedLength:       31,
			ExpectedCapacity:     32,
			ExpectedReleaseIndex: 5,
		},
		{
			RequestedLength:      32,
			ExpectedLength:       32,
			ExpectedCapacity:     32,
			ExpectedReleaseIndex: 5,
		},
		{
			RequestedLength:      1 << 31,
			ExpectedLength:       1 << 31,
			ExpectedCapacity:     1 << 31,
			ExpectedReleaseIndex: 31,
		},
		{
			RequestedLength:      1<<32 - 1,
			ExpectedLength:       1<<32 - 1,
			ExpectedCapacity:     1<<32 - 1,
			ExpectedReleaseIndex: -1,
		},
		{
			RequestedLength:      1 << 32,
			ExpectedLength:       1 << 32,
			ExpectedCapacity:     1 << 32,
			ExpectedReleaseIndex: -1,
		},
		{
			RequestedLength:      1<<30 + 42,
			ExpectedLength:       1<<30 + 42,
			ExpectedCapacity:     1 << 31,
			ExpectedReleaseIndex: 31,
		},
		{
			RequestedLength:      1<<30 - 42,
			ExpectedLength:       1<<30 - 42,
			ExpectedCapacity:     1 << 30,
			ExpectedReleaseIndex: 30,
		},
	}

	for i, tc := range tcs {
		i, tc := i, tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()

			pool := New()

			// Disable GC to avoid sync.Pool cleanup
			debug.SetGCPercent(-1)
			defer debug.SetGCPercent(100)

			buf := pool.Acquire(tc.RequestedLength)
			require.NotNil(t, buf)
			require.NotNil(t, buf.B)
			require.Equal(t, tc.ExpectedLength, len(buf.B))
			require.Equal(t, tc.ExpectedCapacity, cap(buf.B))

			if tc.ExpectedReleaseIndex == -1 {
				pool.Release(buf)
				for i := range pool.pools {
					require.Nil(t, pool.pools[i].Get())
				}
			} else {
				bufferMark := byte(i) + 42

				buf.B[0] = bufferMark // To identify buffer later
				pool.Release(buf)

				buf := pool.pools[tc.ExpectedReleaseIndex].Get()
				require.NotNil(t, buf)
				require.Equal(t, bufferMark, buf.(*Buffer).B[0])
			}
		})
	}
}
