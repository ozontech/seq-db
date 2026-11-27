package frac

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

func TestMemoryLimiterWaitsForRelease(t *testing.T) {
	for _, tc := range []struct {
		name          string
		first, second uint64
	}{
		{name: "within limit", first: 60, second: 60},
		{name: "clamped to limit", first: 1000, second: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := NewMemoryLimiter(100)
				release, err := l.Acquire(t.Context(), tc.first)
				require.NoError(t, err)

				acquired := make(chan error, 1)
				go func() {
					release, err := l.Acquire(t.Context(), tc.second)
					if err == nil {
						release()
					}
					acquired <- err
				}()
				synctest.Wait()
				require.Empty(t, acquired)

				release()
				synctest.Wait()
				require.NoError(t, <-acquired)
			})
		})
	}
}

func TestMemoryLimiterCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := NewMemoryLimiter(100)
		release, err := l.Acquire(t.Context(), 100)
		require.NoError(t, err)
		defer release()

		ctx, cancel := context.WithCancel(t.Context())
		acquired := make(chan error, 1)
		go func() {
			_, err := l.Acquire(ctx, 1)
			acquired <- err
		}()
		synctest.Wait()
		require.Empty(t, acquired)

		cancel()
		synctest.Wait()
		require.ErrorIs(t, <-acquired, context.Canceled)
	})
}

func TestMemoryLimiterNil(t *testing.T) {
	var l *MemoryLimiter
	release, err := l.Acquire(t.Context(), 1<<40)
	require.NoError(t, err)
	release()
}
