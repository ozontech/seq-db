package frac

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemoryLimiter(t *testing.T) {
	l := NewMemoryLimiter(100)

	release, err := l.Acquire(context.Background(), 60)
	require.NoError(t, err)
	type acquireResult struct {
		release func()
		err     error
	}
	acquired := make(chan acquireResult)
	go func() {
		release, err := l.Acquire(context.Background(), 60)
		acquired <- acquireResult{release, err}
	}()

	select {
	case <-acquired:
		t.Fatal("acquired over the limit")
	case <-time.After(50 * time.Millisecond):
	}

	release()
	select {
	case r := <-acquired:
		require.NoError(t, r.err)
		release = r.release
	case <-time.After(time.Second):
		t.Fatal("not acquired after release")
	}
	release()

	release, err = l.Acquire(context.Background(), 1000)
	require.NoError(t, err)
	release()

	ctx, cancel := context.WithCancel(context.Background())
	release, err = l.Acquire(ctx, 100)
	require.NoError(t, err)
	cancel()
	_, err = l.Acquire(ctx, 1)
	require.ErrorIs(t, err, context.Canceled)
	release()
	require.True(t, l.sem.TryAcquire(100))

	var unlimited *MemoryLimiter
	release, err = unlimited.Acquire(context.Background(), 1<<40)
	require.NoError(t, err)
	release()
}
