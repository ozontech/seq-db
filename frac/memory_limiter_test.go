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
	acquired := make(chan func())
	go func() {
		release, err := l.Acquire(context.Background(), 60)
		require.NoError(t, err)
		acquired <- release
	}()

	select {
	case <-acquired:
		t.Fatal("acquired over the limit")
	case <-time.After(50 * time.Millisecond):
	}

	release()
	select {
	case release = <-acquired:
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
