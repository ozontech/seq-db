package frac

import (
	"context"

	"golang.org/x/sync/semaphore"
)

type MemoryLimiter struct {
	sem   *semaphore.Weighted
	limit uint64
}

func NewMemoryLimiter(limit uint64) *MemoryLimiter {
	return &MemoryLimiter{sem: semaphore.NewWeighted(int64(limit)), limit: limit}
}

func (l *MemoryLimiter) Acquire(ctx context.Context, bytes uint64) (release func(), err error) {
	if l == nil {
		return func() {}, nil
	}

	bytes = min(bytes, l.limit)
	if err := l.sem.Acquire(ctx, int64(bytes)); err != nil {
		return nil, err
	}

	return func() { l.sem.Release(int64(bytes)) }, nil
}
