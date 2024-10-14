package bulk

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestStruct struct{}

func TestNewPool(t *testing.T) {
	pool := NewPool[TestStruct](3, func() *TestStruct {
		return &TestStruct{}
	})
	assert.Equal(t, 3, cap(pool.ch))
	assert.Equal(t, 3, len(pool.ch))
}

func TestRequestItem(t *testing.T) {
	pool := NewPool[TestStruct](1, func() *TestStruct {
		return &TestStruct{}
	})

	_, err := pool.RequestItem(context.Background())
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

	_, err = pool.RequestItem(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRequestFromClosed(t *testing.T) {
	pool := NewPool[TestStruct](1, func() *TestStruct {
		return &TestStruct{}
	})

	_, err := pool.RequestItem(context.Background())
	assert.NoError(t, err)

	pool.Stop()

	_, err = pool.RequestItem(context.Background())
	assert.ErrorIs(t, err, ErrChannelClosed)
}

func TestBackToPool(t *testing.T) {
	pool := NewPool[TestStruct](1, func() *TestStruct {
		return &TestStruct{}
	})

	item, _ := pool.RequestItem(context.Background())
	pool.BackToPool(item)
	assert.Equal(t, 1, cap(pool.ch))
}
