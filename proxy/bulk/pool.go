package bulk

import (
	"context"
	"errors"
)

var (
	ErrChannelClosed = errors.New("channel closed")
)

type Pool[T any] struct {
	ch chan *T
}

func NewPool[T any](size int, constructor func() *T) *Pool[T] {
	ch := make(chan *T, size)
	for i := 0; i < size; i++ {
		ch <- constructor()
	}
	return &Pool[T]{
		ch: ch,
	}
}

func (p *Pool[T]) Cap() int {
	return cap(p.ch)
}

func (p *Pool[T]) RequestItem(ctx context.Context) (*T, error) {
	select {
	case item, has := <-p.ch:
		if !has {
			return nil, ErrChannelClosed
		}
		return item, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *Pool[T]) BackToPool(item *T) {
	p.ch <- item
}

func (p *Pool[T]) Stop() {
	close(p.ch)
}
