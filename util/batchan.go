package util

import "sync"

// Batchan works as a batching channel. It allows to Send single items, and Fetch all accumulated so far
// Batchan supports multiple writers and multiple readers at the same time
type Batchan[V any] struct {
	out    []V
	mu     sync.Mutex
	notify chan struct{}
}

func NewBatchan[V any]() *Batchan[V] {
	return &Batchan[V]{
		notify: make(chan struct{}, 1),
	}
}

// Send item into Batchan
func (b *Batchan[V]) Send(v V) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.out = append(b.out, v)
	if len(b.out) == 1 {
		b.notify <- struct{}{}
	}
}

// Close Batchan as to inform fetching end that no more items are coming
func (b *Batchan[V]) Close() {
	close(b.notify)
}

// Fetch all items that were sent to the moment and weren't fetched before
// buf will replace inner buffer and shouldn't be reused once given up to Batchan
// Always returns non-empty slice unless closed
// Blocks if no item is available, but Batchan wasn't closed
// Empty returned slice indicates closed Batchan
func (b *Batchan[V]) Fetch(buf []V) []V {
	_, _ = <-b.notify
	b.mu.Lock()
	defer b.mu.Unlock()
	res := b.out
	b.out = buf[:0]
	return res
}
