package resources

import (
	"math/bits"
	"sync"
)

type TypedPool[T any] struct {
	pool sync.Pool
}

func (p *TypedPool[T]) Get() (T, bool) {
	item := p.pool.Get()
	var val T
	if item == nil {
		return val, false
	}
	val, ok := item.(T)
	return val, ok
}

func (p *TypedPool[T]) Put(item T) {
	p.pool.Put(item)
}

type SizedPool[T any] struct {
	pools []TypedPool[[]T]
}

func NewSizedPool[T any](buckets int) SizedPool[T] {
	return SizedPool[T]{
		pools: make([]TypedPool[[]T], buckets),
	}
}

func index(size uint) (idx, leftBorder int) {
	idx = bits.Len((size - 1) >> 8)
	return idx, 1 << (idx + 8)
}

func (p SizedPool[T]) Get(size int) []T {
	idx, poolCapacity := index(uint(size))

	if idx < len(p.pools) {
		if data, ok := p.pools[idx].Get(); ok {
			return data[:size]
		}
	}

	idx++
	if idx < len(p.pools) {
		if data, ok := p.pools[idx].Get(); ok {
			return data[:size]
		}
	}

	return make([]T, size, poolCapacity)
}

func (p SizedPool[T]) Put(item []T) {
	capacity := cap(item)
	idx, leftBorder := index(uint(capacity))

	if idx > 0 && capacity < leftBorder {
		idx--
	}

	if idx < len(p.pools) {
		item = item[:0]
		p.pools[idx].Put(item)
	}
}
