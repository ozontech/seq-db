package cache

import (
	"weak"
)

type Loader[V any] interface {
	Load(key uint32) (value V, size int, err error)
}

type LoaderFunc[V any] func(key uint32) (V, int, error)

func (f LoaderFunc[V]) Load(key uint32) (V, int, error) {
	return f(key)
}

type Cache[V any] interface {
	Get(key uint32, l Loader[V]) (V, error)
}

var (
	_ Cache[any] = (*ConcurrentCache[any])(nil)
	_ Cache[any] = (*Session[any])(nil)
	_ Cache[any] = (*Scan[any])(nil)
)

// Session is a wrapper for [ConcurrentCache] which stores local
// copies of [entry] via weak pointers.
type Session[V any] struct {
	cache *ConcurrentCache[V]
	// The reason why [local] hashmap contains weak-pointers
	// is to prevent memory over-use. Whenever cache will be released
	// entries will be collected by GC and we will have to go to the cache again.
	local map[uint32]weak.Pointer[entry[V]]
}

func NewSession[V any](c *ConcurrentCache[V]) *Session[V] {
	return &Session[V]{
		cache: c,
		local: make(map[uint32]weak.Pointer[entry[V]]),
	}
}

func (s *Session[V]) Get(key uint32, l Loader[V]) (V, error) {
	if e := s.resolve(key); e != nil {
		return e.value, nil
	}

	value, e, err := s.cache.get(key, l)
	if err != nil {
		return value, err
	}

	s.local[key] = weak.Make(e)
	return value, nil
}

func (s *Session[V]) resolve(key uint32) *entry[V] {
	wp, ok := s.local[key]
	if !ok {
		return nil
	}

	e := wp.Value()
	if e == nil {
		delete(s.local, key)
		return nil
	}

	return e
}

// [Scan] wrapper is used exclusively for compaction
// to prevent unnecessary evictions from cache.
type Scan[V any] struct {
	key   uint32
	value V
	ok    bool
}

func NewScan[V any]() *Scan[V] {
	return &Scan[V]{}
}

func (s *Scan[V]) Get(key uint32, l Loader[V]) (V, error) {
	if s.ok && s.key == key {
		return s.value, nil
	}

	value, _, err := l.Load(key)
	if err != nil {
		return value, err
	}

	s.key, s.value, s.ok = key, value, true
	return value, nil
}
