package resources

type MapAllocator[K comparable, V any] struct {
	pool     *TypedPool[map[K]V]
	releases *CallStack
}

func NewMapAllocator[K comparable, V any](pool *TypedPool[map[K]V], releases *CallStack) MapAllocator[K, V] {
	return MapAllocator[K, V]{
		pool:     pool,
		releases: releases,
	}
}

func (a MapAllocator[K, V]) Alloc(size int) map[K]V {
	obj, ok := a.pool.Get()
	if ok {
		clear(obj)
	} else {
		obj = make(map[K]V, size)
	}
	a.releases.Defer(func() { a.pool.Put(obj) })
	return obj
}

type ObjectAllocator[T any] struct {
	pool     *TypedPool[*T]
	releases *CallStack
}

func NewObjectAllocator[T any](pool *TypedPool[*T], releases *CallStack) ObjectAllocator[T] {
	return ObjectAllocator[T]{
		pool:     pool,
		releases: releases,
	}
}

func (a ObjectAllocator[T]) Alloc(newFn func() *T, resetFn func(*T)) *T {
	obj, ok := a.pool.Get()
	if ok {
		resetFn(obj)
	} else {
		obj = newFn()
	}
	a.releases.Defer(func() { a.pool.Put(obj) })
	return obj
}
