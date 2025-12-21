package resources

type MapsPool[K comparable, V any] struct {
	pool     *TypedPool[map[K]V]
	releases *CallStack
}

func NewMapsPool[K comparable, V any](pool *TypedPool[map[K]V], releases *CallStack) MapsPool[K, V] {
	return MapsPool[K, V]{
		pool:     pool,
		releases: releases,
	}
}

func (a MapsPool[K, V]) Alloc(size int) map[K]V {
	obj, ok := a.pool.Get()
	if ok {
		clear(obj)
	} else {
		obj = make(map[K]V, size)
	}
	a.releases.Defer(func() { a.pool.Put(obj) })
	return obj
}

type ObjectsPool[T any] struct {
	pool     *TypedPool[*T]
	releases *CallStack
}

func NewObjectsPool[T any](pool *TypedPool[*T], releases *CallStack) ObjectsPool[T] {
	return ObjectsPool[T]{
		pool:     pool,
		releases: releases,
	}
}

func (a ObjectsPool[T]) Get(newFn func() *T, resetFn func(*T)) *T {
	obj, ok := a.pool.Get()
	if ok {
		resetFn(obj)
	} else {
		obj = newFn()
	}
	a.releases.Defer(func() { a.pool.Put(obj) })
	return obj
}
