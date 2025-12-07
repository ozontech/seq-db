package resources

func NewBytes(releases *CallStack) SliceAllocator[byte] {
	return NewSliceAllocator(&BytesPool, releases)
}

func NewStrings(releases *CallStack) SliceAllocator[string] {
	return NewSliceAllocator(&StringsPool, releases)
}

func NewUint32Slices(releases *CallStack) SliceAllocator[[]uint32] {
	return NewSliceAllocator(&Uint32SlicesPool, releases)
}

type SliceAllocator[T any] struct {
	pool     *SizedPool[T]
	releases *CallStack
}

func NewSliceAllocator[T any](pool *SizedPool[T], releases *CallStack) SliceAllocator[T] {
	return SliceAllocator[T]{
		pool:     pool,
		releases: releases,
	}
}

func (a SliceAllocator[T]) AllocSlice(size int) []T {
	data := a.pool.Get(size)
	a.releases.Defer(func() { a.pool.Put(data) })
	return data[:size]
}
