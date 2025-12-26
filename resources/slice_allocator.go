package resources

func NewBytes(releases *CallStack) SlicesPool[byte] {
	return NewSlicesPool(&BytesPool, releases)
}

func NewStrings(releases *CallStack) SlicesPool[string] {
	return NewSlicesPool(&StringsPool, releases)
}

func NewUint32Slices(releases *CallStack) SlicesPool[[]uint32] {
	return NewSlicesPool(&Uint32SlicesPool, releases)
}

func NewBytesSlices(releases *CallStack) SlicesPool[[]byte] {
	return NewSlicesPool(&BytesSlicesPool, releases)
}

type SlicesPool[T any] struct {
	pool     *SizedPool[T]
	releases *CallStack
}

func NewSlicesPool[T any](pool *SizedPool[T], releases *CallStack) SlicesPool[T] {
	return SlicesPool[T]{
		pool:     pool,
		releases: releases,
	}
}

func (a SlicesPool[T]) GetSlice(size int) []T {
	data := a.pool.Get(size)
	a.releases.Defer(func() { a.pool.Put(data) })
	return data[:size]
}
