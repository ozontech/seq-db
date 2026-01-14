package resources

import (
	"unsafe"
)

func NewUint32s(releases *CallStack) SliceOnBytes[uint32] {
	return NewSliceOnBytes[uint32](releases)
}

func NewUint64s(releases *CallStack) SliceOnBytes[uint64] {
	return NewSliceOnBytes[uint64](releases)
}

type SliceOnBytes[T any] struct {
	pool     *SizedPool[byte]
	releases *CallStack
}

func NewSliceOnBytes[T any](releases *CallStack) SliceOnBytes[T] {
	return SliceOnBytes[T]{
		pool:     &BytesPool,
		releases: releases,
	}
}

func (a SliceOnBytes[T]) GetSlice(size int) []T {
	var empty T
	itemSize := int(unsafe.Sizeof(empty))

	buf := a.pool.Get(size * itemSize)
	capacity := cap(buf) / itemSize
	data := unsafe.Slice((*T)(unsafe.Pointer(unsafe.SliceData(buf))), capacity)[:size]
	a.releases.Defer(func() { a.pool.Put(buf) })
	return data
}
