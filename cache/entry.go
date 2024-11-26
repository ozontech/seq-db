package cache

import (
	"sync"

	"github.com/ozontech/seq-db/bytespool"
)

type Releasable[V any] interface {
	Value() V
	Release()
	CopyForRead() Releasable[V]
}

type entry[V any] struct {
	// data is written under Cache.mu lock
	// can be read without lock if wg is nil or waited on
	data bytespool.Releasable[V]
	// wg is written under Cache.mu lock
	// can be read without lock if was waited on
	// (after previous read under lock)
	// wg allows to wait for value to be ready
	// and indicates entry current state
	// if not nil, wait on it
	// if after waiting it's still here,
	// value wasn't initialized and entry is abandoned, retry
	// if wg is nil, everything is good, value can be used
	wg *sync.WaitGroup
	// gen is written and read only under Cache.mu lock
	gen *Generation
	// size is written under Cache.mu lock
	// can be read without lock if wg is nil or waited on
	size uint64
	// function should be called on release cache item
	// release func()
	// value is written under Cache.mu lock
	// can be read without lock if wg is nil or waited on
}

func (e *entry[V]) updateGeneration(ng *Generation) {
	if ng != e.gen {
		e.gen.size.Sub(e.size)
		ng.size.Add(e.size)
		e.gen = ng
	}
}

type fakeReleasable[V any] struct {
	orig V
}

func (f fakeReleasable[V]) Value() V {
	return f.orig
}

func (f fakeReleasable[V]) CopyForRead() bytespool.Releasable[V] {
	return f
}

func (f fakeReleasable[V]) Release() {}

func newFakeReleasable[V any](value V) bytespool.Releasable[V] {
	return fakeReleasable[V]{orig: value}
}
