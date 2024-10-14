package frac

import (
	"sync"
)

type UInt64s struct {
	mu   sync.RWMutex
	vals []uint64
}

func NewIDs() *UInt64s {
	r := &UInt64s{
		vals: make([]uint64, 0, 4),
		mu:   sync.RWMutex{},
	}

	return r
}

func (l *UInt64s) Len() uint32 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return uint32(len(l.vals))
}

func (l *UInt64s) GetVals() []uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// we only append, so it's safe to return slice here

	return l.vals
}

func (l *UInt64s) append(val uint64) uint32 {
	l.vals = append(l.vals, val)
	pos := len(l.vals) - 1

	return uint32(pos)
}

func (l *UInt64s) Append(val uint64) uint32 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.append(val)
}
