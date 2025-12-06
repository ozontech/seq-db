package active2

import (
	"slices"
	"sync"
	"unsafe"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
)

type indexerResources struct {
	releasers []func()
}

var (
	poolAllocator          = sync.Pool{}
	poolMetaData           = sync.Pool{}
	poolMetaToken          = sync.Pool{}
	poolUint32Slices       = sync.Pool{}
	poolTokenizerMetaToken = sync.Pool{}
	poolMetaTokenMap       = sync.Pool{}
	poolStrings            = sync.Pool{}
)

func newIndexerResources() *indexerResources {
	ai, ok := poolAllocator.Get().(*indexerResources)
	if ok {
		ai.releasers = ai.releasers[:0]
	} else {
		ai = &indexerResources{releasers: make([]func(), 0, 64)}
	}
	return ai
}

func (r *indexerResources) newUint32s(size int) []uint32 {
	buf, free := acquireSlice[uint32](size)
	r.releasers = append(r.releasers, free)
	return buf
}

func (r *indexerResources) newInts(size int) []int {
	buf, free := acquireSlice[int](size)
	r.releasers = append(r.releasers, free)
	return buf
}

func (r *indexerResources) newBytes(size int) []byte {
	buf := bytespool.AcquireLen(size)
	r.releasers = append(r.releasers, func() { bytespool.Release(buf) })
	return buf.B
}

func (r *indexerResources) newDocPos(size int) []seq.DocPos {
	buf, free := acquireSlice[seq.DocPos](size)
	r.releasers = append(r.releasers, free)
	return buf
}

func (r *indexerResources) newMetaTokenMap(size int) map[tokenKey]uint32 {
	buf, ok := poolMetaTokenMap.Get().(map[tokenKey]uint32)
	if !ok {
		buf = make(map[tokenKey]uint32, size)
	} else {
		clear(buf)
	}
	r.releasers = append(r.releasers, func() { poolMetaTokenMap.Put(buf) })
	return buf
}

func (r *indexerResources) newUint32Slices(size int) [][]uint32 {
	bufPtr, free := acquireFromPoolPtr[[]uint32](&poolUint32Slices, size)
	r.releasers = append(r.releasers, free)
	return *bufPtr
}

func (r *indexerResources) newMetaTokens(size int) []tokenKey {
	bufPtr, free := acquireFromPoolPtr[tokenKey](&poolMetaToken, size)
	r.releasers = append(r.releasers, free)
	return *bufPtr
}

func (r *indexerResources) newTokenizerMetaTokensPtr(size int) *[]tokenizer.MetaToken {
	bufPtr, free := acquireFromPoolPtr[tokenizer.MetaToken](&poolTokenizerMetaToken, size)
	r.releasers = append(r.releasers, free)
	return bufPtr
}

func (r *indexerResources) newStrings(size int) []string {
	bufPtr, free := acquireFromPoolPtr[string](&poolStrings, size)
	r.releasers = append(r.releasers, free)
	return *bufPtr
}

func (a *indexerResources) newTokenizerMetaTokens(size int) ([]tokenizer.MetaToken, func([]tokenizer.MetaToken)) {
	bufPtr := a.newTokenizerMetaTokensPtr(size)
	return *bufPtr, func(mt []tokenizer.MetaToken) { *bufPtr = mt }
}

func (r *indexerResources) newMetaData(size int) []indexer.MetaData {
	bufPtr, free := acquireFromPoolPtr[indexer.MetaData](&poolMetaData, size)
	r.releasers = append(r.releasers, free)
	return *bufPtr
}

func (r *indexerResources) releaseAll() {
	for _, r := range r.releasers {
		r()
	}
	poolAllocator.Put(r)
}

func acquireSlice[T any](size int) ([]T, func()) {
	var tmp T
	itemSize := int(unsafe.Sizeof(tmp))
	buf := bytespool.AcquireLen(int(size) * itemSize)
	res := unsafe.Slice((*T)(unsafe.Pointer(unsafe.SliceData(buf.B))), size)
	return res, func() { bytespool.Release(buf) }
}

func acquireFromPoolPtr[T any](pool *sync.Pool, size int) (*[]T, func()) {
	buf, ok := pool.Get().([]T)
	if !ok {
		buf = make([]T, size)
	} else {
		buf = slices.Grow(buf[:0], size)[:size]
	}
	return &buf, func() { pool.Put(buf) }
}
