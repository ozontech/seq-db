package util

import (
	"sync"

	"github.com/ozontech/seq-db/bytespool"
)

type BufferPool struct {
	pool sync.Pool
}

func (q *BufferPool) Get() *bytespool.Buffer {
	v := q.pool.Get()
	if v != nil {
		return v.(*bytespool.Buffer)
	}
	return new(bytespool.Buffer)
}

func (q *BufferPool) Put(b *bytespool.Buffer) {
	b.Reset()
	q.pool.Put(b)
}
