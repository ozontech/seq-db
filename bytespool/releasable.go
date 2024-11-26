package bytespool

import (
	"sync/atomic"
)

type Releasable[V any] interface {
	Value() V
	Release()
	CopyForRead() Releasable[V]
}

type ReleasableBytes struct {
	Buf *Buffer
	cnt *atomic.Int64
}

func (b ReleasableBytes) Value() []byte {
	return b.Buf.B
}

func (b ReleasableBytes) Copy() []byte {
	return append(make([]byte, 0, len(b.Buf.B)), b.Buf.B...)
}

func (b ReleasableBytes) CopyForRead() Releasable[[]byte] {
	b.cnt.Add(1)
	return b
}

func (b ReleasableBytes) Release() {
	if b.Buf != nil {
		if b.cnt.Add(-1) == 0 {
			Release(b.Buf)
			b.Buf = nil
		}
	}
}

func NewReleasableBytes(l uint64) ReleasableBytes {
	c := atomic.Int64{}
	c.Add(1)
	return ReleasableBytes{
		Buf: Acquire(int(l)),
		cnt: &c,
	}
}
