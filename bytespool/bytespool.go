package bytespool

import (
	"fmt"
	"math/bits"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var bytesPool = New()

// Acquire gets a byte buffer with given length from the global pool.
func Acquire(length int) *Buffer {
	return bytesPool.Acquire(length)
}

// AcquireReset gets a byte buffer with zero length from the global pool.
func AcquireReset(length int) *Buffer {
	s := bytesPool.Acquire(length)
	s.Reset()
	return s
}

// Release puts the byte buffer to the global pool.
func Release(buf *Buffer) {
	bytesPool.Release(buf)
}

var (
	// get metrics
	hitCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db",
		Subsystem: "common",
		Name:      "bytes_pool_get_hits_total",
		Help:      "",
	}, []string{"capacity"})
	missCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db",
		Subsystem: "common",
		Name:      "bytes_pool_get_misses_total",
		Help:      "",
	}, []string{"capacity"})

	// put metrics
	putCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db",
		Subsystem: "common",
		Name:      "bytes_pool_puts_total",
		Help:      "",
	}, []string{"capacity"})
	putOversizeCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db",
		Subsystem: "common",
		Name:      "bytes_pool_put_oversizes_total",
		Help:      "",
	})
)

type Buffer struct {
	B []byte
}

func (b *Buffer) Reset() {
	b.B = b.B[:0]
}

func (b *Buffer) Write(buf []byte) (int, error) {
	oldSlice := b.B
	b.B = append(b.B, buf...)
	if cap(oldSlice) != cap(b.B) {
		// The slice was reallocated, so return it to pool
		Release(&Buffer{B: oldSlice})
	}
	return len(buf), nil
}

const (
	pools       = 32
	maxCapacity = 1 << (pools - 1)
)

// Pool consists of N sync.Pool's,
// each pool is a set of temporary Buffer[T] with capacity from [2^n, 2^n+1),
// where the n is the pool index.
type Pool struct {
	// pools contains pools for byte slices of various capacities.
	//
	//	pools[0] [1,  2)
	//	pools[1] [2,  4)
	//	pools[2] [4,  8)
	//	pools[3] [8,  16)
	//	pools[4] [16, 32)
	//	pools[5] [32, 64)
	//	pools[6] [64, 128)
	//	...
	//	pools[30] [1 073 741 824, 2 147 483 648)
	//	pools[31] [2 147 483 648, 4 294 967 296)
	pools [pools]sync.Pool

	metrics [pools]poolMetrics
}

type poolMetrics struct {
	hitCounter  prometheus.Counter
	missCounter prometheus.Counter
	putCounter  prometheus.Counter
}

func New() *Pool {
	metrics := [pools]poolMetrics{}
	for idx := range metrics {
		capacity := byteCountIEC(1 << idx)
		metrics[idx] = poolMetrics{
			hitCounter:  hitCounter.WithLabelValues(capacity),
			missCounter: missCounter.WithLabelValues(capacity),
			putCounter:  putCounter.WithLabelValues(capacity),
		}
	}

	return &Pool{
		pools:   [pools]sync.Pool{},
		metrics: metrics,
	}
}

// Acquire retrieves a Buffer with given length and rounded up capacity of the power of two.
// Panics when length < 0.
func (p *Pool) Acquire(length int) *Buffer {
	if length <= 0 || length > maxCapacity {
		return &Buffer{B: make([]byte, length)}
	}

	idx, poolCapacity := index(length)
	buf := p.getByIndex(idx)
	if buf != nil {
		buf.B = buf.B[:length]
		return buf
	}

	// Trying to get a buffer from the next interval
	idx++
	if idx < pools {
		buf := p.getByIndex(idx)
		if buf != nil {
			buf.B = buf.B[:length]
			return buf
		}
	}

	return &Buffer{B: make([]byte, length, poolCapacity)}
}

func (p *Pool) getByIndex(idx int) *Buffer {
	anyBuf := p.pools[idx].Get()
	if anyBuf != nil {
		p.metrics[idx].hitCounter.Inc()
		return anyBuf.(*Buffer)
	}
	p.metrics[idx].missCounter.Inc()
	return nil
}

// Release returns Buffer to the pool.
func (p *Pool) Release(buf *Buffer) {
	capacity := cap(buf.B)
	if capacity == 0 {
		return
	}
	if capacity > maxCapacity {
		putOversizeCounter.Inc()
		return
	}

	idx, leftBorder := index(capacity)
	if capacity != leftBorder {
		// Put to the previous internal
		// because capacity is not power of 2, and it is lower than left border
		idx--
	}
	p.metrics[idx].putCounter.Inc()
	p.pools[idx].Put(buf)
}

// index returns pool index and left border of the range
func index(size int) (idx, leftBorder int) {
	idx = bits.Len(uint(size - 1))
	return idx, 1 << idx
}

func byteCountIEC(b int) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.0f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}
