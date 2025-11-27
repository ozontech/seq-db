package bytespool

import (
	"fmt"
	"math/bits"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var bytesPool = New()

// AcquireLen gets a byte buffer with given length from the global pool.
func AcquireLen(length int) *Buffer {
	b := bytesPool.Acquire(length)
	b.B = b.B[:length]
	return b
}

// Acquire gets a byte buffer with zero length from the global pool.
func Acquire(capacity int) *Buffer {
	b := bytesPool.Acquire(capacity)
	return b
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
		Help:      "Count of hits while obtaining a buffer from the bytes pool",
	}, []string{"capacity"})
	missCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db",
		Subsystem: "common",
		Name:      "bytes_pool_get_misses_total",
		Help:      "Count of misses while obtaining a buffer from the bytes pool",
	}, []string{"capacity"})

	// put metrics
	putCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db",
		Subsystem: "common",
		Name:      "bytes_pool_puts_total",
		Help:      "Count of buffers return to the bytes pool",
	}, []string{"capacity"})
	putOversizeCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db",
		Subsystem: "common",
		Name:      "bytes_pool_put_oversizes_total",
		Help:      "Count of reject buffers on return due to exceeding capacity",
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
	pools       = 24
	maxCapacity = 1 << (pools + 8 - 1)
)

// Pool consists of N sync.Pool's,
// each pool is a set of temporary Buffer with at least 2^n+8 capacity where the n is the pool index.
type Pool struct {
	// pools contains pools for byte slices of various capacities.
	//
	//	pools[0] at least 256 capacity
	//	pools[1] at least 512 capacity
	//	...
	//	pools[22] at least 1 073 741 824 capacity
	//	pools[23] at least 2 147 483 648 capacity
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
func (p *Pool) Acquire(capacity int) *Buffer {
	if capacity < 0 {
		panic(fmt.Errorf("invalid capacity: %d", capacity))
	}
	if capacity > maxCapacity {
		return &Buffer{B: make([]byte, 0, capacity)}
	}

	idx, poolCapacity := index(capacity)
	buf := p.getByIndex(idx)
	if buf != nil {
		buf.Reset()
		return buf
	}

	// Trying to get a buffer from the next interval
	idx++
	if idx < pools {
		buf := p.getByIndex(idx)
		if buf != nil {
			buf.Reset()
			return buf
		}
	}

	return &Buffer{B: make([]byte, 0, poolCapacity)}
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
	if capacity > maxCapacity {
		putOversizeCounter.Inc()
		return
	}

	idx, poolCapacity := index(capacity)
	if capacity != poolCapacity && idx != 0 {
		// buf is not from the pool, put to the previous interval.
		idx--
	}
	p.metrics[idx].putCounter.Inc()
	p.pools[idx].Put(buf)
}

// index returns pool index and at least capacity size of it.
func index(size int) (idx, leftBorder int) {
	if size <= 0 {
		return 0, 1 << 8
	}
	idx = bits.Len(uint(size-1)) - 8
	if idx < 0 {
		idx = 0
	}
	return idx, 1 << (idx + 8)
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
