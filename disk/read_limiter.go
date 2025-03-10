package disk

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
)

type ReadLimiter struct {
	sem    chan struct{}
	metric prometheus.Counter
}

func NewReadLimiter(maxReadsNum int, counter prometheus.Counter) *ReadLimiter {
	return &ReadLimiter{
		sem:    make(chan struct{}, maxReadsNum),
		metric: counter,
	}
}

func (r *ReadLimiter) ReadAt(f *os.File, buf []byte, offset int64) (int, error) {
	r.sem <- struct{}{}
	n, err := f.ReadAt(buf, offset)
	<-r.sem

	if r.metric != nil {
		r.metric.Add(float64(n))
	}
	return n, err
}
