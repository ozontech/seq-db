package storage

import (
	"io"

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

func (rl *ReadLimiter) ReadAt(r io.ReaderAt, buf []byte, offset int64) (int, error) {
	rl.sem <- struct{}{}
	n, err := r.ReadAt(buf, offset)
	<-rl.sem

	if rl.metric != nil {
		rl.metric.Add(float64(n))
	}
	return n, err
}
