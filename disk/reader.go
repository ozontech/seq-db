package disk

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
)

type Reader struct {
	sem    chan struct{}
	metric prometheus.Counter
}

func NewReader(maxReadsNum int, counter prometheus.Counter) *Reader {
	return &Reader{
		sem:    make(chan struct{}, maxReadsNum),
		metric: counter,
	}
}

func (r *Reader) ReadAt(f *os.File, buf []byte, offset int64) (int, error) {
	r.sem <- struct{}{}
	n, err := f.ReadAt(buf, offset)
	<-r.sem

	if r.metric != nil {
		r.metric.Add(float64(n))
	}
	return n, err
}
