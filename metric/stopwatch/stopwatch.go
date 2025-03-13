package stopwatch

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	meterStageLabel = "stage"
)

type Metric interface {
	Stop()
}

// Stopwatch is designed to measure the time of execution of code fragments.
// Unlike OpenTelemetry Tracing it is extremely simple and lightweight.
// Even small fragments can be measured. There is no means for transferring/collecting data
// some where (storage, etc.) Stopwatch gives access to measurements only in runtime.
//   - supports nested metrics
//   - supports sampling
type Stopwatch struct {
	root   *metricSampled
	metric *metricSampled

	nowFn   func() time.Time
	sinceFn func(time.Time) time.Duration
}

func New() *Stopwatch {
	sw := &Stopwatch{
		nowFn:   time.Now,
		sinceFn: time.Since,
	}
	sw.Reset()
	return sw
}

func (s *Stopwatch) Reset() {
	s.root = newMetricSampled(s, nil)
	s.metric = s.root
}

func (s *Stopwatch) Start(name string) Metric {
	m := s.metric.startNested(name)
	s.metric = m
	return m
}

func (s *Stopwatch) GetValues() map[string]time.Duration {
	return s.root.getValues()
}

func (s *Stopwatch) GetCounts() map[string]uint32 {
	return s.root.getCounts()
}

type ExportOption func(prometheus.Labels) prometheus.Labels

func SetLabel(name, value string) ExportOption {
	return func(labels prometheus.Labels) prometheus.Labels {
		labels[name] = value
		return labels
	}
}

func (s *Stopwatch) Export(m *prometheus.HistogramVec, options ...ExportOption) {
	labels := prometheus.Labels{}
	for _, o := range options {
		labels = o(labels)
	}

	for name, val := range s.GetValues() {
		labels[meterStageLabel] = name
		m.With(labels).Observe(val.Seconds())
	}
	s.Reset()
}

func (s *Stopwatch) ExportValuesAndCounts(mv, mc *prometheus.HistogramVec, options ...ExportOption) {
	labels := prometheus.Labels{}
	for _, o := range options {
		labels = o(labels)
	}

	for name, val := range s.GetValues() {
		labels[meterStageLabel] = name
		mv.With(labels).Observe(val.Seconds())
	}
	for name, cnt := range s.GetCounts() {
		labels[meterStageLabel] = name
		mc.With(labels).Observe(float64(cnt))
	}
	s.Reset()
}
