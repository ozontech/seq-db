package stopwatch

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	stopwatchStageLabel = "stage"
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
	t := &Stopwatch{
		nowFn:   time.Now,
		sinceFn: time.Since,
	}
	t.Reset()
	return t
}

func (sw *Stopwatch) Reset() {
	sw.root = newStopwatchMetricSampled(sw, nil)
	sw.metric = sw.root
}

func (sw *Stopwatch) Start(name string) Metric {
	m := sw.metric.startNested(name)
	sw.metric = m
	return m
}

func (sw *Stopwatch) GetValues() map[string]time.Duration {
	return sw.root.getValues()
}

func (sw *Stopwatch) GetCounts() map[string]uint32 {
	return sw.root.getCounts()
}

type UpdateMetricOption func(prometheus.Labels) prometheus.Labels

func SetLabel(name, value string) UpdateMetricOption {
	return func(labels prometheus.Labels) prometheus.Labels {
		labels[name] = value
		return labels
	}
}

func (sw *Stopwatch) Export(m *prometheus.HistogramVec, options ...UpdateMetricOption) {
	labels := prometheus.Labels{}
	for _, o := range options {
		labels = o(labels)
	}

	for name, val := range sw.GetValues() {
		labels[stopwatchStageLabel] = name
		m.With(labels).Observe(val.Seconds())
	}
	sw.Reset()
}

func (sw *Stopwatch) ExportValuesAndCounts(mv, mc *prometheus.HistogramVec, options ...UpdateMetricOption) {
	labels := prometheus.Labels{}
	for _, o := range options {
		labels = o(labels)
	}

	for name, val := range sw.GetValues() {
		labels[stopwatchStageLabel] = name
		mv.With(labels).Observe(val.Seconds())
	}
	for name, cnt := range sw.GetCounts() {
		labels[stopwatchStageLabel] = name
		mc.With(labels).Observe(float64(cnt))
	}
	sw.Reset()
}
