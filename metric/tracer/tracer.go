package tracer

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	tracerStageLabel = "stage"
)

type Metric interface {
	Stop()
}

// Tracer is designed to measure the time of execution of code fragments.
// Unlike OpenTelemetry Tracing it is extremely simple and lightweight.
// Even small fragments can be measured. There is no means for transferring/collecting data
// some where (storage, etc.) Tracer gives access to measurements only in runtime.
//   - supports nested metrics
//   - supports sampling
type Tracer struct {
	root   *metricSampled
	metric *metricSampled

	nowFn   func() time.Time
	sinceFn func(time.Time) time.Duration
}

func New() *Tracer {
	t := &Tracer{
		nowFn:   time.Now,
		sinceFn: time.Since,
	}
	t.Reset()
	return t
}

func (tr *Tracer) Reset() {
	tr.root = newTracerMetricSampled(tr, nil)
	tr.metric = tr.root
}

func (tr *Tracer) Start(name string) Metric {
	m := tr.metric.startNested(name)
	tr.metric = m
	return m
}

func (tr *Tracer) GetValues() map[string]time.Duration {
	return tr.root.getValues()
}

func (tr *Tracer) GetCounts() map[string]uint32 {
	return tr.root.getCounts()
}

type UpdateMetricOption func(prometheus.Labels) prometheus.Labels

func SetLabel(name, value string) UpdateMetricOption {
	return func(labels prometheus.Labels) prometheus.Labels {
		labels[name] = value
		return labels
	}
}

func (tr *Tracer) UpdateMetric(m *prometheus.HistogramVec, options ...UpdateMetricOption) {
	labels := prometheus.Labels{}
	for _, o := range options {
		labels = o(labels)
	}

	for name, val := range tr.GetValues() {
		labels[tracerStageLabel] = name
		m.With(labels).Observe(val.Seconds())
	}
	tr.Reset()
}

func (tr *Tracer) UpdateMetricValuesAndCounts(mv, mc *prometheus.HistogramVec, options ...UpdateMetricOption) {
	labels := prometheus.Labels{}
	for _, o := range options {
		labels = o(labels)
	}

	for name, val := range tr.GetValues() {
		labels[tracerStageLabel] = name
		mv.With(labels).Observe(val.Seconds())
	}
	for name, cnt := range tr.GetCounts() {
		labels[tracerStageLabel] = name
		mc.With(labels).Observe(float64(cnt))
	}
	tr.Reset()
}
