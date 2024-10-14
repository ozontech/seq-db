package tracer

import (
	"math/bits"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

const (
	samplingBase    = 10
	metricSeparator = " >> "
)

type metricSampled struct {
	stopped   bool
	unstopped uint32

	count            uint32
	measured         uint32
	duration         time.Duration
	next             uint32
	startTime        time.Time
	samplingSequence func() uint32

	tracer   *Tracer
	parent   *metricSampled
	children map[string]*metricSampled
}

// Returns 1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256, etc. sequence generator
func expSamplingSequence() func() uint32 {
	var item uint32
	return func() uint32 {
		step := 1 << bits.Len(uint(item+1)>>2) // this formula is integer variant of 2^log2((x+1)/4)
		item += uint32(step)
		return item
	}
}

func newTracerMetricSampled(tracer *Tracer, parent *metricSampled) *metricSampled {
	return &metricSampled{
		tracer:           tracer,
		parent:           parent,
		children:         make(map[string]*metricSampled),
		samplingSequence: expSamplingSequence(),
	}
}

func (m *metricSampled) start() {
	m.stopped = false
	m.parent.unstopped++
	if m.needSkip() {
		m.startTime = time.Time{}
		return
	}
	m.startTime = m.tracer.nowFn()
}

func (m *metricSampled) startNested(name string) *metricSampled {
	nested := m.nested(name)
	nested.start()
	return nested
}

func (m *metricSampled) nested(name string) *metricSampled {
	nested, ok := m.children[name]
	if !ok {
		nested = newTracerMetricSampled(m.tracer, m)
		m.children[name] = nested
	}
	return nested
}

// should we skip due to sampling
func (m *metricSampled) needSkip() bool {
	if m.count < m.next {
		return true
	}
	m.next = m.samplingSequence()
	return false
}

func (m *metricSampled) Stop() {
	if m.stopped {
		logger.Warn("wrong Tracer usage: call Stop() w/o Start()")
		return
	}

	if m.unstopped > 0 {
		for name, child := range m.children { // stop nested recursively
			if !child.stopped {
				logger.Warn("tracer metric don't stopped", zap.String("name", name))
				child.Stop()
			}
		}
	}

	m.stopped = true
	m.parent.unstopped--
	m.tracer.metric = m.parent

	m.count++

	if !m.startTime.IsZero() {
		m.duration += m.tracer.sinceFn(m.startTime)
		m.measured++
	}
}

func (m *metricSampled) getValues() map[string]time.Duration {
	result := make(map[string]time.Duration)

	for name, child := range m.children {
		if !child.stopped {
			logger.Warn("wrong Tracer usage: call getValues() w/o Stop()", zap.String("name", name))
			continue
		}

		curValue := child.getValue()
		values := child.getValues()
		if len(values) > 0 {
			var sum time.Duration
			for metric, value := range values {
				sum += value
				result[name+metricSeparator+metric] = value
			}
			result[name+metricSeparator+"others"] = getNotNegative(curValue - sum) // sum can be greater due to sampling and interpolation
		} else {
			result[name] = curValue
		}
	}
	return result
}

func (m *metricSampled) getValue() time.Duration {
	if m.measured == 0 {
		return 0
	}
	return m.duration / time.Duration(m.measured) * time.Duration(m.count) // interpolation
}

func (m *metricSampled) getCounts() map[string]uint32 {
	result := make(map[string]uint32)
	for name, child := range m.children {
		if !child.stopped {
			continue
		}
		result[name] = child.count
		for metric, count := range child.getCounts() {
			result[name+metricSeparator+metric] = count
		}
	}
	return result
}

func getNotNegative(v time.Duration) time.Duration {
	if v < 0 {
		return 0
	}
	return v
}
