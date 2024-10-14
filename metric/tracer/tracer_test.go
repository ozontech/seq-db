package tracer

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func getTimerFn(nowCnt, sinceCnt, tikerCnt *int) (func() time.Time, func(t time.Time) time.Duration, func()) {
	start := time.Time{}
	now := func() time.Time {
		*nowCnt++
		return start
	}
	since := func(t time.Time) time.Duration {
		*sinceCnt++
		return start.Sub(t)
	}
	tiker := func() {
		*tikerCnt++
		start = start.Add(1)
	}
	return now, since, tiker
}

func TestSamplingTracer(t *testing.T) {
	var tikerFn func()
	var nowCnt, sinceCnt, tikerCnt int

	now := time.Now()
	rand.Seed(now.UnixNano())

	n := 5555

	samples := make([]uint32, 0)
	nextFn := expSamplingSequence()
	next := nextFn()
	for next < uint32(n) {
		samples = append(samples, next)
		next = nextFn()
	}
	samples = append(samples, next)

	for i := 1; i < n; i++ {
		nowCnt = 0
		sinceCnt = 0
		tikerCnt = 0

		tr := New()
		tr.nowFn, tr.sinceFn, tikerFn = getTimerFn(&nowCnt, &sinceCnt, &tikerCnt)

		for j := 0; j < i; j++ {
			l2 := tr.Start("test")
			tikerFn()
			l2.Stop()
		}
		expected := 0
		for expected < len(samples) && i > int(samples[expected]) {
			expected++
		}
		assert.Equal(t, expected, sinceCnt, "for %d", i)
	}
}

func TestTracer(t *testing.T) {
	var tikerFn func()
	var nowCnt, sinceCnt, tikerCnt int

	now := time.Now()
	rand.Seed(now.UnixNano())

	tr := New()

	tr.nowFn, tr.sinceFn, tikerFn = getTimerFn(&nowCnt, &sinceCnt, &tikerCnt)

	n := 1000
	m := tr.Start("level1")
	tracerTestLevel1(n, tr, tikerFn)
	tikerFn()
	m.Stop()

	expextedValues := map[string]time.Duration{
		"level1 >> cycle1 >> cycle2 >> stub1":            time.Duration(n * n),
		"level1 >> cycle1 >> cycle2 >> stub2":            time.Duration(n * n),
		"level1 >> cycle1 >> cycle2 >> others":           time.Duration(n),
		"level1 >> cycle1 >> level2 >> cycle2 >> stub1":  time.Duration(n * n),
		"level1 >> cycle1 >> level2 >> cycle2 >> stub2":  time.Duration(n * n),
		"level1 >> cycle1 >> level2 >> cycle2 >> others": time.Duration(n),
		"level1 >> cycle1 >> level2 >> others":           time.Duration(n),
		"level1 >> cycle1 >> others":                     0,
		"level1 >> others":                               0,
	}
	assert.Equal(t, expextedValues, tr.GetValues())

	expextedCounts := map[string]uint32{
		"level1":                                        1,
		"level1 >> cycle1":                              1,
		"level1 >> cycle1 >> cycle2":                    uint32(n),
		"level1 >> cycle1 >> cycle2 >> stub1":           uint32(n * n),
		"level1 >> cycle1 >> cycle2 >> stub2":           uint32(n * n),
		"level1 >> cycle1 >> level2":                    uint32(n),
		"level1 >> cycle1 >> level2 >> cycle2":          uint32(n),
		"level1 >> cycle1 >> level2 >> cycle2 >> stub1": uint32(n * n),
		"level1 >> cycle1 >> level2 >> cycle2 >> stub2": uint32(n * n),
	}
	assert.Equal(t, expextedCounts, tr.GetCounts())
}

func tracerTestLevel1(n int, tr *Tracer, tikerFn func()) {

	m1 := tr.Start("cycle1")

	for i := 0; i < n; i++ {

		m2 := tr.Start("level2")
		tracerTestLevel2(n, tr, tikerFn)
		tikerFn()
		m2.Stop()

		tracerTestLevel2(n, tr, tikerFn)
	}

	tikerFn()
	m1.Stop()

}

func tracerTestLevel2(n int, tr *Tracer, tikerFn func()) {

	m0 := tr.Start("cycle2")

	for i := 0; i < n; i++ {
		m1 := tr.Start("stub1")
		stub()
		tikerFn()
		m1.Stop()

		m1 = tr.Start("stub2")
		stub()
		tikerFn()
		m1.Stop()
	}

	tikerFn()
	m0.Stop()

}

func stub() {
}
