package bench

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"
)

type Payload interface {
	Start(cleaner *Cleaner) // run main payload loop, block until ready for measurement
}

type Meter interface {
	Start(n int) // requests benchmark to run measurement and waits beginning
	Wait()       // waits measurement end
	Step()       // makes progress through measurement, eventually releasing Wait
}

type Cleaner struct {
	TeardownStart atomic.Bool
	WgTeardown    sync.WaitGroup
	WgCleanUp     sync.WaitGroup
}

func (c *Cleaner) CleanUp(fn func()) {
	c.WgCleanUp.Add(1)
	go func() {
		c.WgTeardown.Wait()
		fn()
		c.WgCleanUp.Done()
	}()
}

func Run(b *testing.B, payload Payload, meter Meter, cleaner *Cleaner) {
	cleaner.WgTeardown.Add(1)
	cleaner.WgCleanUp.Add(1)

	payload.Start(cleaner)

	// Actual benchmark
	b.Run("Measure", func(b *testing.B) {
		meter.Start(b.N)
		b.ResetTimer()
		meter.Wait()
	})

	cleaner.TeardownStart.Store(true)
	cleaner.WgTeardown.Done()
	cleaner.WgTeardown.Wait()
	cleaner.WgCleanUp.Done()
	cleaner.WgCleanUp.Wait()
	runtime.GC()
}

type IncMeter struct {
	new      atomic.Bool
	n        atomic.Int32
	wgStart  sync.WaitGroup
	wgFinish sync.WaitGroup
}

func (m *IncMeter) Start(n int) {
	m.wgStart.Add(1)
	m.wgFinish.Add(1)
	if !m.new.CAS(false, true) {
		panic("Running bench in parallel is not allowed")
	}
	if !m.n.CAS(0, int32(n)) {
		panic("Running bench in parallel is not allowed")
	}
	m.wgStart.Wait()
}

func (m *IncMeter) Step() {
	if m.new.CAS(true, false) {
		// with small probability we can sleep here due to context switch
		// and report wgStart after wgFinish
		// this is ok, though it will lead to almost zero bench time
		// for one iteration, with not too big n
		// a small price to pay for lock-free benchmark
		m.wgStart.Done()
		return
	}
	for n := m.n.Load(); n != 0; n = m.n.Load() {
		if m.n.CAS(n, n-1) {
			if n == 1 {
				m.wgFinish.Done()
			}
			return
		}
	}
}

func (m *IncMeter) Wait() {
	m.wgFinish.Wait()
}

type LockingIncMeter struct {
	new      bool
	n        int
	mux      sync.Mutex
	wgStart  sync.WaitGroup
	wgFinish sync.WaitGroup
}

func (m *LockingIncMeter) Start(n int) {
	m.mux.Lock()
	func() {
		defer m.mux.Unlock()
		m.wgStart.Add(1)
		m.wgFinish.Add(1)
		if m.new || m.n != 0 {
			panic("Running bench in parallel is not allowed")
		}
		m.new = true
		m.n = n
	}()
	m.wgStart.Wait()
}

func (m *LockingIncMeter) Step() {
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.new {
		m.new = false
		m.wgStart.Done()
		return
	}
	if m.n != 0 {
		m.n--
		if m.n == 0 {
			m.wgFinish.Done()
		}
	}
}

func (m *LockingIncMeter) Wait() {
	m.wgFinish.Wait()
}

type timeMeter struct {
	sleep    time.Duration
	wgStart  sync.WaitGroup
	wgFinish sync.WaitGroup
}

func NewTimeMeter(sleep time.Duration) Meter {
	return &timeMeter{
		sleep: sleep,
	}
}

func (m *timeMeter) Start(n int) {
	m.wgStart.Add(1)
	m.wgFinish.Add(1)
	go func() {
		m.wgStart.Done()
		time.Sleep(time.Duration(n) * m.sleep)
		m.wgFinish.Done()
	}()
	m.wgStart.Wait()
}

func (m *timeMeter) Step() {}

func (m *timeMeter) Wait() {
	m.wgFinish.Wait()
}

type warmupMeter struct {
	Meter
	stepsRemaining atomic.Int32
	delayStarted   atomic.Bool
	delayDone      atomic.Bool
	delay          time.Duration
}

func NewWarmupMeter(nested Meter, skipSteps int32, delay time.Duration) Meter {
	meter := warmupMeter{
		Meter: nested,
		delay: delay,
	}
	meter.stepsRemaining.Store(skipSteps)
	return &meter
}

func (m *warmupMeter) Step() {
	// fast track
	if m.delayDone.Load() {
		m.Meter.Step()
		return
	}

	// try to decrement steps first
	steps := m.stepsRemaining.Load()
	for steps != 0 {
		if m.stepsRemaining.CAS(steps, steps-1) {
			return
		}
		steps = m.stepsRemaining.Load()
	}

	// try to wait for delay
	if m.delayStarted.CAS(false, true) {
		if m.delay != 0 {
			go func() {
				time.Sleep(m.delay)
				if !m.delayDone.CAS(false, true) {
					panic("WarmupMeter internal error")
				}
			}()
		} else if !m.delayDone.CAS(false, true) {
			panic("WarmupMeter internal error")
		}
	}

	// delay already started, and not yet done
	// skip this step then
}
