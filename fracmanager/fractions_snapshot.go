package fracmanager

import (
	"math"
	"sync"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/seq"
)

// RefCounter provides reference counting capability.
type RefCounter interface {
	Inc()
	Dec()
}

// fractionsSnapshot represents a point-in-time view of multiple fractions
// with associated reference counters to keep them alive.
type fractionsSnapshot struct {
	counters    []RefCounter    // Reference counters to keep fractions alive
	fractions   []frac.Fraction // The actual fractions
	names       map[string]int
	oldestLocal uint64
	oldestTotal uint64
}

func newFractionsSnapshot(capacity int) fractionsSnapshot {
	return fractionsSnapshot{
		counters:    make([]RefCounter, 0, capacity),
		fractions:   make([]frac.Fraction, 0, capacity),
		names:       make(map[string]int, capacity),
		oldestLocal: math.MaxUint64,
		oldestTotal: math.MaxUint64,
	}
}

func (fs *fractionsSnapshot) AddActive(a *syncAppender) {
	fs.names[a.Info().Name()] = len(fs.fractions)

	fs.counters = append(fs.counters, a)
	fs.fractions = append(fs.fractions, a)

	fs.oldestLocal = min(fs.oldestLocal, a.Info().CreationTime)
	fs.oldestTotal = min(fs.oldestTotal, fs.oldestLocal)
}

func (fs *fractionsSnapshot) AddSealed(s *refCountedSealed) {
	fs.names[s.Info().Name()] = len(fs.fractions)

	fs.counters = append(fs.counters, s)
	fs.fractions = append(fs.fractions, s)

	fs.oldestLocal = min(fs.oldestLocal, s.Info().CreationTime)
	fs.oldestTotal = min(fs.oldestTotal, fs.oldestLocal)
}

func (fs *fractionsSnapshot) AddRemote(r *refCountedRemote) {
	fs.names[r.Info().Name()] = len(fs.fractions)

	fs.counters = append(fs.counters, r)
	fs.fractions = append(fs.fractions, r)

	fs.oldestTotal = min(fs.oldestTotal, r.Info().CreationTime)
}

// AcquireAll returns the fractions and a release function.
// Caller must call the release function when done to decrement reference counts.
func (fs *fractionsSnapshot) AcquireAll() ([]frac.Fraction, func()) {
	for _, c := range fs.counters {
		c.Inc()
	}

	counters := fs.counters // make copy of counters
	return fs.fractions, func() {
		for _, c := range counters {
			c.Dec()
		}
	}
}

func (fs *fractionsSnapshot) AcquireInRange(from, to seq.MID) ([]frac.Fraction, func()) {
	fracs := make(List, 0)
	counters := make([]RefCounter, 0)

	for i := range len(fs.fractions) {
		f := fs.fractions[i]
		c := fs.counters[i]

		if f.IsIntersecting(from, to) {
			fracs = append(fracs, f)
			c.Inc()
			counters = append(counters, c)
		}
	}

	return fracs, func() {
		for _, c := range counters {
			c.Dec()
		}
	}
}

func (fs *fractionsSnapshot) AcquireOne(name string) (frac.Fraction, func(), bool) {
	i, ok := fs.names[name]
	if !ok {
		return nil, func() {}, false
	}

	c := fs.counters[i]
	f := fs.fractions[i]

	c.Inc()
	return f, c.Dec, true
}

type refCounterWg struct {
	wg sync.WaitGroup
}

func (p *refCounterWg) Inc() { p.wg.Add(1) }

func (p *refCounterWg) Dec() { p.wg.Done() }

// refCountedActive wraps frac.Active with reference counting.
// Destroy releases the underlying Active after all references are gone.
type refCountedActive struct {
	refCounterWg
	*frac.Active
}

// Destroy waits for all references to be released and then releases the Active.
func (p *refCountedActive) Destroy() {
	p.wg.Wait()
	p.Release()
}

// refCountedSealed wraps frac.Sealed with reference counting.
type refCountedSealed struct {
	refCounterWg
	*frac.Sealed
}

// Destroy waits for all references to be released and then destroys the Sealed.
func (p *refCountedSealed) Destroy() {
	p.wg.Wait()
	p.Suicide()
}

// refCountedRemote wraps frac.Remote with reference counting.
type refCountedRemote struct {
	refCounterWg
	*frac.Remote
}

// Destroy waits for all references to be released and then destroys the Remote.
func (p *refCountedRemote) Destroy() {
	p.wg.Wait()
	p.Suicide()
}
