package fracmanager

import (
	"errors"
	"sync"

	"github.com/ozontech/seq-db/frac"
)

var (
	ErrFractionNotWritable = errors.New("fraction is not writable")
)

type ReleaseSnapshot func()

type fractionsSnapshot struct {
	wg sync.WaitGroup
	f  []frac.Fraction
}

func newFractionsSnapshot(capacity int) *fractionsSnapshot {
	return &fractionsSnapshot{
		f: make([]frac.Fraction, 0, capacity),
	}
}

func (c *fractionsSnapshot) Fractions() ([]frac.Fraction, ReleaseSnapshot) {
	c.wg.Add(1)
	return c.f, c.wg.Done
}

func (c *fractionsSnapshot) AppendActive(a *syncActiveDestroyable) {
	a.wg = &c.wg
	c.f = append(c.f, a.active)
}

func (c *fractionsSnapshot) AppendSealed(s *syncSealedDestroyable) {
	s.wg = &c.wg
	c.f = append(c.f, s.sealed)
}

func (c *fractionsSnapshot) AppendRemote(r *syncRemoteDestroyable) {
	r.wg = &c.wg
	c.f = append(c.f, r.remote)
}

func (c *fractionsSnapshot) Len() int {
	if c == nil {
		return 0
	}
	return len(c.f)
}

type syncActiveDestroyable struct {
	wg     *sync.WaitGroup
	active *frac.Active
	sealed *frac.Sealed
}

func (s syncActiveDestroyable) Destroy() {
	s.wg.Wait()
	s.active.Release()
}

type syncSealedDestroyable struct {
	wg     *sync.WaitGroup
	sealed *frac.Sealed
	remote *frac.Remote
}

func (s syncSealedDestroyable) Destroy() {
	s.wg.Wait()
	s.sealed.Suicide()
}

type syncRemoteDestroyable struct {
	wg     *sync.WaitGroup
	remote *frac.Remote
}

func (s syncRemoteDestroyable) Destroy() {
	s.wg.Wait()
	s.remote.Suicide()
}
