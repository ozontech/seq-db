package fracmanager

import (
	"context"
)

// todo: get rid of such methods
func (fm *FracManager) WaitIdleForTests() {
	fm.Writer().WaitWriteIdle()
}

// todo: get rid of such methods
func (fm *FracManager) SealForcedForTests() {
	fm.mu.Lock()
	fm.lc.Rotate(0)
	fm.mu.Unlock()

	fm.lc.WaitSealing()
}

// todo: get rid of such methods
func (fm *FracManager) OffloadForcedForTests() {
	fm.SealForcedForTests() // Offloading works only for sealed fractions.

	fm.mu.Lock()
	fm.lc.OffloadLocal(context.Background(), 0)
	fm.mu.Unlock()

	fm.lc.WaitOffloading()
}
