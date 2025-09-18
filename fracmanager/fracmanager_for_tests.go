package fracmanager

import (
	"context"
	"sync"
)

// todo(eguguchkin) Get rid off such methods "for tests only"

func (fm *FracManager) SealForcedForTests() {
	_ = fm.lm.Seal(fm.lm.Rotate())
}

func (fm *FracManager) OffloadForcedForTests() {
	// Offloading works only for sealed fractions.
	fm.SealForcedForTests()

	var wg sync.WaitGroup
	fm.lm.OffloadLocal(context.Background(), 0, &wg)
	wg.Wait()
}

func (fm *FracManager) WaitIdleForTests() {
	fm.Writer().WaitWriteIdle()
}
