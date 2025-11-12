package fracmanager

import "sync"

func (fm *FracManager) WaitIdleForTests() {
	fm.Writer().WaitWriteIdle()
}

func (fm *FracManager) SealForcedForTests() {
	active := fm.rotate()
	if active.frac.Info().DocsTotal > 0 {
		fm.seal(active)
	}
}

func (fm *FracManager) OffloadForcedForTests() {
	if !(fm.config.OffloadingEnabled && fm.config.OffloadingForced) {
		panic("trying to force offloading when it is disabled")
	}

	// Offloading works only for sealed fractions.
	fm.SealForcedForTests()

	var wg sync.WaitGroup
	fm.cleanupFractions(&wg)
	wg.Wait()
}
