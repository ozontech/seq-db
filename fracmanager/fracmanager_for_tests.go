package fracmanager

import "sync"

func (fm *FracManager) WaitIdleForTests() {
	fm.lc.registry.Active().WaitWriteIdle()
}

func (fm *FracManager) SealForcedForTests() {
	wg := sync.WaitGroup{}
	fm.mu.Lock() // todo: get rid of mutex after removing SealForcedForTests method
	fm.lc.rotate(0, &wg)
	fm.mu.Unlock()

	wg.Wait()
	fm.lc.waitSealingForTests() // todo: get rid of waitSealingForTests method after removing SealForcedForTests method
}

// todo: get rid of this after removing fracmanager.SealForcedForTests()
func (lc *lifecycleManager) waitSealingForTests() {
	lc.sealingWg.Wait()
}
