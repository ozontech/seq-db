package fracmanager

func (fm *FracManager) WaitIdleForTests() {
	fm.Writer().WaitWriteIdle()
}

func (fm *FracManager) SealForcedForTests() {
	active := fm.rotate()
	if active.frac.Info().DocsTotal > 0 {
		fm.seal(active)
	}
}
