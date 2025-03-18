package frac

type EmptyIndexProvider struct{}

func (EmptyIndexProvider) Indexes() []Index { return nil }
