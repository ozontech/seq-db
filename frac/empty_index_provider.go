package frac

type EmptyIndexProvider struct {
}

func (EmptyIndexProvider) Type() string {
	return TypeActive
}

func (EmptyIndexProvider) Indexes() []Index {
	return []Index{}
}
