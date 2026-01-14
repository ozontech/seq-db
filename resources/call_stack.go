package resources

type CallStack struct {
	stack []func()
}

func (s *CallStack) Defer(f func()) {
	s.stack = append(s.stack, f)
}

func (s *CallStack) CallAll() {
	for i := len(s.stack) - 1; i >= 0; i-- {
		s.stack[i]()
	}
	s.stack = s.stack[:0]
}
