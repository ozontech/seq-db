package util

type Semaphore struct {
	b chan struct{}
}

func NewSemaphore(capacity int) *Semaphore {
	return &Semaphore{
		b: make(chan struct{}, capacity),
	}
}

func (s *Semaphore) Capacity() int {
	return cap(s.b)
}

func (s *Semaphore) InProgress() int {
	return len(s.b)
}

func (s *Semaphore) TryToAcquire() bool {
	select {
	case s.b <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Semaphore) Acquire() {
	s.b <- struct{}{}
}

func (s *Semaphore) Release() {
	<-s.b
}
