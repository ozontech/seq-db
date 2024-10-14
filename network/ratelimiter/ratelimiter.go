package ratelimiter

import (
	"sync"
	"time"

	"go.uber.org/atomic"
)

type RateLimiter struct {
	limitPerSec  float64
	stop         atomic.Bool
	mu           *sync.Mutex
	counters     map[string]float64
	metricSetter func(v float64)
}

func NewRateLimiter(limitPerSec float64, metricSetter func(v float64)) *RateLimiter {
	return &RateLimiter{
		limitPerSec:  limitPerSec,
		mu:           &sync.Mutex{},
		counters:     make(map[string]float64),
		metricSetter: metricSetter,
	}
}

func (r *RateLimiter) Stop() {
	r.stop.Store(true)
}

func (r *RateLimiter) Start() {
	go r.start()
}

func (r *RateLimiter) start() {
	for {
		if r.stop.Load() {
			return
		}

		time.Sleep(time.Millisecond * 100)
		r.mu.Lock()
		for key := range r.counters {
			r.counters[key] -= r.limitPerSec
			if r.counters[key] <= 0 {
				delete(r.counters, key)
			}
		}
		size := len(r.counters)
		r.mu.Unlock()
		if r.metricSetter != nil {
			r.metricSetter(float64(size))
		}
	}
}

func (r *RateLimiter) Account(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.counters[key] > r.limitPerSec*10 {
		return false
	}

	r.counters[key] += 10
	return true
}
