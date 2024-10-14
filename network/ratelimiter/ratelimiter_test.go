package ratelimiter

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRateLimiter(t *testing.T) {
	limit := 5.0
	seconds := 1
	rl := NewRateLimiter(limit, nil)

	rl.Start()
	defer rl.Stop()

	wg := sync.WaitGroup{}
	wg.Add(1)
	keys := []string{"key1"}
	vals := []int{0}
	go func() {
		t := time.Now()
		for {
			index := rand.Int() % len(keys)
			if rl.Account(keys[index]) {
				vals[index]++
			}

			if time.Since(t) >= time.Duration(seconds)*time.Second {
				break
			}
		}
		wg.Done()
	}()

	wg.Wait()

	for _, val := range vals {
		assert.LessOrEqual(t, int(limit*float64(seconds)), val, "wrong limited counter")
	}
}
