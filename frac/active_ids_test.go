package frac

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSeqListAppend(t *testing.T) {
	gr := 4
	list := NewIDs()
	wg := sync.WaitGroup{}
	wg.Add(gr)
	for i := 0; i < gr; i++ {
		go func() {
			pos := make([]uint32, 0)
			for x := 0; x < 200000; x++ {
				pos = append(pos, list.Append(uint64(x)))
			}
			vals := list.GetVals()
			for x := 0; x < 200000; x++ {
				val := vals[pos[x]]
				assert.Equal(t, x, int(val), "wrong val")
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkMutexListAppend(b *testing.B) {
	gr := 2
	mu := sync.Mutex{}
	b.SetBytes(int64(gr * 8000000))
	for b.Loop() {
		list := make([]uint64, 0)
		wg := sync.WaitGroup{}
		wg.Add(gr)
		for range gr {
			go func() {
				for x := range 800000 {
					mu.Lock()
					list = append(list, uint64(x))
					mu.Unlock()
				}
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func BenchmarkSeqListAppend(b *testing.B) {
	gr := 2
	b.SetBytes(int64(gr * 8000000))
	for b.Loop() {
		list := NewIDs()
		wg := sync.WaitGroup{}
		wg.Add(gr)
		for range gr {
			go func() {
				for x := range 800000 {
					list.Append(uint64(x))
				}
				wg.Done()
			}()
		}
		wg.Wait()
	}
}
