package cache

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestCacheSize(t *testing.T) {
	const SIZE = int(10 * units.MiB)
	cleaner := NewCleaner(0, nil)
	c := NewConcurrentCache[[]byte](cleaner, nil)

	_, _ = c.Get(1, LoaderFunc[[]byte](func(uint32) ([]byte, int, error) { return make([]byte, SIZE), SIZE, nil }))

	total := cleaner.getSize()
	assert.Equal(t, uint64(SIZE)+c.entrySize, total, "wrong cache size")
}

func TestClean(t *testing.T) {
	const SizeTotal = 10 * units.MiB
	const Size1 = 10 * units.MiB
	const Size2 = 2 * units.MiB
	const Size3 = 4 * units.MiB
	const Size4 = 2 * units.MiB

	cleaner := NewCleaner(uint64(SizeTotal), nil)

	c1 := NewConcurrentCache[[]byte](cleaner, nil)
	c2 := NewConcurrentCache[[]byte](cleaner, nil)
	c3 := NewConcurrentCache[[]byte](cleaner, nil)

	stat := &CleanStat{}
	_, _ = c1.Get(0, LoaderFunc[[]byte](func(uint32) ([]byte, int, error) { return make([]byte, Size1), int(Size1), nil }))

	cleaner.Rotate()
	cleaner.Cleanup(stat)

	_, _ = c1.Get(1, LoaderFunc[[]byte](func(uint32) ([]byte, int, error) { return make([]byte, Size2), int(Size2), nil }))
	_, _ = c2.Get(1, LoaderFunc[[]byte](func(uint32) ([]byte, int, error) { return make([]byte, Size3), int(Size3), nil }))
	_, _ = c3.Get(1, LoaderFunc[[]byte](func(uint32) ([]byte, int, error) { return make([]byte, Size4), int(Size4), nil }))

	bytesTotal := cleaner.getSize()

	assert.Equal(t, int(c1.entrySize+uint64(Size1)), int(stat.BytesReleased), "wrong free buckets")
	assert.Equal(t, 1, int(stat.BucketsCleaned), "wrong cleaned buckets")

	actual := c1.entrySize + uint64(Size2) + c2.entrySize + uint64(Size3) + c3.entrySize + uint64(Size4)
	assert.Equal(t, int(actual), int(bytesTotal), "wrong cache size")
}

func testStress(size, workers, records int, get func(*ConcurrentCache[[]uint64], int)) {
	cleaner := NewCleaner(uint64(size), nil)
	c := NewConcurrentCache[[]uint64](cleaner, nil)

	done := atomic.Bool{}
	wgClean := sync.WaitGroup{}
	wgClean.Add(1)
	go func() {
		defer wgClean.Done()
		for !done.Load() {
			cleaner.Cleanup(&CleanStat{})
			time.Sleep(10 * time.Microsecond)
		}
	}()
	defer func() {
		done.Store(true)
		wgClean.Wait()
	}()

	wgGet := sync.WaitGroup{}
	wgGet.Add(workers)
	for g := 0; g < workers; g++ {
		go func() {
			defer wgGet.Done()
			for i := 0; i < records; i++ {
				get(c, i)
			}
		}()
	}
	wgGet.Wait()
}

func TestStress(t *testing.T) {
	const (
		objCount  = 1000
		getCount  = 100_000
		cacheSize = 128 * units.KiB
	)
	testStress(int(cacheSize), 64, getCount, func(c *ConcurrentCache[[]uint64], i int) {
		key := uint32(rand.Intn(objCount))
		var err interface{}
		panicFired := false
		if (rand.Intn(100)) == 0 {
			err = &struct{}{}
			defer func() {
				err1 := recover()
				if err1 == err {
					return
				}
				if err1 == nil && panicFired {
					t.Errorf("cache should have panicked")
				}
				if err1 != nil {
					panic(err)
				}
			}()
		}
		val, _ := c.Get(key, LoaderFunc[[]uint64](func(uint32) ([]uint64, int, error) {
			time.Sleep(1 * time.Millisecond)
			if err != nil {
				panicFired = true
				panic(err)
			}
			return []uint64{uint64(key)}, 32, nil
		}))
		if val == nil {
			t.Errorf("cache is corrupted")
		}
		if val[0] != uint64(key) {
			t.Errorf("value is wrong")
		}
	})
}

func BenchmarkBucketClean(b *testing.B) {
	cleaner := NewCleaner(0, nil)
	c := NewConcurrentCache[int](cleaner, nil)

	for b.Loop() {
		b.StopTimer()

		for i := range 1000 {
			_, _ = c.Get(uint32(i), LoaderFunc[int](func(uint32) (int, int, error) { return i, 4, nil }))
		}

		cleaner.markStale(cleaner.getSize())

		b.StartTimer()

		size := c.Cleanup()
		if size == 0 {
			b.FailNow()
		}
	}
}
