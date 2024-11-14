package cache

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/ozontech/seq-db/consts"
)

func TestCacheSize(t *testing.T) {
	const SIZE = 10 * consts.MB
	cleaner := NewCleaner(0, nil)
	c := NewCache[[]byte](cleaner, nil)

	c.Get(1, func() ([]byte, int) { return make([]byte, SIZE), SIZE })

	total := cleaner.getSize()
	assert.Equal(t, uint64(SIZE)+c.entrySize, total, "wrong cache size")
}

func TestClean(t *testing.T) {
	const SizeTotal = 10 * consts.MB
	const Size1 = 10 * consts.MB
	const Size2 = 2 * consts.MB
	const Size3 = 4 * consts.MB
	const Size4 = 2 * consts.MB

	cleaner := NewCleaner(SizeTotal, nil)

	c1 := NewCache[[]byte](cleaner, nil)
	c2 := NewCache[[]byte](cleaner, nil)
	c3 := NewCache[[]byte](cleaner, nil)

	stat := &CleanStat{}
	c1.Get(0, func() ([]byte, int) { return make([]byte, Size1), Size1 })

	cleaner.Rotate()
	cleaner.Cleanup(stat)

	c1.Get(1, func() ([]byte, int) { return make([]byte, Size2), Size2 })
	c2.Get(1, func() ([]byte, int) { return make([]byte, Size3), Size3 })
	c3.Get(1, func() ([]byte, int) { return make([]byte, Size4), Size4 })

	bytesTotal := cleaner.getSize()

	assert.Equal(t, int(c1.entrySize+Size1), int(stat.BytesReleased), "wrong free buckets")
	assert.Equal(t, 1, int(stat.BucketsCleaned), "wrong cleaned buckets")

	actual := c1.entrySize + Size2 + c2.entrySize + Size3 + c3.entrySize + Size4
	assert.Equal(t, int(actual), int(bytesTotal), "wrong cache size")
}

func testStress(size, workers, records int, get func(*Cache[[]uint64], int)) {
	cleaner := NewCleaner(uint64(size), nil)
	c := NewCache[[]uint64](cleaner, nil)

	done := atomic.Bool{}
	wgClean := sync.WaitGroup{}
	wgClean.Add(1)
	go func() {
		defer wgClean.Done()
		for !done.Load() {
			stat := &CleanStat{}
			cleaner.Cleanup(stat)
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
	const span = 100
	testStress(10*consts.KB, 64, 100000, func(c *Cache[[]uint64], i int) {
		j := rand.Intn(span) + i
		key := uint32(j)
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
		val := c.Get(key, func() ([]uint64, int) {
			time.Sleep(1 * time.Millisecond)
			if err != nil {
				panicFired = true
				panic(err)
			}
			return []uint64{uint64(key)}, 8
		})
		if val == nil {
			t.Errorf("cache is corrupted")
		}
		if val[0] != uint64(key) {
			t.Errorf("value is wrong")
		}
	})
}

func BenchmarkBucketClean(b *testing.B) {
	b.StopTimer()

	cleaner := NewCleaner(0, nil)
	c := NewCache[int](cleaner, nil)

	for r := 0; r < b.N; r++ {
		for i := 0; i < 1000; i++ {
			c.Get(uint32(i), func() (int, int) { return i, 4 })
		}
		cleaner.markStale(cleaner.getSize())
		b.StartTimer()
		size := c.Cleanup()
		b.StopTimer()
		if size == 0 {
			b.FailNow()
		}
	}
}
