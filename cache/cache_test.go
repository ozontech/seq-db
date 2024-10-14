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
	size := &atomic.Uint64{}
	cleaner := NewCleaner([]*atomic.Uint64{size})
	c := NewCache[[]byte](size, nil)
	cleaner.AddBuckets(c)

	c.Get(1, func() ([]byte, int) { return make([]byte, SIZE), SIZE })

	total := cleaner.GetSize()
	assert.Equal(t, uint64(SIZE), total, "wrong cache size")
}

func TestClean(t *testing.T) {
	const SizeTotal = 10 * consts.MB
	const Size1 = 10 * consts.MB
	const Size2 = 2 * consts.MB
	const Size3 = 4 * consts.MB
	const Size4 = 2 * consts.MB

	size := &atomic.Uint64{}
	cleaner := NewCleaner([]*atomic.Uint64{size})
	c1 := NewCache[[]byte](size, nil)
	c2 := NewCache[[]byte](size, nil)
	c3 := NewCache[[]byte](size, nil)
	cleaner.AddBuckets(c1, c2, c3)
	stat := &CleanStat{}

	c1.Get(0, func() ([]byte, int) { return make([]byte, Size1), Size1 })
	cleaner.CleanUp(SizeTotal, stat)

	c1.Get(1, func() ([]byte, int) { return make([]byte, Size2), Size2 })
	c2.Get(1, func() ([]byte, int) { return make([]byte, Size3), Size3 })
	c3.Get(1, func() ([]byte, int) { return make([]byte, Size4), Size4 })

	success := cleaner.CleanUp(SizeTotal, stat)
	assert.Truef(t, success, "cache doesn't clean")

	bytesTotal := cleaner.GetSize()

	assert.Equal(t, Size1, int(stat.Bytes), "wrong free buckets")
	assert.Equal(t, 1, int(stat.Buckets), "wrong cleaned buckets")

	assert.Equal(t, Size2+Size3+Size4, int(bytesTotal), "wrong cache size")
}

func testStress(size, workers, records int, get func(*Cache[[]uint64], int)) {
	s := &atomic.Uint64{}
	cleaner := NewCleaner([]*atomic.Uint64{s})
	c := NewCache[[]uint64](s, nil)
	cleaner.AddBuckets(c)
	stat := &CleanStat{}

	done := atomic.Bool{}
	wgClean := sync.WaitGroup{}
	wgClean.Add(1)
	go func() {
		defer wgClean.Done()
		for !done.Load() {
			cleaner.CleanUp(uint64(size), stat)
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
	c := NewCache[int](nil, nil)
	for r := 0; r < 1000; r++ {
		for i := 0; i < b.N; i++ {
			c.Get(uint32(i), func() (int, int) { return i, 4 })
		}
		b.StartTimer()
		size := c.RetireOldGenerations(0)
		b.StopTimer()
		if size == 0 || size != b.N*4 {
			b.FailNow()
		}
	}
}
