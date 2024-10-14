package util

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func join[V any](b *Batchan[V]) []V {
	var res []V
	var buf []V
	for {
		buf = b.Fetch(buf)
		time.Sleep(1 * time.Millisecond)
		if len(buf) == 0 {
			break
		}
		res = append(res, buf...)
	}
	return res
}

func TestBatchan(t *testing.T) {
	b := NewBatchan[byte]()
	go func() {
		b.Send('a')
		b.Send('b')
		b.Send('c')
		b.Send('d')
		time.Sleep(2 * time.Millisecond)
		b.Send('e')
		b.Send('f')
		time.Sleep(20 * time.Millisecond)
		b.Send('g')
		time.Sleep(20 * time.Millisecond)
		b.Send('h')
		for i := byte(0); i < 10; i++ {
			time.Sleep(250 * time.Microsecond)
			b.Send('0' + i)
		}
		b.Send('x')
		b.Send('y')
		b.Send('z')
		b.Close()
	}()
	assert.Equal(t, string(join(b)), "abcdefgh0123456789xyz")
}

func stress(t *testing.T, writers, values, readers int) {
	b := NewBatchan[int]()
	res := make(chan []int, readers)
	var readersWg sync.WaitGroup
	readersWg.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			res <- join(b)
			readersWg.Done()
		}()
	}
	var writersWg sync.WaitGroup
	writersWg.Add(writers)
	for i := 0; i < writers; i++ {
		ii := i
		go func() {
			for j := 0; j < values; j++ {
				b.Send(j + ii*values)
			}
			writersWg.Done()
		}()
	}
	cnt := make([]int, writers*values)
	writersWg.Wait()
	b.Close()
	readersWg.Wait()
	close(res)
	for i := range res {
		for _, j := range i {
			cnt[j]++
		}
	}
	for _, i := range cnt {
		if i != 1 {
			t.FailNow()
		}
	}
}

func TestStressSimple(t *testing.T) {
	stress(t, 1, 1, 1)
}

func TestStressSmall(t *testing.T) {
	stress(t, 2, 1000, 2)
}

func TestStressBig(t *testing.T) {
	stress(t, 100, 10000, 100)
}

func TestStressLong(t *testing.T) {
	stress(t, 1, 1000000, 1)
}
