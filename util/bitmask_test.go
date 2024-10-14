package util

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBitmask(t *testing.T) {
	const maxSize = 1000
	rand.Seed(time.Now().UnixMicro())
	for k := 0; k < maxSize; k++ {
		l := rand.Intn(maxSize) + 1
		m := make([]bool, l)
		bm := NewBitmask(l)
		for i := 0; i < l; i++ {
			v := rand.Int31n(2) == 1
			bm.Set(i, v)
			m[i] = v
		}

		data := bm.GetBitmaskBinary()       // serialize
		newBm := LoadBitmask(bm.size, data) // deserialize

		assert.Equal(t, bm.bin, newBm.bin)

		for i, v := range m {
			assert.Equal(t, v, bm.Get(i), "wrong value in %d pos", i)
		}
	}
}

func TestSimpleBitmask(t *testing.T) {
	const maxSize = 96

	// Set true
	bm := NewBitmask(maxSize)
	for i := 0; i < maxSize; i++ {
		bm.Set(i, true)
	}

	assert.Equal(t, []byte{
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	}, bm.bin)

	for i := 0; i < maxSize; i++ {
		assert.True(t, bm.Get(i))
	}

	// Set false
	for i := maxSize / 3; i < maxSize/3*2+8; i++ {
		bm.Set(i, false)
	}

	assert.Equal(t, []byte{
		0xff, 0xff, 0xff, 0xff, 0x00, 0x00,
		0x00, 0x00, 0x00, 0xff, 0xff, 0xff,
	}, bm.bin)

	for i := 0; i < maxSize/3; i++ {
		assert.True(t, bm.Get(i))
	}

	for i := maxSize / 3; i < maxSize/3*2+8; i++ {
		assert.False(t, bm.Get(i))
	}

	for i := maxSize/3*2 + 8; i < maxSize; i++ {
		assert.True(t, bm.Get(i))
	}

	assert.True(t, bm.HasBitsIn(0, maxSize/3-1))
	assert.True(t, bm.HasBitsIn(8, maxSize/3+8))
	assert.False(t, bm.HasBitsIn(maxSize/3, maxSize/3*2-1))
	assert.False(t, bm.HasBitsIn(maxSize/3*2, maxSize/3*2+7))
	assert.False(t, bm.HasBitsIn(maxSize/3, maxSize/3*2+7))
	assert.True(t, bm.HasBitsIn(maxSize/3, maxSize/3*2+8))
	assert.True(t, bm.HasBitsIn(maxSize/3*2+8, maxSize-1))
}

func BenchmarkBitmask(b *testing.B) {
	const size = 10000
	rand.Seed(time.Now().UnixMicro())

	b.StopTimer()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bm := NewBitmask(size)
		for i := 0; i < size; i++ {
			v := rand.Int31n(2) == 1
			bm.Set(i, v)
		}

		b.StartTimer()
		for i := 0; i < size-1; i++ {
			for j := i + 1; j < size; j++ {
				bm.HasBitsIn(i, j)
			}
		}
		b.StopTimer()
	}
}
