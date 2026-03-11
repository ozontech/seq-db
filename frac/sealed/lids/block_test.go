package lids

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/config"
)

func TestBlockPack(t *testing.T) {
	testCases := []struct {
		name      string
		lids      []uint32
		offsets   []uint32
		isLastLID bool
		generator func() ([]uint32, []uint32)
	}{
		{
			name:      "small_single_token",
			lids:      generate(4),
			offsets:   []uint32{0, 4},
			isLastLID: true,
		},
		{
			name:      "small_a_few_token",
			lids:      generate(6),
			offsets:   []uint32{0, 3, 6},
			isLastLID: true,
		},
		{
			name:      "small_not_last_lid",
			lids:      generate(3),
			offsets:   []uint32{0, 3},
			isLastLID: false,
		},
		{
			name:      "small_single_lid",
			lids:      []uint32{100},
			offsets:   []uint32{0, 1},
			isLastLID: true,
		},
		{
			name:      "small_big_lids",
			lids:      []uint32{math.MaxUint32 - 100, math.MaxUint32 - 50, math.MaxUint32 - 10},
			offsets:   []uint32{0, 3},
			isLastLID: true,
		},
		{
			name:      "small_few_tokens",
			lids:      generate(8),
			offsets:   []uint32{0, 3, 6, 8},
			isLastLID: false,
		},
		{
			name: "medium_many_tokens",
			generator: func() ([]uint32, []uint32) {
				lids := make([]uint32, 0)
				offsets := []uint32{0}
				startLID := uint32(100)
				for i := 0; i < 10; i++ {
					for j := 0; j < 3; j++ {
						lids = append(lids, startLID+uint32(i*10+j))
					}
					offsets = append(offsets, uint32(len(lids)))
					startLID += 30
				}
				return lids, offsets
			},
			isLastLID: true,
		},
		{
			name: "large_is_last_lid_false",
			generator: func() ([]uint32, []uint32) {
				lids := make([]uint32, 0, 200)
				startLID := uint32(1000)
				for i := 0; i < 200; i++ {
					lids = append(lids, startLID+uint32(i*10))
				}
				return lids, []uint32{0, uint32(len(lids))}
			},
			isLastLID: false,
		},
		{
			name: "large_many_tokens",
			generator: func() ([]uint32, []uint32) {
				lids := make([]uint32, 0, 150)
				offsets := []uint32{0}
				groupSize := 30
				for group := 0; group < 5; group++ {
					for i := 0; i < groupSize; i++ {
						lids = append(lids, 1+uint32(group*groupSize*10+i*10))
					}
					offsets = append(offsets, uint32(len(lids)))
				}
				return lids, offsets
			},
			isLastLID: true,
		},
		{
			name:      "medium_128_lids",
			lids:      generate(128),
			offsets:   []uint32{0, 128},
			isLastLID: true,
		},
		{
			name:      "medium_127_lids",
			lids:      generate(127),
			offsets:   []uint32{0, 127},
			isLastLID: true,
		},
		{
			name:      "medium_129_lids",
			lids:      generate(129),
			offsets:   []uint32{0, 129},
			isLastLID: true,
		},
		{
			name:      "medium_4k_lids",
			lids:      generate(4096),
			offsets:   []uint32{0, 4096},
			isLastLID: true,
		},
		{
			name:      "medium_4k_minus_one_lids",
			lids:      generate(4095),
			offsets:   []uint32{0, 10, 50, 100, 150, 190, 1000, 1500, 4095},
			isLastLID: true,
		},
		{
			name:      "medium_4k_plus_one_lids",
			lids:      generate(4097),
			offsets:   []uint32{0, 10, 50, 100, 150, 190, 1000, 1500, 4097},
			isLastLID: true,
		},
		{
			name:      "medium_64k_lids",
			lids:      generate(65536),
			offsets:   []uint32{0, 65536},
			isLastLID: false,
		},
		{
			name:      "medium_64k_minus_one_lids",
			lids:      generate(65535),
			offsets:   []uint32{0, 10, 50, 100, 150, 190, 1000, 1500, 65535},
			isLastLID: true,
		},
		{
			name:      "medium_64k_plus_one_lids",
			lids:      generate(65537),
			offsets:   []uint32{0, 10, 50, 100, 150, 190, 1000, 1500, 65537},
			isLastLID: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var lids []uint32
			var offsets []uint32

			if tc.generator != nil {
				lids, offsets = tc.generator()
			} else {
				lids = tc.lids
				offsets = tc.offsets
			}

			block := &Block{
				LIDs:      lids,
				Offsets:   offsets,
				IsLastLID: tc.isLastLID,
			}

			packed := block.Pack(nil, nil)
			require.NotEmpty(t, packed)

			unpacked := &Block{}
			buf := &UnpackBuffer{}
			err := unpacked.Unpack(packed, config.CurrentFracVersion, buf)

			require.NoError(t, err)
			assert.EqualExportedValues(t, block, unpacked)
		})
	}
}

func generate(n int) []uint32 {
	v := make([]uint32, n)
	last := uint32(100)
	for i := range v {
		v[i] = last
		last += uint32(1 + rand.Intn(5))
	}
	return v
}

func TestBlockPack_ReuseBuffer(t *testing.T) {
	// Test that UnpackBuffer can be reused
	block1 := &Block{
		LIDs:      []uint32{100, 150, 200},
		Offsets:   []uint32{0, 3},
		IsLastLID: true,
	}

	block2 := &Block{
		LIDs:      []uint32{300, 350, 400, 450},
		Offsets:   []uint32{0, 4},
		IsLastLID: true,
	}

	packed1 := block1.Pack(nil, nil)
	packed2 := block2.Pack(nil, nil)

	buf := &UnpackBuffer{}

	unpacked1 := &Block{}
	err := unpacked1.Unpack(packed1, config.CurrentFracVersion, buf)
	require.NoError(t, err)
	assert.Equal(t, block1.LIDs, unpacked1.LIDs)

	unpacked2 := &Block{}
	err = unpacked2.Unpack(packed2, config.CurrentFracVersion, buf)
	require.NoError(t, err)
	assert.Equal(t, block2.LIDs, unpacked2.LIDs)
}

func BenchmarkBlock_Pack(b *testing.B) {
	lids := generate(64 * 1024)

	block := &Block{
		LIDs:      lids,
		Offsets:   []uint32{0, 64 * 1024},
		IsLastLID: true,
	}
	tmp := make([]uint32, 0, 64*1024/4)

	for b.Loop() {
		block.Pack(nil, tmp)
	}
}

func BenchmarkBlock_Unpack(b *testing.B) {
	lids := generate(64 * 1024)

	block := &Block{
		LIDs:      lids,
		Offsets:   []uint32{0, 64 * 1024},
		IsLastLID: true,
	}
	packed := block.Pack(nil, nil)

	buf := &UnpackBuffer{}
	unpacked := &Block{}

	b.ResetTimer()
	for b.Loop() {
		err := unpacked.Unpack(packed, config.CurrentFracVersion, buf)
		assert.NoError(b, err)
	}
}
