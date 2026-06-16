package lids

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/node"
)

func TestBlockPack(t *testing.T) {
	testCases := []struct {
		name      string
		lids      []uint32
		offsets   []uint32
		generator func() ([]uint32, []uint32)
	}{
		{
			name:    "small_single_token",
			lids:    generate(4),
			offsets: []uint32{0, 4},
		},
		{
			name:    "small_a_few_token",
			lids:    generate(6),
			offsets: []uint32{0, 3, 6},
		},
		{
			name:    "small_single_lid",
			lids:    []uint32{100},
			offsets: []uint32{0, 1},
		},
		{
			name:    "small_big_lids",
			lids:    []uint32{math.MaxUint32 - 100, math.MaxUint32 - 50, math.MaxUint32 - 10},
			offsets: []uint32{0, 3},
		},
		{
			name:    "small_few_tokens",
			lids:    generate(8),
			offsets: []uint32{0, 3, 6, 8},
		},
		{
			name: "large_delta_encoded",
			generator: func() ([]uint32, []uint32) {
				lids := make([]uint32, 0)
				offsets := []uint32{0}
				startLID := uint32(100)
				for i := 0; i < 100; i++ {
					for j := 0; j < 3; j++ {
						lids = append(lids, startLID+uint32(i*10+j))
					}
					offsets = append(offsets, uint32(len(lids)))
					startLID += 30
				}
				return lids, offsets
			},
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
		},
		{
			name:    "medium_128_lids",
			lids:    generate(128),
			offsets: []uint32{0, 128},
		},
		{
			name:    "medium_127_lids",
			lids:    generate(127),
			offsets: []uint32{0, 127},
		},
		{
			name:    "medium_129_lids",
			lids:    generate(129),
			offsets: []uint32{0, 129},
		},
		{
			name:    "medium_4k_bitmap_and_small_list",
			lids:    generate(4096),
			offsets: []uint32{0, 4093, 4096},
		},
		{
			name:    "medium_4k_small_list_and_bitmap",
			lids:    generate(4096),
			offsets: []uint32{0, 3, 4096},
		},
		{
			name:    "medium_4k_hybrid",
			lids:    generate(4096),
			offsets: []uint32{0, 1000, 1005, 1010, 2000, 2100, 2103, 2106, 2107, 3000, 3500, 3505, 4096},
		},
		{
			name:    "medium_4k",
			lids:    generate(4096),
			offsets: []uint32{0, 4096},
		},
		{
			name:    "medium_4095",
			lids:    generate(4095),
			offsets: []uint32{0, 10, 50, 100, 150, 190, 1000, 1500, 4095},
		},
		{
			name:    "medium_4097",
			lids:    generate(4097),
			offsets: []uint32{0, 10, 50, 100, 150, 190, 1000, 1500, 4097},
		},
		{
			name:    "medium_64k_lids",
			lids:    generate(65536),
			offsets: []uint32{0, 65536},
		},
		{
			name:    "medium_64k_minus_one_lids",
			lids:    generate(65535),
			offsets: []uint32{0, 10, 50, 100, 150, 190, 1000, 1500, 65535},
		},
		{
			name:    "medium_64k_plus_one_lids",
			lids:    generate(65537),
			offsets: []uint32{0, 10, 50, 100, 150, 190, 1000, 1500, 65537},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var lidsIn []uint32
			var offsets []uint32

			if tc.generator != nil {
				lidsIn, offsets = tc.generator()
			} else {
				lidsIn = tc.lids
				offsets = tc.offsets
			}

			block := &UnpackedBlock{
				LIDs:    lidsIn,
				Offsets: offsets,
			}

			packer := NewBlockPacker()
			packer.LidsBitmapThreshold = 25

			packed := packer.Pack(block, nil)
			require.NotEmpty(t, packed)

			buf := &UnpackBuffer{}
			var unpacked Block
			require.NoError(t, unpacked.Unpack(packed, config.CurrentFracVersion, buf))

			assertListsEqual(t, block, &unpacked)
		})
	}
}

func TestBlockPack_VariableMixed(t *testing.T) {
	small := generate(10)
	large := generate(30)
	block := &UnpackedBlock{
		LIDs:    append(append([]uint32{}, small...), large...),
		Offsets: []uint32{0, uint32(len(small)), uint32(len(small) + len(large))},
	}

	packed := NewBlockPacker().Pack(block, nil)

	buf := &UnpackBuffer{}
	var ub Block
	require.NoError(t, ub.Unpack(packed, config.CurrentFracVersion, buf))
	assert.Equal(t, 2, ub.GetCount())
	assert.Equal(t, small, ToArray(ub.GetLIDs(0)))
	assert.Equal(t, large, ToArray(ub.GetLIDs(1)))
}

func ToArray(b node.LIDBatch) []uint32 {
	if b.IsEmpty() {
		return nil
	}
	out := make([]uint32, 0, b.Len())
	for _, lid := range b.CopyLIDs(true, nil) {
		out = append(out, lid.Unpack())
	}
	return out
}

func TestBlockPack_ReuseBuffer(t *testing.T) {
	block1 := &UnpackedBlock{
		LIDs:    generate(64 * 1024),
		Offsets: []uint32{0, 3},
	}

	block2 := &UnpackedBlock{
		LIDs:    generate(64 * 1024),
		Offsets: []uint32{0, 4},
	}

	packer := NewBlockPacker()
	packed1 := packer.Pack(block1, nil)
	packed2 := packer.Pack(block2, nil)

	buf2 := &UnpackBuffer{}

	var unpacked1, unpacked2 Block
	require.NoError(t, unpacked1.Unpack(packed1, config.CurrentFracVersion, buf2))
	require.NoError(t, unpacked2.Unpack(packed2, config.CurrentFracVersion, buf2))

	assertListsEqual(t, block1, &unpacked1)
	assertListsEqual(t, block2, &unpacked2)
}

func assertListsEqual(t *testing.T, src *UnpackedBlock, blk *Block) {
	t.Helper()
	require.Equal(t, len(src.Offsets)-1, blk.GetCount())
	for i := 0; i < blk.GetCount(); i++ {
		want := src.LIDs[src.Offsets[i]:src.Offsets[i+1]]
		assert.Equal(t, want, ToArray(blk.GetLIDs(i)))
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

func BenchmarkBlock_Pack(b *testing.B) {
	lidsIn := generate(64 * 1024)

	block := &UnpackedBlock{
		LIDs:    lidsIn,
		Offsets: []uint32{0, 64 * 1024},
	}
	packer := NewBlockPacker()

	for b.Loop() {
		packer.Pack(block, nil)
	}
}

func BenchmarkBlock_Unpack(b *testing.B) {
	lidsIn := generate(64 * 1024)

	block := &UnpackedBlock{
		LIDs:    lidsIn,
		Offsets: []uint32{0, 64 * 1024},
	}
	packed := NewBlockPacker().Pack(block, nil)

	buf := &UnpackBuffer{}

	b.ResetTimer()
	for b.Loop() {
		var ub Block
		assert.NoError(b, ub.Unpack(packed, config.CurrentFracVersion, buf))
	}
}
