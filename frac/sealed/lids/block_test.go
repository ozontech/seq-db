package lids

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlock_Pack_Unpack(t *testing.T) {
	testCases := []struct {
		name      string
		lids      []uint32
		offsets   []uint32
		isLastLID bool
		setup     func() ([]uint32, []uint32)
	}{
		{
			name:      "SingleToken",
			lids:      []uint32{100, 150, 200, 250},
			offsets:   []uint32{0, 4},
			isLastLID: true,
		},
		{
			name:      "MultipleTokens",
			lids:      []uint32{100, 150, 200, 250, 300, 350},
			offsets:   []uint32{0, 3, 6},
			isLastLID: true,
		},
		{
			name:      "NotLastLID",
			lids:      []uint32{100, 150, 200},
			offsets:   []uint32{0, 3},
			isLastLID: false,
		},
		{
			name:      "SingleLID",
			lids:      []uint32{100},
			offsets:   []uint32{0, 1},
			isLastLID: true,
		},
		{
			name: "ConsecutiveLIDs",
			lids: func() []uint32 {
				lids := make([]uint32, 50)
				for i := range lids {
					lids[i] = uint32(1000 + i)
				}
				return lids
			}(),
			offsets:   []uint32{0, 50},
			isLastLID: true,
		},
		{
			name:      "LargeLIDs",
			lids:      []uint32{math.MaxUint32 - 100, math.MaxUint32 - 50, math.MaxUint32 - 10},
			offsets:   []uint32{0, 3},
			isLastLID: true,
		},
		{
			name:      "MultipleTokens_IsLastLID_False",
			lids:      []uint32{100, 150, 200, 250, 300, 350, 400, 450},
			offsets:   []uint32{0, 3, 6, 8},
			isLastLID: false,
		},
		{
			name: "ManyTokens",
			setup: func() ([]uint32, []uint32) {
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
			name: "LargeBlock",
			setup: func() ([]uint32, []uint32) {
				lids := make([]uint32, 0, 200)
				startLID := uint32(1000)
				for i := 0; i < 200; i++ {
					lids = append(lids, startLID+uint32(i*10))
				}
				return lids, []uint32{0, uint32(len(lids))}
			},
			isLastLID: true,
		},
		{
			name: "LargeBlock_IsLastLID_False",
			setup: func() ([]uint32, []uint32) {
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
			name: "LargeBlockWithMultipleTokens",
			setup: func() ([]uint32, []uint32) {
				lids := make([]uint32, 0, 150)
				offsets := []uint32{0}
				startLID := uint32(1000)
				groupSize := 30
				for group := 0; group < 5; group++ {
					for i := 0; i < groupSize; i++ {
						lids = append(lids, startLID+uint32(group*groupSize*10+i*10))
					}
					offsets = append(offsets, uint32(len(lids)))
					startLID += uint32(groupSize * 10)
				}
				return lids, offsets
			},
			isLastLID: true,
		},
		{
			name: "LargeBlockWithMultipleTokens_IsLastLID_False",
			setup: func() ([]uint32, []uint32) {
				lids := make([]uint32, 0, 150)
				offsets := []uint32{0}
				startLID := uint32(1000)
				groupSize := 30
				for group := 0; group < 5; group++ {
					for i := 0; i < groupSize; i++ {
						lids = append(lids, startLID+uint32(group*groupSize*10+i*10))
					}
					offsets = append(offsets, uint32(len(lids)))
					startLID += uint32(groupSize * 10)
				}
				return lids, offsets
			},
			isLastLID: false,
		},
		{
			name: "Exactly128LIDs",
			setup: func() ([]uint32, []uint32) {
				lids := make([]uint32, 128)
				startLID := uint32(1000)
				for i := 0; i < 128; i++ {
					lids[i] = startLID + uint32(i*5)
				}
				return lids, []uint32{0, 128}
			},
			isLastLID: true,
		},
		{
			name: "Exactly128LIDs_IsLastLID_False",
			setup: func() ([]uint32, []uint32) {
				lids := make([]uint32, 128)
				startLID := uint32(1000)
				for i := 0; i < 128; i++ {
					lids[i] = startLID + uint32(i*5)
				}
				return lids, []uint32{0, 128}
			},
			isLastLID: false,
		},
		{
			name: "127LIDs",
			setup: func() ([]uint32, []uint32) {
				lids := make([]uint32, 127)
				startLID := uint32(1000)
				for i := 0; i < 127; i++ {
					lids[i] = startLID + uint32(i*5)
				}
				return lids, []uint32{0, 127}
			},
			isLastLID: true,
		},
		{
			name: "129LIDs",
			setup: func() ([]uint32, []uint32) {
				lids := make([]uint32, 129)
				startLID := uint32(1000)
				for i := 0; i < 129; i++ {
					lids[i] = startLID + uint32(i*5)
				}
				return lids, []uint32{0, 129}
			},
			isLastLID: true,
		},
		{
			name: "129LIDs_IsLastLID_False",
			setup: func() ([]uint32, []uint32) {
				lids := make([]uint32, 129)
				startLID := uint32(1000)
				for i := 0; i < 129; i++ {
					lids[i] = startLID + uint32(i*5)
				}
				return lids, []uint32{0, 129}
			},
			isLastLID: false,
		},
		{
			name: "64k_65536_LIDs",
			setup: func() ([]uint32, []uint32) {
				size := 65536
				lids := make([]uint32, size)
				startLID := uint32(1000)
				for i := 0; i < size; i++ {
					lids[i] = startLID + uint32(i)
				}
				return lids, []uint32{0, uint32(size)}
			},
			isLastLID: false,
		},
		{
			name: "64k_65539_LIDs",
			setup: func() ([]uint32, []uint32) {
				size := 65539
				lids := make([]uint32, size)
				startLID := uint32(1000)
				for i := 0; i < size; i++ {
					lids[i] = startLID + uint32(i)
				}
				return lids, []uint32{0, uint32(size)}
			},
			isLastLID: false,
		},
		{
			name: "64k_65533_LIDs",
			setup: func() ([]uint32, []uint32) {
				size := 65533
				lids := make([]uint32, size)
				startLID := uint32(1000)
				for i := 0; i < size; i++ {
					lids[i] = startLID + uint32(i)
				}
				return lids, []uint32{0, uint32(size)}
			},
			isLastLID: false,
		},
		{
			name:      "IsLastLID_True",
			lids:      []uint32{100, 150, 200, 250, 300},
			offsets:   []uint32{0, 5},
			isLastLID: true,
		},
		{
			name:      "IsLastLID_False",
			lids:      []uint32{100, 150, 200, 250, 300},
			offsets:   []uint32{0, 5},
			isLastLID: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var lids []uint32
			var offsets []uint32

			if tc.setup != nil {
				lids, offsets = tc.setup()
			} else {
				lids = tc.lids
				offsets = tc.offsets
			}

			block := &Block{
				LIDs:      lids,
				Offsets:   offsets,
				IsLastLID: tc.isLastLID,
			}

			packed := block.Pack(nil)
			require.NotEmpty(t, packed, "packed data should not be empty")

			unpacked := &Block{}
			buf := &UnpackBuffer{}
			err := unpacked.Unpack(packed, buf)
			require.NoError(t, err, "unpack should succeed")

			assert.Equal(t, block.LIDs, unpacked.LIDs, "LIDs should match")
			assert.Equal(t, block.Offsets, unpacked.Offsets, "Offsets should match")
			assert.Equal(t, block.IsLastLID, unpacked.IsLastLID, "IsLastLID should match")
		})
	}
}

func TestBlock_Pack_4k(t *testing.T) {
	lids := make([]uint32, 4*1024)
	startLID := uint32(1000)
	for i := 0; i < 4*1024; i++ {
		lids[i] = startLID + uint32(i)
	}

	block := &Block{
		LIDs:      lids,
		Offsets:   []uint32{0, 10, 50, 100, 1000, 1500, 2000, 2500, 3000, 4 * 1024},
		IsLastLID: true,
	}

	packed := block.Pack(nil)
	fmt.Println("packed len: ", len(packed))
	require.NotEmpty(t, packed, "packed data should not be empty")

	unpacked := &Block{}
	buf := &UnpackBuffer{}
	err := unpacked.Unpack(packed, buf)
	require.NoError(t, err, "unpack should succeed")

	assert.Equal(t, block.LIDs, unpacked.LIDs, "LIDs should match")
	assert.Equal(t, block.Offsets, unpacked.Offsets, "Offsets should match")
	assert.Equal(t, block.IsLastLID, unpacked.IsLastLID, "IsLastLID should match")
}

func TestBlock_Pack_4k_Dense(t *testing.T) {
	lids := make([]uint32, 4*1024)
	startLID := uint32(1000)
	for i := 0; i < 4*1024; i++ {
		lids[i] = startLID + uint32(i)
	}
	offsets := make([]uint32, 2*1024)
	for i := 0; i < 2*1024; i++ {
		offsets[i] = uint32(i)
	}
	offsets = append(offsets, 4*1024)

	block := &Block{
		LIDs:      lids,
		Offsets:   offsets,
		IsLastLID: true,
	}

	packed := block.Pack(nil)
	fmt.Println("packed len: ", len(packed))
	require.NotEmpty(t, packed, "packed data should not be empty")

	unpacked := &Block{}
	buf := &UnpackBuffer{}
	err := unpacked.Unpack(packed, buf)
	require.NoError(t, err, "unpack should succeed")

	assert.Equal(t, block.LIDs, unpacked.LIDs, "LIDs should match")
	assert.Equal(t, block.Offsets, unpacked.Offsets, "Offsets should match")
	assert.Equal(t, block.IsLastLID, unpacked.IsLastLID, "IsLastLID should match")
}

func TestBlock_Pack_64k(t *testing.T) {
	lids := make([]uint32, 64*1024)
	startLID := uint32(1000)
	for i := 0; i < 64*1024; i++ {
		lids[i] = startLID + uint32(i)
	}

	block := &Block{
		LIDs:      lids,
		Offsets:   []uint32{0, 10, 50, 100, 1000, 1500, 2000, 2500, 3000, 64 * 1024},
		IsLastLID: true,
	}

	packed := block.Pack(nil)
	fmt.Println("packed len: ", len(packed))
	require.NotEmpty(t, packed, "packed data should not be empty")

	unpacked := &Block{}
	buf := &UnpackBuffer{}
	err := unpacked.Unpack(packed, buf)
	require.NoError(t, err, "unpack should succeed")

	assert.Equal(t, block.LIDs, unpacked.LIDs, "LIDs should match")
	assert.Equal(t, block.Offsets, unpacked.Offsets, "Offsets should match")
	assert.Equal(t, block.IsLastLID, unpacked.IsLastLID, "IsLastLID should match")
}

func TestBlock_Pack_64k_Dense(t *testing.T) {
	lids := make([]uint32, 64*1024)
	startLID := uint32(1000)
	for i := 0; i < 64*1024; i++ {
		lids[i] = startLID + uint32(i)
	}
	offsets := make([]uint32, 32*1024)
	for i := 0; i < 32*1024; i++ {
		offsets[i] = uint32(i)
	}
	offsets = append(offsets, 64*1024)

	block := &Block{
		LIDs:      lids,
		Offsets:   offsets,
		IsLastLID: true,
	}

	packed := block.Pack(nil)
	fmt.Println("packed len: ", len(packed))
	require.NotEmpty(t, packed, "packed data should not be empty")

	unpacked := &Block{}
	buf := &UnpackBuffer{}
	err := unpacked.Unpack(packed, buf)
	require.NoError(t, err, "unpack should succeed")

	assert.Equal(t, block.LIDs, unpacked.LIDs, "LIDs should match")
	assert.Equal(t, block.Offsets, unpacked.Offsets, "Offsets should match")
	assert.Equal(t, block.IsLastLID, unpacked.IsLastLID, "IsLastLID should match")
}

func TestBlock_Pack_Unpack_ReuseBuffer(t *testing.T) {
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

	packed1 := block1.Pack(nil)
	packed2 := block2.Pack(nil)

	buf := &UnpackBuffer{}

	unpacked1 := &Block{}
	err := unpacked1.Unpack(packed1, buf)
	require.NoError(t, err)
	assert.Equal(t, block1.LIDs, unpacked1.LIDs)

	// Reuse the same buffer
	unpacked2 := &Block{}
	err = unpacked2.Unpack(packed2, buf)
	require.NoError(t, err)
	assert.Equal(t, block2.LIDs, unpacked2.LIDs)
}

func BenchmarkBlock_Pack(b *testing.B) {
	lids := make([]uint32, 200)
	startLID := uint32(1000)
	for i := 0; i < 200; i++ {
		lids[i] = startLID + uint32(i*10)
	}

	block := &Block{
		LIDs:      lids,
		Offsets:   []uint32{0, uint32(len(lids))},
		IsLastLID: true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		block.Pack(nil)
	}
}

func BenchmarkBlock_Unpack(b *testing.B) {
	lids := make([]uint32, 4*1024)
	startLID := uint32(1000)
	for i := 0; i < 4*1024; i++ {
		lids[i] = startLID + uint32(i*10)
	}

	block := &Block{
		LIDs:      lids,
		Offsets:   []uint32{0, 100, 500, 600, 800, 1000, 1100, 1250, 1500, 2000, 2500, 3000, 3500, 4 * 1024},
		IsLastLID: true,
	}

	packed := block.Pack(nil)
	buf := &UnpackBuffer{}
	unpacked := &Block{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		unpacked.Unpack(packed, buf)
	}
}
