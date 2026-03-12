package packer

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompressDeltaBitpackUint32(t *testing.T) {
	testCases := []struct {
		name   string
		values []uint32
	}{
		{
			name:   "empty",
			values: []uint32{},
		},
		{
			name:   "small_single_value",
			values: []uint32{1},
		},
		{
			name:   "small_few_values",
			values: []uint32{1, 4, 7, 8, 10},
		},
		{
			name:   "small_127_values",
			values: generateUint32(127),
		},
		{
			name:   "small_128",
			values: generateUint32(128),
		},
		{
			name:   "small_129",
			values: generateUint32(129),
		},
		{
			name:   "midium_4k",
			values: generateUint32(4096),
		},
		{
			name:   "midium_4k_more",
			values: generateUint32(4105),
		},
		{
			name:   "midium_64k",
			values: generateUint32(64 * 1024),
		},
		{
			name:   "midium_64k_more",
			values: generateUint32(64*1024 + 34),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compressed := CompressDeltaBitpackUint32([]byte{}, tc.values, []uint32{})
			_, decompressed, err := DecompressDeltaBitpackUint32(compressed, []uint32{})
			require.NoError(t, err)
			if len(tc.values) > 0 {
				require.Equal(t, tc.values, decompressed)
			} else {
				require.Equal(t, 0, len(decompressed))
			}
		})
	}
}

func TestCompressDeltaBitpackUint64(t *testing.T) {
	testCases := []struct {
		name   string
		values []uint64
	}{
		{
			name:   "empty",
			values: []uint64{},
		},
		{
			name:   "small_single_value",
			values: []uint64{1},
		},
		{
			name:   "small_few_values",
			values: []uint64{1, 4, 7, 8, 10},
		},
		{
			name:   "small_127_values",
			values: generateUint64(127),
		},
		{
			name:   "small_128",
			values: generateUint64(128),
		},
		{
			name:   "small_129",
			values: generateUint64(129),
		},
		{
			name:   "midium_4k",
			values: generateUint64(4096),
		},
		{
			name:   "midium_4k_more",
			values: generateUint64(4105),
		},
		{
			name:   "midium_64k",
			values: generateUint64(64 * 1024),
		},
		{
			name:   "midium_64k_more",
			values: generateUint64(64*1024 + 34),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compressed := CompressDeltaBitpackUint64([]byte{}, tc.values, []uint64{})
			_, decompressed, err := DecompressDeltaBitpackUint64(compressed, []uint64{})
			require.NoError(t, err)
			if len(tc.values) > 0 {
				require.Equal(t, tc.values, decompressed)
			} else {
				require.Equal(t, 0, len(decompressed))
			}
		})
	}
}

func generateUint32(n int) []uint32 {
	v := make([]uint32, n)
	last := uint32(100)
	for i := range v {
		v[i] = last
		last += uint32(1 + rand.Intn(5))
	}
	return v
}

func generateUint64(n int) []uint64 {
	v := make([]uint64, n)
	last := uint64(100)
	for i := range v {
		v[i] = last
		last += uint64(1 + rand.Intn(5))
	}
	return v
}
