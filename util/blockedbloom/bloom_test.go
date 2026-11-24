package blockedbloom

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hashUint64(value uint64) uint64 {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], value)
	return HashKey(data[:])
}

func TestBlockedBloomFalsePositiveRate(t *testing.T) {
	const keys = 1_000_000
	for _, tc := range []struct {
		name           string
		fpr            float64
		queries        int
		maxBitsKey     float64
		maxMeasuredFPR float64
	}{
		{
			name: "1pct", fpr: 0.01, queries: 1_000_000,
			maxBitsKey: 10.2, maxMeasuredFPR: 0.012,
		},
		{
			name: "0.1pct", fpr: 0.001, queries: 1_000_000,
			maxBitsKey: 16, maxMeasuredFPR: 0.0018,
		},
		{
			name: "0.01pct", fpr: 0.0001, queries: 4_000_000,
			maxBitsKey: 23, maxMeasuredFPR: 0.00032,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filter, err := New(keys, tc.fpr)
			require.NoError(t, err)

			for i := range keys {
				filter.Add(hashUint64(uint64(i)))
			}

			falseNegatives := 0
			for i := range keys {
				if !filter.MayContain(hashUint64(uint64(i))) {
					falseNegatives++
				}
			}
			require.Zero(t, falseNegatives)

			falsePositives := 0
			for i := range tc.queries {
				if filter.MayContain(hashUint64(uint64(i + keys))) {
					falsePositives++
				}
			}

			load := float64(keys) / float64(filter.BlockCount())
			modeledFPR := blockFalsePositiveRate(load, filter.layout.probes)
			assert.LessOrEqual(t, modeledFPR, tc.fpr)

			measuredFPR := float64(falsePositives) / float64(tc.queries)
			bitsPerKey := float64(filter.BlockCount()*bloomBlockBits) / keys
			t.Logf(
				"FPR %.4g, %.2f bits per key, %d probes",
				measuredFPR, bitsPerKey, filter.layout.probes,
			)

			assert.LessOrEqual(t, measuredFPR, tc.maxMeasuredFPR)
			assert.LessOrEqual(t, bitsPerKey, tc.maxBitsKey)
		})
	}
}

func BenchmarkBlockedBloom(b *testing.B) {
	const keys = 1 << 20
	filter, err := New(keys, 0.01)
	require.NoError(b, err)

	hashes := make([]uint64, keys)
	for i := range hashes {
		hashes[i] = hashUint64(uint64(i))
		filter.Add(hashes[i])
	}

	b.Run("Add", func(b *testing.B) {
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			filter.Add(hashes[i&(keys-1)])
			i++
		}
	})

	b.Run("Hit", func(b *testing.B) {
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			filter.MayContain(hashes[i&(keys-1)])
			i++
		}
	})

	for i := range hashes {
		hashes[i] = hashUint64(uint64(i + keys))
	}

	b.Run("Miss", func(b *testing.B) {
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			filter.MayContain(hashes[i&(keys-1)])
			i++
		}
	})
}
