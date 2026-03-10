package seqids

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
)

func TestBlockMIDs_Pack(t *testing.T) {
	tests := []struct {
		name   string
		values []uint64
	}{
		{
			name:   "small_single",
			values: []uint64{12345678901234},
		},
		{
			name:   "small_few",
			values: []uint64{100, 200, 300, 400, 500},
		},
		{
			name:   "small_4k",
			values: generate(1000000000000, 1000000),
		},
		{
			name:   "small_4k_large_values",
			values: generate(0xFFFFFFFF00000000, 1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := BlockMIDs{Values: tt.values}

			packed := original.Pack(nil, nil)

			cache := NewCache()
			defer cache.Release()

			unpacked := BlockMIDs{}
			err := unpacked.Unpack(packed, config.CurrentFracVersion, cache)

			require.NoError(t, err)
			require.EqualExportedValues(t, unpacked, original)
		})
	}
}

func generate(base uint64, increment uint64) []uint64 {
	values := make([]uint64, consts.IDsPerBlock)
	for i := range values {
		values[i] = base + uint64(i)*increment
	}
	return values
}
