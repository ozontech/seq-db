package seqids

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
)

func TestBlockMIDs_Pack_Unpack(t *testing.T) {
	tests := []struct {
		name        string
		values      []uint64
		fracVersion config.BinaryDataVersion
	}{
		{
			name:        "SlowPath_SmallBlock",
			values:      []uint64{100, 200, 300, 400, 500},
			fracVersion: config.BinaryDataV3,
		},
		{
			name:        "SlowPath_EmptyBlock",
			values:      []uint64{},
			fracVersion: config.BinaryDataV3,
		},
		{
			name:        "SlowPath_SingleValue",
			values:      []uint64{12345678901234},
			fracVersion: config.BinaryDataV3,
		},
		{
			name:        "FastPath_4kBlock",
			values:      generate4kMIDs(1000000000000, 1000000),
			fracVersion: config.BinaryDataV3,
		},
		{
			name:        "FastPath_4kBlock_LargeValues",
			values:      generate4kMIDs(0xFFFFFFFF00000000, 1),
			fracVersion: config.BinaryDataV3,
		},
		{
			name:        "FastPath_4kBlock_Sequential",
			values:      generate4kMIDs(0, 1),
			fracVersion: config.BinaryDataV3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := BlockMIDs{Values: tt.values}

			packed := original.Pack(nil)
			t.Logf("packed len: %d", len(packed))

			cache := NewCache()
			defer cache.Release()

			unpacked := BlockMIDs{}
			err := unpacked.Unpack(packed, tt.fracVersion, cache)
			require.NoError(t, err)

			require.Equal(t, len(original.Values), len(unpacked.Values), "length mismatch")
			for i := range original.Values {
				require.Equal(t, original.Values[i], unpacked.Values[i], "value mismatch at index %d", i)
			}
		})
	}
}

func generate4kMIDs(base uint64, increment uint64) []uint64 {
	values := make([]uint64, consts.IDsPerBlock)
	for i := range values {
		values[i] = base + uint64(i)*increment
	}
	return values
}
