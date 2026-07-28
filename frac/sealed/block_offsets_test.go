package sealed

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/config"
)

func TestBlockOffsetsPack(t *testing.T) {
	t.Parallel()

	offsets := []uint64{10, 25, 40}
	block := BlockOffsets{Offsets: offsets}

	packed := block.Pack(nil)

	require.Equal(t, packBlockOffsetsForTest(offsets, config.BinaryDataV6), packed)
}

func TestBlockOffsetsUnpack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    []byte
		fracVer config.BinaryDataVersion
		want    []uint64
		wantErr string
	}{
		{
			name:    "current_format",
			data:    packBlockOffsetsForTest([]uint64{10, 25, 40}, config.BinaryDataV6),
			fracVer: config.BinaryDataV6,
			want:    []uint64{10, 25, 40},
		},
		{
			name:    "legacy_format",
			data:    packBlockOffsetsForTest([]uint64{10, 25, 40}, config.BinaryDataV5),
			fracVer: config.BinaryDataV5,
			want:    []uint64{10, 25, 40},
		},
		{
			name:    "current_empty",
			data:    packBlockOffsetsForTest(nil, config.BinaryDataV6),
			fracVer: config.BinaryDataV6,
			want:    []uint64{},
		},
		{
			name:    "legacy_header_truncated",
			data:    binary.LittleEndian.AppendUint32(nil, 0),
			fracVer: config.BinaryDataV5,
			wantErr: "missing IDsTotal",
		},
		{
			name:    "offsets_count_mismatch",
			data:    binary.LittleEndian.AppendUint32(nil, 1),
			fracVer: config.BinaryDataV6,
			wantErr: "offset count mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var block BlockOffsets
			err := block.Unpack(tt.data, tt.fracVer)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, block.Offsets)
		})
	}
}

func packBlockOffsetsForTest(offsets []uint64, fracVer config.BinaryDataVersion) []byte {
	data := binary.LittleEndian.AppendUint32(nil, uint32(len(offsets)))
	if fracVer < config.BinaryDataV6 {
		data = binary.LittleEndian.AppendUint32(data, 42)
	}

	var prev uint64
	for _, offset := range offsets {
		data = binary.AppendVarint(data, int64(offset-prev))
		prev = offset
	}

	return data
}
