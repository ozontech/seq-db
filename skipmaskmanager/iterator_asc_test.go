package skipmaskmanager

import (
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/seq"
)

func TestIteratorAsc(t *testing.T) {
	multipleBlocksSize := maxLIDsBlockLen*3 + 15
	multipleBlocksLIDs := make([]seq.LID, 0, multipleBlocksSize)
	for i := range multipleBlocksSize {
		multipleBlocksLIDs = append(multipleBlocksLIDs, seq.LID(i+1))
	}

	reversed := make([]uint32, len(multipleBlocksLIDs))
	copy(reversed, lidsToUint32s(multipleBlocksLIDs))
	slices.Reverse(reversed)

	type testCase struct {
		title          string
		minLID, maxLID uint32
		expected       []uint32
	}

	tests := []testCase{
		{
			title:    "ok_without_borders",
			minLID:   0,
			maxLID:   math.MaxUint32,
			expected: reversed,
		},
		{
			title:    "ok_with_borders",
			minLID:   maxLIDsBlockLen + 11,
			maxLID:   uint32(len(multipleBlocksLIDs) - (maxLIDsBlockLen + 11)),
			expected: reversed[maxLIDsBlockLen+11 : len(multipleBlocksLIDs)-(maxLIDsBlockLen+10)],
		},
		{
			title:    "ok_out_of_borders",
			minLID:   uint32(len(multipleBlocksLIDs) + 100),
			maxLID:   uint32(len(multipleBlocksLIDs) + 200),
			expected: []uint32{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.title, func(t *testing.T) {
			rawSkipMask := marshalSkipMask(nil, &SkipMaskBinIn{LIDs: multipleBlocksLIDs})
			filePath := filepath.Join(t.TempDir(), "some.skipmask")
			err := os.WriteFile(filePath, rawSkipMask, 0o644)
			require.NoError(t, err)

			loader := newLoader(filePath, cache.NewConcurrentCache[[]lidsBlockHeader](nil, nil))

			iterator := (*IteratorAsc)(NewIterator(loader, tc.minLID, tc.maxLID))
			resLIDs := make([]uint32, 0, len(tc.expected))
			for lid := iterator.Next(); !lid.IsNull(); lid = iterator.Next() {
				resLIDs = append(resLIDs, lid.Unpack())
			}
			require.Equal(t, tc.expected, resLIDs)

			require.NoError(t, loader.release())
		})
	}
}
