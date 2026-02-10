package docsfilter

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/seq"
)

func TestIteratorDesc(t *testing.T) {
	multipleBlocksSize := maxLIDsBlockLen*3 + 15
	multipleBlocksLIDs := make([]seq.LID, 0, multipleBlocksSize)
	for i := range multipleBlocksSize {
		multipleBlocksLIDs = append(multipleBlocksLIDs, seq.LID(i))
	}

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
			expected: lidsToUint32s(multipleBlocksLIDs),
		},
		{
			title:    "ok_with_borders",
			minLID:   maxLIDsBlockLen + 10,
			maxLID:   uint32(len(multipleBlocksLIDs) - (maxLIDsBlockLen + 10)),
			expected: lidsToUint32s(multipleBlocksLIDs[maxLIDsBlockLen+10 : len(multipleBlocksLIDs)-(maxLIDsBlockLen+9)]),
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
			rawDocsFilter := marshalDocsFilter(nil, &DocsFilterBinIn{LIDs: multipleBlocksLIDs})
			filePath := filepath.Join(t.TempDir(), "some.filter")
			err := os.WriteFile(filePath, rawDocsFilter, 0o644)
			require.NoError(t, err)

			loader, err := newLoader(filePath, cache.NewCache[[]lidsBlockHeader](nil, nil))
			require.NoError(t, err)

			iterator := (*IteratorDesc)(NewIterator(loader, tc.minLID, tc.maxLID))
			resLIDs := make([]uint32, 0, len(tc.expected))
			for lid, has := iterator.Next(); has; lid, has = iterator.Next() {
				resLIDs = append(resLIDs, lid)
			}
			require.Equal(t, tc.expected, resLIDs)
		})
	}
}
