package docsfilter

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/seq"
)

func TestIterator(t *testing.T) {
	multipleBlocksSize := maxLIDsBlockLen*3 + 15
	multipleBlocksLIDs := make([]seq.LID, 0, multipleBlocksSize)
	for i := range multipleBlocksSize {
		multipleBlocksLIDs = append(multipleBlocksLIDs, seq.LID(i))
	}

	type testCase struct {
		title          string
		minLID, maxLID uint32
		expected       []seq.LID
	}

	tests := []testCase{
		{
			title:    "ok_without_borders",
			minLID:   0,
			maxLID:   math.MaxUint32,
			expected: multipleBlocksLIDs,
		},
		{
			title:    "ok_with_borders",
			minLID:   maxLIDsBlockLen + 10,
			maxLID:   uint32(len(multipleBlocksLIDs) - (maxLIDsBlockLen + 10)),
			expected: multipleBlocksLIDs[maxLIDsBlockLen+10 : len(multipleBlocksLIDs)-(maxLIDsBlockLen+9)],
		},
	}

	for _, tc := range tests {
		t.Run(tc.title, func(t *testing.T) {
			rawDocsFilter := marshalDocsFilter(nil, &DocsFilterBin{LIDs: multipleBlocksLIDs})
			filePath := filepath.Join(t.TempDir(), "some.filter")
			err := os.WriteFile(filePath, rawDocsFilter, 0o644)
			require.NoError(t, err)

			loader, err := newLoader(filePath)
			require.NoError(t, err)

			iterator := NewIterator(loader, tc.minLID, tc.maxLID)
			resLIDs := make([]seq.LID, 0, len(tc.expected))
			for {
				lid, has := iterator.Next()
				if !has {
					break
				}
				resLIDs = append(resLIDs, seq.LID(lid))

			}
			require.Equal(t, tc.expected, resLIDs)
		})
	}
}
