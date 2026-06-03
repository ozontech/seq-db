package skipmaskmanager

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/seq"
)

func TestLoader(t *testing.T) {
	multipleBlocksSize := maxLIDsBlockLen*3 + 15
	multipleBlocksLIDs := make([]seq.LID, 0, multipleBlocksSize)
	for i := range multipleBlocksSize {
		multipleBlocksLIDs = append(multipleBlocksLIDs, seq.LID(i))
	}

	rawSkipMask := marshalSkipMask(nil, &SkipMaskBinIn{LIDs: multipleBlocksLIDs})
	filePath := filepath.Join(t.TempDir(), "some.skipmask")
	err := os.WriteFile(filePath, rawSkipMask, 0o644)
	require.NoError(t, err)

	loader := newLoader(filePath, cache.NewCache[[]lidsBlockHeader](nil, nil))

	// test load to []uint32
	resLIDs := make([]uint32, 0, len(multipleBlocksLIDs))
	const numberOfBlocks = 4
	for i := range numberOfBlocks {
		err := loader.loadBlock(i, func(lid uint32) {
			resLIDs = append(resLIDs, lid)
		})
		require.NoError(t, err)
	}
	require.Equal(t, lidsToUint32s(multipleBlocksLIDs), resLIDs)
	require.NoError(t, loader.release())

	// test load to bitmap
	bitmap := roaring.New()
	err = loader.loadToBitmap(bitmap, 0, math.MaxUint32)
	require.NoError(t, err)
	require.Equal(t, lidsToUint32s(multipleBlocksLIDs), bitmap.ToArray())
}
