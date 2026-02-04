package docsfilter

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/seq"
)

func TestLoader(t *testing.T) {
	multipleBlocksSize := maxLIDsBlockLen*3 + 15
	multipleBlocksLIDs := make([]seq.LID, 0, multipleBlocksSize)
	for i := range multipleBlocksSize {
		multipleBlocksLIDs = append(multipleBlocksLIDs, seq.LID(i))
	}

	rawDocsFilter := marshalDocsFilter(nil, &DocsFilterBin{LIDs: multipleBlocksLIDs})
	filePath := filepath.Join(t.TempDir(), "some.filter")
	err := os.WriteFile(filePath, rawDocsFilter, 0o644)
	require.NoError(t, err)

	loader, err := newLoader(filePath)
	require.NoError(t, err)

	err = loader.loadHeaders()
	require.NoError(t, err)
	require.Len(t, loader.headers, 4)

	resLIDs := make([]seq.LID, 0, len(multipleBlocksLIDs))
	const numberOfBlocks = 4
	for i := range numberOfBlocks {
		block, err := loader.loadBlock(i)
		require.NoError(t, err)
		resLIDs = append(resLIDs, block...)
	}
	require.Equal(t, multipleBlocksLIDs, resLIDs)

	require.NoError(t, loader.release())
}
