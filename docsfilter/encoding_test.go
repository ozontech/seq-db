package docsfilter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/seq"
)

func TestMarshalUnmarshalLIDsFilter(t *testing.T) {
	test := func(df DocsFilterBin) {
		t.Helper()

		rawDocsFilter := marshalDocsFilter(nil, &df)
		var out DocsFilterBin
		tail, err := unmarshalDocsFilter(&out, rawDocsFilter)
		require.NoError(t, err)
		require.Equal(t, 0, len(tail))
		assert.Equal(t, df, out)
	}

	test(DocsFilterBin{LIDs: []seq.LID{0, 1, 2, 3}})
	test(DocsFilterBin{LIDs: []seq.LID{10, 15, 22, 18, 105, 1010}})
	test(DocsFilterBin{LIDs: []seq.LID{11}})

	multipleBlocksSize := maxLIDsBlockLen*3 + 15
	multipleBlocksLIDs := make([]seq.LID, 0, multipleBlocksSize)
	for i := range multipleBlocksSize {
		multipleBlocksLIDs = append(multipleBlocksLIDs, seq.LID(i))
	}
	test(DocsFilterBin{LIDs: multipleBlocksLIDs})
}
