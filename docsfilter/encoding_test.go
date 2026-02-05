package docsfilter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/seq"
)

func TestMarshalUnmarshalLIDsFilter(t *testing.T) {
	test := func(df DocsFilterBinIn) {
		t.Helper()

		rawDocsFilter := marshalDocsFilter(nil, &df)
		var out DocsFilterBinOut
		tail, err := unmarshalDocsFilter(&out, rawDocsFilter)
		require.NoError(t, err)
		require.Equal(t, 0, len(tail))
		assert.Equal(t, lidsToUint32s(df.LIDs), out.LIDs)
	}

	test(DocsFilterBinIn{LIDs: []seq.LID{0, 1, 2, 3}})
	test(DocsFilterBinIn{LIDs: []seq.LID{10, 15, 22, 18, 105, 1010}})
	test(DocsFilterBinIn{LIDs: []seq.LID{11}})

	multipleBlocksSize := maxLIDsBlockLen*3 + 15
	multipleBlocksLIDs := make([]seq.LID, 0, multipleBlocksSize)
	for i := range multipleBlocksSize {
		multipleBlocksLIDs = append(multipleBlocksLIDs, seq.LID(i))
	}
	test(DocsFilterBinIn{LIDs: multipleBlocksLIDs})
}

func lidsToUint32s(in []seq.LID) []uint32 {
	out := make([]uint32, 0, len(in))
	for _, i := range in {
		out = append(out, uint32(i))
	}
	return out
}
