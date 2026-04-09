package skipmaskmanager

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/seq"
)

func TestMarshalUnmarshalSkipMask(t *testing.T) {
	test := func(df SkipMaskBinIn) {
		t.Helper()

		rawSkipMask := marshalSkipMask(nil, &df)
		var out SkipMaskBinOut
		tail, err := unmarshalSkipMask(&out, rawSkipMask)
		require.NoError(t, err)
		require.Equal(t, 0, len(tail))
		assert.Equal(t, lidsToUint32s(df.LIDs), out.LIDs)
	}

	test(SkipMaskBinIn{LIDs: []seq.LID{0, 1, 2, 3}})
	test(SkipMaskBinIn{LIDs: []seq.LID{10, 15, 22, 18, 105, 1010}})
	test(SkipMaskBinIn{LIDs: []seq.LID{11}})

	multipleBlocksSize := maxLIDsBlockLen*3 + 15
	multipleBlocksLIDs := make([]seq.LID, 0, multipleBlocksSize)
	for i := range multipleBlocksSize {
		multipleBlocksLIDs = append(multipleBlocksLIDs, seq.LID(i))
	}
	test(SkipMaskBinIn{LIDs: multipleBlocksLIDs})
}

func lidsToUint32s(in []seq.LID) []uint32 {
	out := make([]uint32, 0, len(in))
	for _, i := range in {
		out = append(out, uint32(i))
	}
	return out
}
