package frac

import (
	"testing"

	"github.com/ozontech/seq-db/packer"
	"github.com/ozontech/seq-db/seq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnpackCache_ValsCapacity(t *testing.T) {
	cache := NewUnpackCache()
	require.Equal(t, defaultValsCapacity, cap(cache.values))
}

func TestUnpackRIDs(t *testing.T) {
	ids := []seq.ID{{RID: 993}, {RID: 444}, {RID: 123}, {RID: 658}, {RID: 2213}}
	rids := make([]uint64, 0, len(ids))
	for _, id := range ids {
		rids = append(rids, uint64(id.RID))
	}

	varintPacker := packer.NewBytesPacker([]byte{})
	var rid, prev uint64
	for _, id := range ids {
		rid = uint64(id.RID)
		varintPacker.PutVarint(int64(rid - prev))
		prev = rid
	}

	noVarintPacker := packer.NewBytesPacker([]byte{})
	idsBlock := DiskIDsBlock{ids: ids}
	idsBlock.packRIDs(noVarintPacker)

	// varint case
	cache := NewUnpackCache()
	cache.unpackRIDs(0, varintPacker.Data, BinaryDataV0)
	assert.Equal(t, rids, cache.values)

	// no varint case
	cache = NewUnpackCache()
	cache.unpackRIDs(0, noVarintPacker.Data, BinaryDataV1)
	assert.Equal(t, rids, cache.values)

	// wrong format
	assert.Panics(t, func() {
		cache = NewUnpackCache()
		cache.unpackRIDs(0, varintPacker.Data, BinaryDataV1)
	})
}
