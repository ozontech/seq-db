package ids

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/seq"
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

	varint := []byte{}
	var rid, prev uint64
	for _, id := range ids {
		rid = uint64(id.RID)
		varint = binary.AppendVarint(varint, int64(rid-prev))
		prev = rid
	}

	noVarint := []byte{}
	ridsBlock := BlockRIDs{Values: rids}
	noVarint = ridsBlock.Pack(noVarint)

	// varint case
	cache := NewUnpackCache()
	cache.unpackRIDs(0, varint, conf.BinaryDataV0)
	assert.Equal(t, rids, cache.values)

	// no varint case
	cache = NewUnpackCache()
	cache.unpackRIDs(0, noVarint, conf.BinaryDataV1)
	assert.Equal(t, rids, cache.values)

	// wrong format
	assert.Panics(t, func() {
		cache = NewUnpackCache()
		cache.unpackRIDs(0, varint, conf.BinaryDataV1)
	})
}
