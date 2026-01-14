package active

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/tokenizer"
	"github.com/stretchr/testify/assert"
)

func TestMerge_DuplicateIDs(t *testing.T) {
	// create several indexes with overlapping IDs
	idx1 := createTestIndex(t, []seq.ID{
		{MID: 100, RID: 1}, // ID 100 will be duplicated
		{MID: 101, RID: 2},
	})

	idx2 := createTestIndex(t, []seq.ID{
		{MID: 100, RID: 1}, // duplicate of ID 100 from the first index
		{MID: 102, RID: 3},
	})

	idx3 := createTestIndex(t, []seq.ID{
		{MID: 103, RID: 4},
		{MID: 104, RID: 5},
	})

	// call mergeIndexes with indexes containing duplicated IDs
	indexes := []*memIndex{idx1, idx2, idx3}
	result := mergeIndexes(indexes)

	expectedIDs := []seq.ID{
		{MID: 104, RID: 5},
		{MID: 103, RID: 4},
		{MID: 102, RID: 3},
		{MID: 101, RID: 2},
		{MID: 100, RID: 1},
	}
	assert.Equal(t, expectedIDs, result.ids)
	assert.Equal(t, len(expectedIDs), int(result.docsCount))
	assert.Equal(t, len(expectedIDs)+1, int(result.docsSize), "we can't adjust the total size during deduplication")

	// verify that the _all_ token is correct
	allRange := result.fieldsTokens[seq.TokenAll]
	allTID := allRange.start
	assert.Equal(t, uint32(1), allRange.count)
	assert.Empty(t, result.tokenLIDs[allTID], "empty list means ALL documents")

	// verify that the foo:bar token is correct
	fooRange := result.fieldsTokens["foo"]
	fooTID := fooRange.start
	assert.Equal(t, uint32(1), fooRange.count)
	assert.Equal(t, []uint32{1, 2, 3, 4, 5}, result.tokenLIDs[fooTID], "")
}

func createTestIndex(t *testing.T, ids []seq.ID) *memIndex {
	meta := []byte{}
	for i, id := range ids {
		md := indexer.MetaData{
			ID:   id,
			Size: 1,
			Tokens: []tokenizer.MetaToken{
				{Key: []byte("foo"), Value: []byte("bar")},
				{Key: []byte("num"), Value: []byte(fmt.Sprintf("token_%d", i))},
				{Key: []byte("foo"), Value: []byte("bar")}, // duplicate
			},
		}
		tmp := md.MarshalBinaryTo(nil)
		meta = binary.LittleEndian.AppendUint32(meta, uint32(len(tmp)))
		meta = append(meta, tmp...)
	}
	idx, err := NewMemIndex(storage.CompressDocBlock(meta, nil, 1))
	assert.NoError(t, err)
	return idx
}

func TestMemIndexPool_Add_DuplicateBulk(t *testing.T) {
	idx1 := createTestIndex(t, []seq.ID{
		{MID: 100, RID: 1},
		{MID: 101, RID: 2},
	})

	idx2 := createTestIndex(t, []seq.ID{
		{MID: 102, RID: 3},
		{MID: 103, RID: 4},
	})

	idx3 := createTestIndex(t, []seq.ID{
		{MID: 102, RID: 3},
		{MID: 103, RID: 4},
	})

	assert.NotEqual(t, idx1.hash, idx2.hash)
	assert.Equal(t, idx2.hash, idx3.hash)

	info := frac.NewInfo("test", 0, 0)
	pool := NewIndexPool(info)

	// add the first index
	pool.Add(idx1, 10, 10)

	// verify the index was added
	snapshot1, release1 := pool.Snapshot()
	assert.Len(t, snapshot1.indexes, 1)
	release1()

	// add the second index
	pool.Add(idx2, 10, 10)

	// verify the index was added
	snapshot2, release2 := pool.Snapshot()
	assert.Len(t, snapshot2.indexes, 2)
	release2()

	// add the third index with the same hash
	pool.Add(idx3, 10, 10)

	// verify the third index was NOT added (should be ignored)
	snapshot3, release3 := pool.Snapshot()
	assert.Len(t, snapshot3.indexes, 2, "third index with the same hash should not be added")

	// verify that the first and second indexes remain
	assert.Equal(t, seq.MID(101), snapshot3.indexes[0].ids[0].MID)
	assert.Equal(t, seq.MID(103), snapshot3.indexes[1].ids[0].MID)
	release3()

	// verify statistics - DocsTotal should only account for the first index
	assert.Equal(t, uint32(4), info.DocsTotal)
	assert.Equal(t, uint64(4), info.DocsRaw)
	assert.Equal(t, uint64(20), info.DocsOnDisk)
	assert.Equal(t, uint64(20), info.MetaOnDisk)
	assert.Equal(t, seq.MID(100), info.From)
	assert.Equal(t, seq.MID(103), info.To)
}

func TestIndexer_TokenDeduplication(t *testing.T) {
	idx := createTestIndex(t, []seq.ID{
		{MID: 100, RID: 1},
		{MID: 101, RID: 2},
	})
	assert.Len(t, idx.tokenLIDs[idx.fieldsTokens[seq.TokenAll].start], 0)
	assert.Len(t, idx.tokenLIDs[idx.fieldsTokens["foo"].start], 2)
	assert.Len(t, idx.tokenLIDs[idx.fieldsTokens["num"].start+0], 1)
	assert.Len(t, idx.tokenLIDs[idx.fieldsTokens["num"].start+1], 1)
	assert.Equal(t, 4, idx.allTokenLIDsCount)
}
