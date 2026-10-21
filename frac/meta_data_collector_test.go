package frac

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
)

// buildCollector builds a collector from metas in append order and returns it
// together with the appended ids as DocsPositions.SetMultiple would report.
func buildCollector(metas []indexer.MetaData) (*metaDataCollector, []seq.ID) {
	dp := NewSyncDocsPositions()
	c := newMetaDataCollector()
	c.Init(0) // blockIndex 0
	for _, m := range metas {
		c.AppendMeta(m)
	}
	appended := dp.SetMultiple(c.IDs, c.Positions)
	return c, appended
}

// doc returns the parent meta of a document; nested returns the zero-size
// meta sharing the parent's ID and position, like indexer does.
func doc(mid seq.MID, rid uint64, size uint32, key, value string) indexer.MetaData {
	return indexer.MetaData{
		ID:     seq.ID{MID: mid, RID: seq.RID(rid)},
		Size:   size,
		Tokens: []tokenizer.MetaToken{{Key: []byte(key), Value: []byte(value)}},
	}
}

func nestedOf(id seq.ID, key, value string) indexer.MetaData {
	return indexer.MetaData{
		ID:     id, // nested metas carry the parent's ID and zero size
		Size:   0,
		Tokens: []tokenizer.MetaToken{{Key: []byte(key), Value: []byte(value)}},
	}
}

const (
	mid  = seq.MID(1000)
	size = 8
)

func TestMetaDataCollectorFilter(t *testing.T) {
	idA := seq.ID{MID: mid, RID: 1}
	idB := seq.ID{MID: mid, RID: 2}
	idC := seq.ID{MID: mid, RID: 3}

	t.Run("no duplicates", func(t *testing.T) {
		c, appended := buildCollector([]indexer.MetaData{
			doc(mid, 1, size, "f", "a"),
			doc(mid, 2, size, "f", "b"),
		})
		require.Len(t, appended, 2)

		c.Filter(appended)

		require.Equal(t, uint32(2), c.DocsCounter)
		require.Equal(t, []seq.ID{idA, idB}, c.IDs)
		require.Equal(t, c.DocsCounter, uint32(len(c.IDs)))
	})

	t.Run("in-bulk duplicate of plain docs", func(t *testing.T) {
		// same content twice in one bulk: same ID, different positions
		c, appended := buildCollector([]indexer.MetaData{
			doc(mid, 1, size, "f", "a"),
			doc(mid, 1, size, "f", "a"),
		})
		firstPos := c.Positions[0]
		require.Len(t, appended, 1) // second occurrence skipped by DocsPositions

		c.Filter(appended)

		require.Equal(t, uint32(1), c.DocsCounter)
		require.Equal(t, []seq.ID{idA}, c.IDs)
		// the kept meta is the first one, matching the stored DocsPositions entry
		require.Equal(t, firstPos, c.Positions[0])
	})

	t.Run("in-bulk duplicate keeps the first nested group", func(t *testing.T) {
		// parent + nested meta twice: nested metas share the parent's ID and
		// position, only the position distinguishes the duplicate document
		c, appended := buildCollector([]indexer.MetaData{
			doc(mid, 1, size, "p", "a"),
			nestedOf(idA, "span", "s1"),
			doc(mid, 1, size, "p", "a"),
			nestedOf(idA, "span", "s1"),
		})
		require.Len(t, appended, 2) // parent + nested of the first doc

		c.Filter(appended)

		// the first document group survives whole: parent + nested
		require.Equal(t, uint32(2), c.DocsCounter)
		require.Equal(t, []seq.ID{idA, idA}, c.IDs)
		require.Equal(t, c.Positions[0], c.Positions[1])
		// nested tokens stay grouped under the kept parent
		require.Equal(t, []uint32{1, 1}, c.tokensInDocs)
	})

	t.Run("cross-bulk duplicate is dropped entirely", func(t *testing.T) {
		// a new doc and a doc already indexed in an earlier bulk (at a
		// different position): the duplicate contributes no lids
		dp := NewSyncDocsPositions()
		dp.idToPos[idA] = seq.PackDocPos(0, 0) // stored position from bulk 1

		c := newMetaDataCollector()
		c.Init(0)
		c.AppendMeta(doc(mid, 3, size, "f", "c")) // new doc
		c.AppendMeta(doc(mid, 1, size, "f", "a")) // duplicate from bulk 1
		appended := dp.SetMultiple(c.IDs, c.Positions)

		c.Filter(appended)

		require.Equal(t, uint32(1), c.DocsCounter)
		require.Equal(t, []seq.ID{idC}, c.IDs)
		require.Equal(t, seq.MID(mid), c.MinMID)
		require.Equal(t, seq.MID(mid), c.MaxMID)
	})

	t.Run("mixed new and duplicate docs", func(t *testing.T) {
		dp := NewSyncDocsPositions()
		dp.idToPos[idA] = seq.PackDocPos(0, 0) // from bulk 1

		c := newMetaDataCollector()
		c.Init(0)
		c.AppendMeta(doc(mid, 2, size, "f", "b")) // new
		c.AppendMeta(doc(mid, 1, size, "f", "a")) // cross-bulk duplicate
		c.AppendMeta(doc(mid, 3, size, "f", "c")) // new
		c.AppendMeta(doc(mid, 3, size, "f", "c")) // in-bulk duplicate
		appended := dp.SetMultiple(c.IDs, c.Positions)

		c.Filter(appended)

		require.Equal(t, uint32(2), c.DocsCounter)
		require.Equal(t, []seq.ID{idB, idC}, c.IDs)
	})
}
