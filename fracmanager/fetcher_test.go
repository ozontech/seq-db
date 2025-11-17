package fracmanager

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tests/common"
)

func testFetcher(t *testing.T, fetcher *Fetcher, hasHint bool) {
	dataDir := common.GetTestTmpDir(t)
	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)
	config := &Config{
		FracSize:  1000,
		TotalSize: 100000,
		DataDir:   dataDir,
	}

	fm, err := newFracManagerWithBackgroundStart(t.Context(), config)
	assert.NoError(t, err)
	dp := indexer.NewTestDocProvider()
	addDummyDoc(t, fm, dp, seq.SimpleID(1))
	fm.WaitIdleForTests()
	info := fm.Active().Info()

	id := seq.IDSource{
		ID: seq.SimpleID(1),
	}
	if hasHint {
		id.Hint = info.Name()
	}

	ids := []seq.IDSource{id}

	docs, err := fetcher.FetchDocs(t.Context(), fm.Fractions(), ids)
	assert.NoError(t, err)
	for _, v := range docs {
		assert.Equal(t, []byte("document"), v)
	}

	fm.SealForcedForTests()
	fm.WaitIdleForTests()
	dp.TryReset()
	addDummyDoc(t, fm, dp, seq.SimpleID(2))
	fm.WaitIdleForTests()

	info = fm.Active().Info()

	newID := seq.IDSource{
		ID: seq.SimpleID(2),
	}
	if hasHint {
		newID.Hint = info.Name()
	}
	ids = append(ids, newID)
	counter := 0
	docs, err = fetcher.FetchDocs(context.TODO(), fm.Fractions(), ids)
	assert.NoError(t, err)
	for _, v := range docs {
		counter++
		assert.Equal(t, []byte("document"), v)
	}
	assert.Equal(t, 2, counter)
}

func TestFetchWithHint(t *testing.T) {
	testFetcher(t, NewFetcher(1), true)
}

func TestFetcher_ShouldFetchMultiFrac(t *testing.T) {
	fetcher := NewFetcher(2)

	frac1 := newFakeFracWithDocs(0, 100, map[seq.ID][]byte{
		seq.SimpleID(30): []byte("doc3"),
		seq.SimpleID(10): []byte("doc1"),
	})
	frac2 := newFakeFracWithDocs(0, 100, map[seq.ID][]byte{
		seq.SimpleID(20): []byte("doc2"),
	})
	frac3 := newFakeFracWithDocs(0, 100, map[seq.ID][]byte{
		seq.SimpleID(40): []byte("doc4"),
	})
	// IDs can be provided in an arbitrary order
	fetchIDs := []seq.IDSource{
		{ID: seq.SimpleID(10)},
		{ID: seq.SimpleID(20)},
		{ID: seq.SimpleID(40)},
		{ID: seq.SimpleID(30)},
	}

	docs, err := fetcher.FetchDocs(context.Background(), List{frac1, frac2, frac3}, fetchIDs)

	assert.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("doc1"), []byte("doc2"), []byte("doc4"), []byte("doc3")}, docs)
	assert.Equal(t, 1, frac1.fetchCount)
	assert.Equal(t, 1, frac2.fetchCount)
	assert.Equal(t, 1, frac3.fetchCount)
}

func TestFetcher_DocNotFound(t *testing.T) {
	fetcher := NewFetcher(2)

	frac1 := newFakeFracWithDocs(0, 100, map[seq.ID][]byte{
		seq.SimpleID(10): []byte("apple"),
	})
	fetchIDs := []seq.IDSource{
		{ID: seq.SimpleID(10)},
		{ID: seq.SimpleID(20)},
	}

	docs, err := fetcher.FetchDocs(context.Background(), List{frac1}, fetchIDs)

	assert.NoError(t, err)
	assert.Len(t, docs, 2)
	// Found doc
	assert.Equal(t, []byte("apple"), docs[0])
	// Not found
	assert.Nil(t, docs[1])
}

func TestFetcher_ShouldUseHints(t *testing.T) {
	fetcher := NewFetcher(2)

	frac1 := newFakeFracWithDocs(0, 100, map[seq.ID][]byte{
		seq.SimpleID(30): []byte("apple"),
		seq.SimpleID(20): []byte("pineapple"),
		seq.SimpleID(10): []byte("orange"),
	})
	frac2 := newFakeFracWithDocs(0, 5, map[seq.ID][]byte{
		seq.SimpleID(5): []byte("banana"),
	})
	frac3 := newFakeFracWithDocs(40, 100, map[seq.ID][]byte{
		seq.SimpleID(50): []byte("mango"),
	})

	fetchIDs := []seq.IDSource{
		{ID: seq.SimpleID(30), Hint: frac1.Info().Name()},
		{ID: seq.SimpleID(20), Hint: frac1.Info().Name()},
		{ID: seq.SimpleID(10), Hint: frac1.Info().Name()},
	}

	docs, err := fetcher.FetchDocs(context.Background(), List{frac1, frac2, frac3}, fetchIDs)

	assert.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("apple"), []byte("pineapple"), []byte("orange")}, docs)

	assert.Equal(t, 1, frac1.fetchCount)
	// Frac2 and Frac3 are never called since we provided hints for all IDs
	assert.Equal(t, 0, frac2.fetchCount)
	assert.Equal(t, 0, frac3.fetchCount)
}

func TestFetcher_ShouldUseHints_MixedScenario(t *testing.T) {
	fetcher := NewFetcher(2)

	frac1 := newFakeFracWithDocs(0, 100, map[seq.ID][]byte{
		seq.SimpleID(30): []byte("apple"),
		seq.SimpleID(20): []byte("pineapple"),
		seq.SimpleID(10): []byte("orange"),
	})
	frac2 := newFakeFracWithDocs(0, 5, map[seq.ID][]byte{
		seq.SimpleID(5): []byte("banana"),
	})
	frac3 := newFakeFracWithDocs(40, 100, map[seq.ID][]byte{
		seq.SimpleID(50): []byte("mango"),
	})

	fetchIDs := []seq.IDSource{
		{ID: seq.SimpleID(30), Hint: frac1.Info().Name()},
		{ID: seq.SimpleID(20), Hint: frac1.Info().Name()},
		{ID: seq.SimpleID(10), Hint: frac1.Info().Name()},
		{ID: seq.SimpleID(50)},
	}

	docs, err := fetcher.FetchDocs(context.Background(), List{frac1, frac2, frac3}, fetchIDs)

	assert.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("apple"), []byte("pineapple"), []byte("orange"), []byte("mango")}, docs)

	assert.Equal(t, 1, frac1.fetchCount)
	// Frac2 is not queried since it does not overlap with request IDs MID range
	assert.Equal(t, 0, frac2.fetchCount)
	// Frac3 is now queried since it overlaps with a requested IDs MID(50) and no hint is provided
	assert.Equal(t, 1, frac3.fetchCount)
}

func TestFetcher_OutOfRangeFractions(t *testing.T) {
	fetcher := NewFetcher(2)

	frac1 := newFakeFracWithDocs(30, 100, map[seq.ID][]byte{
		seq.SimpleID(50): []byte("apple"),
		seq.SimpleID(30): []byte("banana"),
	})
	frac2 := newFakeFracWithDocs(60, 100, map[seq.ID][]byte{
		seq.SimpleID(70): []byte("pineapple"),
	})
	frac3 := newFakeFracWithDocs(0, 15, map[seq.ID][]byte{
		seq.SimpleID(10): []byte("orange"),
	})

	fetchIDs := []seq.IDSource{
		{ID: seq.SimpleID(50)},
		{ID: seq.SimpleID(40)},
		{ID: seq.SimpleID(30)},
		{ID: seq.SimpleID(20)},
	}

	docs, err := fetcher.FetchDocs(context.Background(), List{frac1}, fetchIDs)

	assert.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("apple"), nil, []byte("banana"), nil}, docs)

	assert.Equal(t, 1, frac1.fetchCount)
	// We should never call frac2 and frac3 since their MID range do not overlap with requested IDs MID range
	assert.Equal(t, 0, frac2.fetchCount)
	assert.Equal(t, 0, frac3.fetchCount)
}

func TestFetcher_FetchError(t *testing.T) {
	fetcher := NewFetcher(2)

	frac1 := newFakeFracWithDocs(0, 100, map[seq.ID][]byte{
		seq.SimpleID(20): []byte("apple"),
		seq.SimpleID(10): []byte("banana"),
	})
	frac2 := newFakeFracWithFetchError(0, 100, errors.New("fetch failed"))

	fetchIDs := []seq.IDSource{{ID: seq.SimpleID(20)}}

	_, err := fetcher.FetchDocs(context.Background(), List{frac1, frac2}, fetchIDs)

	assert.ErrorContains(t, err, "fetch failed")
}

func TestFetcher_ContextCancellation(t *testing.T) {
	fetcher := NewFetcher(2)

	frac1 := newFakeFracWithDocs(0, 100, map[seq.ID][]byte{
		seq.SimpleID(10): []byte("apple"),
	})
	frac2 := newFakeFracWithDocs(0, 100, map[seq.ID][]byte{
		seq.SimpleID(20): []byte("banana"),
	})
	frac3 := newFakeFracWithDocs(0, 100, map[seq.ID][]byte{
		seq.SimpleID(30): []byte("pineapple"),
	})
	frac4 := newFakeFracWithDocs(0, 100, map[seq.ID][]byte{
		seq.SimpleID(30): []byte("orange"),
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ids := []seq.IDSource{
		{ID: seq.SimpleID(10)},
		{ID: seq.SimpleID(20)},
	}

	_, err := fetcher.FetchDocs(ctx, List{frac1, frac2, frac3, frac4}, ids)

	assert.Error(t, err)
	assert.ErrorIs(t, context.Canceled, err)
}
