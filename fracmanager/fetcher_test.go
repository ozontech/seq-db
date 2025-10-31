package fracmanager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/seq"
)

func testFetcher(t *testing.T, fetcher *Fetcher, hasHint bool) {
	cfg := &Config{
		FracSize:  1000,
		TotalSize: 100000,
		Fraction:  frac.Config{SkipFsync: false},
	}

	_, fm, tearDown := setupFracManager(t, cfg)
	defer tearDown()

	dp := frac.NewDocProvider()
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

func TestFetchWithoutHint(t *testing.T) {
	testFetcher(t, NewFetcher(1), false)
}
