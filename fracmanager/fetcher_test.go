package fracmanager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tests/common"
)

func testFetcher(t *testing.T, fetcher *Fetcher, hasHint bool) {
	dataDir := common.GetTestTmpDir(t)
	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)
	config := &Config{
		FracSize:     1000,
		TotalSize:    100000,
		ShouldReplay: false,
		DataDir:      dataDir,
	}
	fm, err := newFracManagerWithBackgroundStart(config)
	assert.NoError(t, err)
	dp := frac.NewDocProvider()
	addDummyDoc(t, fm, dp, seq.SimpleID(1))
	fm.WaitIdle()
	info := fm.Active().Info()

	id := seq.IDSource{
		ID: seq.SimpleID(1),
	}
	if hasHint {
		id.Hint = info.Name()
	}

	ids := []seq.IDSource{id}

	docs, err := fetcher.FetchDocs(context.TODO(), fm.GetAllFracs(), ids)
	assert.NoError(t, err)
	for _, v := range docs {
		assert.Equal(t, []byte("document"), v)
	}

	fm.SealForcedForTests()
	fm.WaitIdle()
	dp.TryReset()
	addDummyDoc(t, fm, dp, seq.SimpleID(2))
	fm.WaitIdle()

	info = fm.Active().Info()

	newID := seq.IDSource{
		ID: seq.SimpleID(2),
	}
	if hasHint {
		newID.Hint = info.Name()
	}
	ids = append(ids, newID)
	counter := 0
	docs, err = fetcher.FetchDocs(context.TODO(), fm.GetAllFracs(), ids)
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
