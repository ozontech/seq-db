package fetch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tests/common"
)

func addDummyDoc(t *testing.T, fm *fracmanager.FracManager, dp *frac.DocProvider, seqID seq.ID) {
	doc := []byte("document")
	dp.Append(doc, nil, seqID, seq.Tokens("service:100500", "k8s_pod", "_all_:"))
	docs, metas := dp.Provide()
	assert.NoError(t, fm.Append(context.Background(), docs, metas, atomic.NewUint64(0)))
}

func testFetcher(t *testing.T, fetcher Fetcher, hasHint bool) {
	dataDir := common.GetTestTmpDir(t)
	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)
	config := &fracmanager.Config{
		FracSize:         1000,
		TotalSize:        100000,
		ShouldReplay:     false,
		ShouldRemoveMeta: true,
		DataDir:          dataDir,
	}
	fm, err := fracmanager.NewFracManagerWithBackgroundStart(config)
	assert.NoError(t, err)
	dp := frac.NewDocProvider()
	addDummyDoc(t, fm, dp, seq.SimpleID(1))

	fm.GetActiveFrac().WaitWriteIdle()
	info := fm.GetActiveFrac().Info()

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
	fm.GetActiveFrac().WaitWriteIdle()

	info = fm.GetActiveFrac().Info()

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

func TestOldFetchWithHint(t *testing.T) {
	testFetcher(t, NewDocumentFetcherOld(1), true)
}

func TestOldFetchWithoutHint(t *testing.T) {
	testFetcher(t, NewDocumentFetcherOld(1), false)
}
