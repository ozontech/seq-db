package fetch

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/seq"
)

func TestFetchWithHint(t *testing.T) {
	testFetcher(t, NewDocumentFetcher(1), true)
}

func TestFetchWithoutHint(t *testing.T) {
	testFetcher(t, NewDocumentFetcher(1), false)
}

type testFakeFrac struct {
	frac.Fraction
	fetchCounter *atomic.Int64
	rLockCounter *atomic.Int64
	wg           *sync.WaitGroup
}

func (t *testFakeFrac) Contains(_ seq.MID) bool {
	return true
}

func (t *testFakeFrac) RLock() {
	t.rLockCounter.Inc()
}

func (t *testFakeFrac) RUnlock() {
	t.rLockCounter.Dec()
}

func (t *testFakeFrac) Info() *frac.Info {
	return &frac.Info{}
}

func (t *testFakeFrac) DataProvider(_ context.Context) (frac.DataProvider, func(), bool) {
	t.rLockCounter.Inc()

	provider := &testFakeProvider{
		fetchCounter: t.fetchCounter,
		wg:           t.wg,
	}

	return provider, func() { t.rLockCounter.Dec() }, true
}

type testFakeProvider struct {
	frac.DataProvider

	fetchCounter *atomic.Int64
	wg           *sync.WaitGroup
}

func (t *testFakeProvider) Fetch(_ seq.ID, _ []byte, _, _ *frac.UnpackCache) ([]byte, []byte, error) {
	t.fetchCounter.Inc()
	t.wg.Wait()
	return nil, nil, nil
}

// make sure fractions that are going to be fetched cannot be deleted
func TestNoWLockOnFetch(t *testing.T) {
	workers := 50

	fracList := fracmanager.FracsList{}
	wg := &sync.WaitGroup{}
	fetchCount := &atomic.Int64{}
	rLockCounter := &atomic.Int64{}

	wg.Add(1)
	for i := 0; i < workers; i++ {
		newFrac := &testFakeFrac{wg: wg, fetchCounter: fetchCount, rLockCounter: rLockCounter}
		fracList = append(fracList, newFrac)
	}

	fetcher := NewDocumentFetcher(workers)
	ch := fetcher.submitFetch(context.TODO(), fracList, []seq.IDSource{{ID: seq.SimpleID(100500)}})

	for fetchCount.Load() != int64(workers) {
		time.Sleep(time.Second)
	}

	assert.Equal(t, int64(workers), rLockCounter.Load())

	wg.Done()
	counter := 0
	for range ch {
		counter++
	}
	assert.Equal(t, workers, counter)
	assert.Equal(t, int64(0), rLockCounter.Load())
}
