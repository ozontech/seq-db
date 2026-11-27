package asyncsearcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/mappingprovider"
	"github.com/ozontech/seq-db/seq"
)

type fakeFrac struct {
	frac.Fraction
	info frac.Info
	dp   fakeDP
}

func (f *fakeFrac) Info() *frac.Info {
	return &f.info
}

func (f *fakeFrac) IsIntersecting(from, to seq.MID) bool {
	return true
}

func (f *fakeFrac) Search(context.Context, processor.SearchParams) (*seq.QPR, error) {
	return &f.dp.qpr, nil
}

type fakeDP struct {
	qpr seq.QPR
}

type fakeFractionProvider fracmanager.List

func (fp fakeFractionProvider) AcquireFractionsInRange(from, to seq.MID) (fracmanager.List, func()) {
	return fracmanager.List(fp), func() {}
}

func TestAsyncSearcherMaintain(t *testing.T) {
	r := require.New(t)

	cfg := AsyncSearcherConfig{
		DataDir: t.TempDir(),
	}
	as := newTestAsync(t, cfg)

	req := AsyncSearchRequest{
		ID:        uuid.New().String(),
		Params:    processor.SearchParams{},
		Query:     "*",
		Retention: time.Hour,
	}

	fracs := fakeFractionProvider{
		&fakeFrac{info: frac.Info{Path: "1"}},
	}
	r.NoError(as.StartSearch(req, fracs))

	as.processWg.Wait()
}

func TestMerge(t *testing.T) {
	tests := []struct {
		name     string
		order    seq.DocsOrder
		expected []seq.MID
	}{
		{
			name:     "desc",
			order:    seq.DocsOrderDesc,
			expected: []seq.MID{2, 1},
		},
		{
			name:     "asc",
			order:    seq.DocsOrderAsc,
			expected: []seq.MID{1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			now := time.Now()

			cfg := AsyncSearcherConfig{DataDir: t.TempDir()}
			as := newTestAsync(t, cfg)

			frac1 := &fakeFrac{
				info: frac.Info{Path: "1", From: seq.TimeToMID(now.Add(-time.Minute * 11)), To: seq.TimeToMID(now.Add(-time.Minute * 6))},
				dp:   fakeDP{qpr: seq.QPR{IDs: []seq.IDSource{{ID: seq.ID{MID: 1}}}, Total: 1}},
			}
			frac2 := &fakeFrac{
				info: frac.Info{Path: "2", From: seq.TimeToMID(now.Add(-time.Minute * 6)), To: seq.TimeToMID(now.Add(-time.Minute * 1))},
				dp:   fakeDP{qpr: seq.QPR{IDs: []seq.IDSource{{ID: seq.ID{MID: 2}}}, Total: 1}},
			}
			provider := &fakeFractionProvider{frac1, frac2}

			req := AsyncSearchRequest{
				ID: uuid.New().String(),
				Params: processor.SearchParams{
					Limit: 1000,
					Order: tt.order,
					From:  seq.TimeToMID(now.UTC().Add(-time.Minute * 30).Truncate(time.Millisecond)),
					To:    seq.TimeToMID(now.UTC().Truncate(time.Millisecond)),
				},
				Query:     "*",
				Retention: time.Hour,
			}
			r.NoError(as.StartSearch(req, provider))
			as.processWg.Wait()

			as.merge()

			resp, ok := as.FetchSearchResult(FetchSearchResultRequest{ID: req.ID, Limit: 1000, Order: tt.order})
			r.True(ok)
			r.Equal(AsyncSearchStatusDone, resp.Status)
			r.Len(resp.QPR.IDs, 2)
			r.Equal(tt.expected[0], resp.QPR.IDs[0].ID.MID)
			r.Equal(tt.expected[1], resp.QPR.IDs[1].ID.MID)

			info, ok := as.getSearchInfo(req.ID)
			r.True(ok)
			r.True(info.merged.Load())
		})
	}
}

func TestBuildIntervals(t *testing.T) {
	tests := []struct {
		name     string
		from     seq.MID
		to       seq.MID
		expected []searchInterval
	}{
		{
			name: "single_point_range_from_equals_to",
			from: 100,
			to:   100,
			expected: []searchInterval{
				{100, 100},
			},
		},
		{
			name: "single_interval_small_range",
			from: 0,
			to:   100,
			expected: []searchInterval{
				{0, 100},
			},
		},
		{
			name: "single_interval_exact_split",
			from: 0,
			to:   seq.DurationToMID(defaultSearchInterval),
			expected: []searchInterval{
				{0, 300_000_000_000},
			},
		},
		{
			name: "two_intervals",
			from: 0,
			to:   seq.DurationToMID(defaultSearchInterval) * 2,
			expected: []searchInterval{
				{0, 299_999_999_999},
				{300_000_000_000, 600_000_000_000},
			},
		},
		{
			name: "three_intervals_with_remainder",
			from: 0,
			to:   seq.DurationToMID(defaultSearchInterval)*3 + 50,
			expected: []searchInterval{
				{0, 299_999_999_999},
				{300_000_000_000, 599_999_999_999},
				{600_000_000_000, 899_999_999_999},
				{900_000_000_000, 900_000_000_050},
			},
		},
		{
			name: "minimal_range",
			from: 5,
			to:   6,
			expected: []searchInterval{
				{5, 6},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			result := buildIntervals(tt.from, tt.to)
			r.Equal(tt.expected, result)
			r.Equal(len(result), countIntervals(tt.from, tt.to))
		})
	}
}

func TestAsyncSearchProgressCountsEmptyIntervals(t *testing.T) {
	r := require.New(t)

	cfg := AsyncSearcherConfig{DataDir: t.TempDir()}
	as := newTestAsync(t, cfg)

	// Interval that produces no results, but it must be counted as processed
	provider := &fakeFractionProvider{
		&fakeFrac{info: frac.Info{Path: "1", From: 0, To: seq.DurationToMID(defaultSearchInterval)}},
	}

	req := AsyncSearchRequest{
		ID: uuid.New().String(),
		Params: processor.SearchParams{
			Limit: 1000,
			From:  0,
			To:    seq.DurationToMID(defaultSearchInterval),
		},
		Query:     "*",
		Retention: time.Hour,
	}
	r.NoError(as.StartSearch(req, provider))
	as.processWg.Wait()

	resp, ok := as.FetchSearchResult(FetchSearchResultRequest{ID: req.ID, Limit: 1000, Order: seq.DocsOrderDesc})
	r.True(ok)
	r.Equal(AsyncSearchStatusDone, resp.Status)
	r.Equal(1, resp.IntervalsDone)
	r.Equal(0, resp.IntervalsInQueue)
}

// blockingFrac returns ctx.Err() from Search once the context is cancelled,
// simulating the in-flight search being interrupted by cancellation.
type blockingFrac struct {
	frac.Fraction
	info frac.Info
}

func (f *blockingFrac) Info() *frac.Info {
	return &f.info
}

func (f *blockingFrac) IsIntersecting(from, to seq.MID) bool {
	return true
}

func (f *blockingFrac) Search(ctx context.Context, _ processor.SearchParams) (*seq.QPR, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestAsyncSearchCancelDoesNotSetError(t *testing.T) {
	r := require.New(t)

	cfg := AsyncSearcherConfig{DataDir: t.TempDir()}
	as := newTestAsync(t, cfg)

	provider := &fakeFractionProvider{
		&blockingFrac{info: frac.Info{Path: "1", From: 0, To: seq.DurationToMID(defaultSearchInterval)}},
	}

	req := AsyncSearchRequest{
		ID: uuid.New().String(),
		Params: processor.SearchParams{
			Limit: 1000,
			From:  0,
			To:    seq.DurationToMID(defaultSearchInterval),
		},
		Query:     "*",
		Retention: time.Hour,
	}
	r.NoError(as.StartSearch(req, provider))

	as.CancelSearch(req.ID)
	as.processWg.Wait()

	resp, ok := as.FetchSearchResult(FetchSearchResultRequest{ID: req.ID, Limit: 1000, Order: seq.DocsOrderDesc})
	r.True(ok)
	r.Equal(AsyncSearchStatusCanceled, resp.Status)
	r.Equal("", resp.Error)
	r.Equal(0, resp.IntervalsDone)
	r.Equal(1, resp.IntervalsInQueue)
}

// rangedFakeFrac intersects with a range only if its info does.
type rangedFakeFrac struct {
	frac.Fraction
	info frac.Info
}

func (f *rangedFakeFrac) Info() *frac.Info {
	return &f.info
}

func (f *rangedFakeFrac) IsIntersecting(from, to seq.MID) bool {
	return f.info.IsIntersecting(from, to)
}

type rangedFakeFractionProvider struct {
	fracs fracmanager.List
}

func (fp *rangedFakeFractionProvider) AcquireFractionsInRange(from, to seq.MID) (fracmanager.List, func()) {
	res := make(fracmanager.List, 0)
	for _, f := range fp.fracs {
		if f.IsIntersecting(from, to) {
			res = append(res, f)
		}
	}
	return res, func() {}
}

func TestNarrowSearchInterval(t *testing.T) {
	interval := seq.DurationToMID(defaultSearchInterval)

	fracs := fracmanager.List{
		&rangedFakeFrac{info: frac.Info{Path: "1", From: interval, To: 2 * interval, DocsTotal: 1}},
		&rangedFakeFrac{info: frac.Info{Path: "2", From: 3 * interval, To: 4 * interval, DocsTotal: 1}},
	}

	tests := []struct {
		name     string
		from     seq.MID
		to       seq.MID
		expected []seq.MID // [SearchFrom, SearchTo]
	}{
		{
			name:     "request_inside_one_frac",
			from:     3 * interval / 2,
			to:       2 * interval,
			expected: []seq.MID{3 * interval / 2, 2 * interval},
		},
		{
			name:     "request_from_before_dataset_from",
			from:     0,
			to:       2 * interval,
			expected: []seq.MID{interval, 2 * interval},
		},
		{
			name:     "request_to_after_dataset_to",
			from:     3 * interval,
			to:       10 * interval,
			expected: []seq.MID{3 * interval, 4 * interval},
		},
		{
			name:     "request_wider_than_dataset",
			from:     0,
			to:       10 * interval,
			expected: []seq.MID{interval, 4 * interval},
		},
		{
			name:     "request_between_fracs_is_collapsed",
			from:     2*interval + 10,
			to:       3*interval - 10,
			expected: []seq.MID{3*interval - 10, 3*interval - 10},
		},
		{
			name:     "request_after_dataset_is_collapsed",
			from:     5 * interval,
			to:       6 * interval,
			expected: []seq.MID{6 * interval, 6 * interval},
		},
		{
			name:     "request_before_dataset_is_collapsed",
			from:     0,
			to:       10,
			expected: []seq.MID{10, 10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			as := AsyncSearcher{}
			from, to := as.narrowSearchInterval(tt.from, tt.to, &rangedFakeFractionProvider{fracs: fracs})
			r.Equal(tt.expected[0], from)
			r.Equal(tt.expected[1], to)
		})
	}
}

func newTestAsync(t *testing.T, cfg AsyncSearcherConfig) *AsyncSearcher {
	t.Helper()
	mp, err := mappingprovider.New("", mappingprovider.WithMapping(seq.Mapping{}))
	require.NoError(t, err)
	return newAsync(cfg, mp, nil)
}

func writeQPRFile(t *testing.T, dir, name string, qpr *seq.QPR) string {
	t.Helper()
	p := path.Join(dir, name)
	require.NoError(t, compressQPR(qpr, func(compressed []byte) error {
		return os.WriteFile(p, compressed, 0o666)
	}))
	return p
}

func addToQueue(as *AsyncSearcher, id string) {
	as.requestsMu.Lock()
	defer as.requestsMu.Unlock()
	as.requests[id] = asyncSearchInfo{
		Request:   AsyncSearchRequest{Retention: time.Hour},
		StartedAt: time.Now(),
	}
}

func removeFromQueue(as *AsyncSearcher, id string) asyncSearchInfo {
	as.requestsMu.Lock()
	defer as.requestsMu.Unlock()
	info := as.requests[id]
	delete(as.requests, id)
	return info
}

func assertNoMergedQPR(t *testing.T, as *AsyncSearcher, id string) {
	t.Helper()
	_, err := os.Stat(path.Join(as.config.DataDir, id+asyncSearchExtMergedQPR))
	require.True(t, os.IsNotExist(err), "merge must not write the merged result for this request")
}

// startTwoIntervalSearch starts a search producing results in two intervals,
// like the one TestMerge uses, and waits for it to finish.
func startTwoIntervalSearch(t *testing.T, as *AsyncSearcher) string {
	t.Helper()

	r := require.New(t)
	now := time.Now()

	frac1 := &fakeFrac{
		info: common.Info{Path: "1", From: seq.TimeToMID(now.Add(-time.Minute * 11)), To: seq.TimeToMID(now.Add(-time.Minute * 6))},
		dp:   fakeDP{qpr: seq.QPR{IDs: []seq.IDSource{{ID: seq.ID{MID: 1}}}, Total: 1}},
	}
	frac2 := &fakeFrac{
		info: common.Info{Path: "2", From: seq.TimeToMID(now.Add(-time.Minute * 6)), To: seq.TimeToMID(now.Add(-time.Minute * 1))},
		dp:   fakeDP{qpr: seq.QPR{IDs: []seq.IDSource{{ID: seq.ID{MID: 2}}}, Total: 1}},
	}
	provider := &fakeFractionProvider{frac1, frac2}

	req := AsyncSearchRequest{
		ID: uuid.New().String(),
		Params: processor.SearchParams{
			Limit: 1000,
			Order: seq.DocsOrderDesc,
			From:  seq.TimeToMID(now.UTC().Add(-time.Minute * 30).Truncate(time.Millisecond)),
			To:    seq.TimeToMID(now.UTC().Truncate(time.Millisecond)),
		},
		Query:     "*",
		Retention: time.Hour,
	}
	r.NoError(as.StartSearch(req, provider))
	as.processWg.Wait()

	return req.ID
}

func TestMergeSkipsErroredRequests(t *testing.T) {
	r := require.New(t)

	cfg := AsyncSearcherConfig{DataDir: t.TempDir()}
	as := newTestAsync(t, cfg)

	id := startTwoIntervalSearch(t, as)

	as.markSearchError(id, errors.New("some error"))

	as.merge()

	assertNoMergedQPR(t, as, id)
	info, ok := as.getSearchInfo(id)
	r.True(ok)
	r.False(info.merged.Load(), "merge must not mark errored requests as merged")
}

func TestMergeSkipsRemovedRequests(t *testing.T) {
	r := require.New(t)

	cfg := AsyncSearcherConfig{DataDir: t.TempDir()}
	as := newTestAsync(t, cfg)

	id := startTwoIntervalSearch(t, as)

	// simulate the request being removed concurrently by removeExpiredResults
	info := removeFromQueue(as, id)

	as.mergeQPRs(mergeJob{ID: id, Info: info})

	assertNoMergedQPR(t, as, id)
	r.False(info.merged.Load(), "mergeQPRs must not touch the state of a removed request")
	qprPaths, err := as.findQPRs(id)
	r.NoError(err)
	r.NotEmpty(qprPaths, "partial results must stay for fetch until retention removes them")
}

func TestMergeMemoryLimitMarksRequestError(t *testing.T) {
	r := require.New(t)

	cfg := AsyncSearcherConfig{DataDir: t.TempDir(), MaxQprMemory: 1}
	as := newTestAsync(t, cfg)

	id := startTwoIntervalSearch(t, as)

	as.merge()

	info, ok := as.getSearchInfo(id)
	r.True(ok)
	r.Contains(info.Error, consts.ErrMemoryLimitExceeded.Error())
	r.True(info.Finished, "the memory limit must terminate the request with an error")
	r.False(info.merged.Load(), "a failed merge must not mark the request as merged")

	assertNoMergedQPR(t, as, id)
	qprPaths, err := as.findQPRs(id)
	r.NoError(err)
	r.NotEmpty(qprPaths, "partial results must stay for fetch until retention removes them")
}

func TestLoadSearchResultMemoryLimit(t *testing.T) {
	r := require.New(t)

	cfg := AsyncSearcherConfig{DataDir: t.TempDir(), MaxQprMemory: 1}
	as := newTestAsync(t, cfg)

	qpr := seq.QPR{IDs: make([]seq.IDSource, 100), Total: 100}
	p := writeQPRFile(t, cfg.DataDir, "test.qpr", &qpr)

	const id = "00000000-0000-0000-0000-000000000000"
	addToQueue(as, id)

	_, _, err := as.loadSearchResult(id, []string{p}, 1000, seq.DocsOrderDesc, 0)
	r.ErrorIs(err, consts.ErrMemoryLimitExceeded)
}

func TestLoadSearchResultStopsOnRemovedRequest(t *testing.T) {
	r := require.New(t)

	cfg := AsyncSearcherConfig{DataDir: t.TempDir()}
	as := newTestAsync(t, cfg)

	qpr := seq.QPR{IDs: []seq.IDSource{{ID: seq.ID{MID: 1}}}, Total: 1}
	paths := make([]string, 0, 3)
	for i := range 3 {
		paths = append(paths, writeQPRFile(t, cfg.DataDir, fmt.Sprintf("test%d.qpr", i), &qpr))
	}

	const id = "00000000-0000-0000-0000-000000000001"
	addToQueue(as, id)

	_, _, err := as.loadSearchResult(id, paths, 1000, seq.DocsOrderDesc, 0)
	r.NoError(err)

	// simulate the request being removed concurrently by removeExpiredResults
	removeFromQueue(as, id)

	_, _, err = as.loadSearchResult(id, paths, 1000, seq.DocsOrderDesc, 0)
	r.ErrorIs(err, errRequestRemoved, "loading must abort on the first liveness check of a removed request")
}

func TestCancelWhileWaitingForSemaphore(t *testing.T) {
	r := require.New(t)

	cfg := AsyncSearcherConfig{DataDir: t.TempDir()}
	as := newTestAsync(t, cfg)

	// fill the semaphore so the search blocks on acquiring it
	for range cap(as.rateLimit) {
		as.rateLimit <- struct{}{}
	}

	provider := &fakeFractionProvider{
		&fakeFrac{info: common.Info{Path: "1", From: 0, To: seq.DurationToMID(defaultSearchInterval)}},
	}
	req := AsyncSearchRequest{
		ID: uuid.New().String(),
		Params: processor.SearchParams{
			Limit: 1000,
			From:  0,
			To:    seq.DurationToMID(defaultSearchInterval),
		},
		Query:     "*",
		Retention: time.Hour,
	}
	r.NoError(as.StartSearch(req, provider))

	as.CancelSearch(req.ID)
	as.processWg.Wait()

	resp, ok := as.FetchSearchResult(FetchSearchResultRequest{ID: req.ID, Limit: 1000, Order: seq.DocsOrderDesc})
	r.True(ok)
	r.Equal(AsyncSearchStatusCanceled, resp.Status)
	r.Equal("", resp.Error, "cancellation must not be recorded as an error")
}

func TestNotProcessedTasksOrder(t *testing.T) {
	r := require.New(t)

	base := time.Now()
	tasks := map[string]asyncSearchInfo{
		"new":  {StartedAt: base.Add(time.Minute)},
		"old":  {StartedAt: base.Add(-time.Minute)},
		"done": {StartedAt: base.Add(-2 * time.Minute), Finished: true},
	}

	r.Equal([]string{"old", "new"}, notProcessedTasks(tasks))
}

func TestCancelAndDeleteOfFinishedRequests(t *testing.T) {
	r := require.New(t)

	cfg := AsyncSearcherConfig{DataDir: t.TempDir()}
	as := newTestAsync(t, cfg)

	id1 := startTwoIntervalSearch(t, as)
	id2 := startTwoIntervalSearch(t, as)

	as.CancelSearch(id1)
	info, ok := as.getSearchInfo(id1)
	r.True(ok, "CancelSearch must not remove the request from the queue")
	r.Equal(AsyncSearchStatusDone, info.Status(), "CancelSearch on a finished request must not change the status")
	r.True(info.CanceledAt.IsZero(), "CancelSearch must not stamp CanceledAt on a finished request")

	as.DeleteSearch(id2)
	info, ok = as.getSearchInfo(id2)
	r.True(ok, "DeleteSearch removes the request only on the next maintenance tick")
	r.Equal(AsyncSearchStatusDone, info.Status(), "DeleteSearch on a finished request must not change the status")
	r.True(info.Expired(), "DeleteSearch expires the request by setting Retention = 0")
	r.True(info.CanceledAt.IsZero(), "DeleteSearch must not stamp CanceledAt on a finished request")
}
