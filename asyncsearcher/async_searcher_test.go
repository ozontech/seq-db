package asyncsearcher

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/mappingprovider"
	"github.com/ozontech/seq-db/seq"
)

type fakeFrac struct {
	frac.Fraction
	info common.Info
	dp   fakeDP
}

func (f *fakeFrac) Info() *common.Info {
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
	mp, err := mappingprovider.New("", mappingprovider.WithMapping(seq.Mapping{}))
	r.NoError(err)

	as := MustStartAsync(cfg, mp, nil)

	req := AsyncSearchRequest{
		ID:        uuid.New().String(),
		Params:    processor.SearchParams{},
		Query:     "*",
		Retention: time.Hour,
	}

	fracs := fakeFractionProvider{
		&fakeFrac{info: common.Info{Path: "1"}},
	}
	r.NoError(as.StartSearch(req, fracs))

	as.processWg.Wait()
}

func TestMerge(t *testing.T) {
	r := require.New(t)
	now := time.Now()

	cfg := AsyncSearcherConfig{DataDir: t.TempDir()}
	mp, err := mappingprovider.New("", mappingprovider.WithMapping(seq.Mapping{}))
	r.NoError(err)

	as := MustStartAsync(cfg, mp, nil)
	t.Cleanup(func() { as.readOnly.Store(false) })

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

	as.merge()

	resp, ok := as.FetchSearchResult(FetchSearchResultRequest{ID: req.ID, Limit: 1000, Order: seq.DocsOrderDesc})
	r.True(ok)
	r.Equal(AsyncSearchStatusDone, resp.Status)
	r.Len(resp.QPR.IDs, 2)
	r.Equal(seq.MID(2), resp.QPR.IDs[0].ID.MID)
	r.Equal(seq.MID(1), resp.QPR.IDs[1].ID.MID)
}

func TestBuildIntervals(t *testing.T) {
	tests := []struct {
		name     string
		from     seq.MID
		to       seq.MID
		expected []searchInterval
	}{
		{
			name:     "empty_range_from_equals_to",
			from:     100,
			to:       100,
			expected: nil,
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
		})
	}
}
