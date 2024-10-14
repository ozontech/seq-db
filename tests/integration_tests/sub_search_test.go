package integration_tests

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tests/setup"
)

type timeSlice []time.Time

func (s timeSlice) Less(i, j int) bool { return s[i].Before(s[j]) }
func (s timeSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s timeSlice) Len() int           { return len(s) }

func (s *IntegrationTestSuite) ingestData(env *setup.TestingEnv, from, to time.Time, fractionRange time.Duration, docsPerFraction int) []time.Time {
	fractionFrom := from
	fractionTo := from.Add(fractionRange * 2)

	docsTimes := make([]time.Time, 0)
	origDocs := make([]string, 0, docsPerFraction)

	for {
		if fractionTo.After(to) {
			fractionTo = to
		}

		step := fractionTo.Sub(fractionFrom) / time.Duration(docsPerFraction)

		for i, ts := 0, fractionFrom; i < docsPerFraction; i++ {
			strTs := ts.Format(time.RFC3339)
			origDocs = append(origDocs, fmt.Sprintf(`{"service":"x%d", "ts":%q}`, i, strTs))
			ts, _ = time.Parse(time.RFC3339, strTs)
			docsTimes = append(docsTimes, ts)
			ts.Add(step)
		}

		setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)
		origDocs = origDocs[:0]

		env.WaitIdle()
		env.SealAll()

		if fractionTo.Equal(to) {
			break
		}
		fractionFrom = fractionFrom.Add(fractionRange)
		fractionTo = fractionTo.Add(fractionRange)
	}

	sort.Sort(timeSlice(docsTimes))

	return docsTimes
}

func fetchFromDocsTimes(from, to time.Time, docsTimes []time.Time) []time.Time {
	i1 := sort.Search(len(docsTimes), func(i int) bool {
		return docsTimes[i].After(from) || docsTimes[i].Equal(from)
	})

	i2 := sort.Search(len(docsTimes), func(i int) bool {
		return docsTimes[i].After(to)
	})

	return docsTimes[i1:i2]
}

func makeHist(data []time.Time, interval time.Duration) map[seq.MID]uint64 {
	r := make(map[seq.MID]uint64)
	mid := seq.MID(interval / time.Millisecond)
	for _, ts := range data {
		t := ts.UnixMilli()
		t -= t % int64(mid)
		r[seq.MID(t)]++
	}
	return r
}

// TestSubSearch reproduces search on many fractions using sub-search feature
func (s *IntegrationTestSuite) TestSubSearch() {
	if s.Config.Name != "Basic" {
		s.T().Skip("no need to run in", s.Config.Name, "env")
	}

	rand.Seed(time.Now().UnixNano())

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	timeRange := 23 * time.Hour
	to := time.Now()
	from := to.Add(-timeRange)

	docsTimes := s.ingestData(env, from, to, 5*time.Minute, 100)

	attempts := 500
	limit := 200
	now := time.Now()

	maxOffset := int64(timeRange.Seconds())

	for i := 0; i < attempts; i++ {
		offsetSecond := rand.Int63n(maxOffset)
		f := from.Add(time.Second * time.Duration(offsetSecond))
		t := f.Add(12 * time.Hour)

		sub := fetchFromDocsTimes(f, t, docsTimes)
		expectedTotal := len(sub)
		expectedCount := limit
		if expectedTotal < limit {
			expectedCount = expectedTotal
		}

		qpr, _, _, err := env.Search("service:*", limit, setup.NoFetch(), setup.WithTotal(false), setup.WithTimeRange(f, t))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), expectedCount, len(qpr.IDs), "wrong doc count in range [%s, %s]", f, t)
	}
	s.T().Log("With Total False:", time.Since(now).Milliseconds())

	now = time.Now()
	for i := 0; i < attempts; i++ {
		offsetSecond := rand.Int63n(maxOffset)
		f := from.Add(time.Second * time.Duration(offsetSecond))
		t := f.Add(12 * time.Hour)

		sub := fetchFromDocsTimes(f, t, docsTimes)
		expectedTotal := len(sub)
		expectedCount := limit
		if expectedTotal < limit {
			expectedCount = expectedTotal
		}

		qpr, _, _, err := env.Search("service:*", limit, setup.NoFetch(), setup.WithTotal(true), setup.WithTimeRange(f, t))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), expectedCount, len(qpr.IDs), "wrong doc count in range [%s, %s]", f, t)
		assert.Equal(s.T(), expectedTotal, int(qpr.Total), "wrong doc count in range [%s, %s]", f, t)
	}
	s.T().Log("With Total True:", time.Since(now).Milliseconds())

	now = time.Now()
	for i := 0; i < attempts; i++ {
		offsetSecond := rand.Int63n(maxOffset)
		f := from.Add(time.Second * time.Duration(offsetSecond))
		t := f.Add(12 * time.Hour)

		sub := fetchFromDocsTimes(f, t, docsTimes)
		expectedTotal := len(sub)
		expectedCount := limit
		if expectedTotal < limit {
			expectedCount = expectedTotal
		}

		interval := 3 * time.Minute
		qpr, _, _, err := env.Search("service:*", limit, setup.NoFetch(), setup.WithTotal(false), setup.WithInterval(interval), setup.WithTimeRange(f, t))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), expectedCount, len(qpr.IDs), "wrong doc count in range [%s, %s]", f, t)
		assert.Equal(s.T(), makeHist(sub, interval), qpr.Histogram, "wrong doc count in range [%s, %s]", f, t)
	}
	s.T().Log("With Histogram:", time.Since(now).Milliseconds())
}
