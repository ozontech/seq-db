package integration_tests

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tests/setup"
	"github.com/ozontech/seq-db/tests/suites"
)

type OffloadingSuite struct {
	suites.Base
}

func TestOffloading(t *testing.T) {
	suite.Run(t, &OffloadingSuite{Base: *suites.NewBase(&setup.TestingEnvConfig{
		Name: "Offloading",

		IngestorCount:  1,
		HotShards:      1,
		HotFactor:      1,
		HotModeEnabled: true,
		IndexAllFields: true,

		FracManagerConfig: &fracmanager.Config{
			// Following values were chosen empirically.
			// We need to have small enough total size to trigger fractions offloading.
			FracSize:  uint64(units.MiB),
			TotalSize: 8 * uint64(units.MiB),

			Fraction: frac.Config{
				SkipSortDocs: true,
			},

			OffloadingEnabled: true,
			OffloadingForced:  true,
		},
	})})
}

func (s *OffloadingSuite) TestSearch() {
	// TODO(dkharms): TestSearch provides very basic test of search functionality.
	// We need somehow reuse all integration test that were written to test offloading.

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	from, to, allDocs := s.bulk(env)
	env.WaitIdle()
	env.OffloadAll()
	s.waitForOffloading(env)

	_, found, _, err := env.Search(
		"service:*", len(allDocs),
		setup.WithTimeRange(from, to),
		setup.WithOrder(seq.DocsOrderAsc),
	)
	s.Require().NoError(err)

	s.Require().Equal(len(allDocs), len(found))
	for i := range allDocs {
		s.Assert().Equal(allDocs[i], string(found[i]))
	}

	_, found, _, err = env.Search(
		"complexity:0", len(allDocs),
		setup.WithTimeRange(from, to),
		setup.WithOrder(seq.DocsOrderAsc),
	)
	s.Require().NoError(err)

	s.Require().Empty(found)

	_, found, _, err = env.Search(
		"service:yet-another-microservice-1*", len(allDocs),
		setup.WithTimeRange(from, to),
		setup.WithOrder(seq.DocsOrderAsc),
	)
	s.Require().NoError(err)

	var k int
	for i := range allDocs {
		if !strings.Contains(allDocs[i], "yet-another-microservice-1") {
			continue
		}
		s.Assert().Equal(allDocs[i], string(found[k]))
		k += 1
	}
}

func (s *OffloadingSuite) TestAggregations() {
	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	from, to, allDocs := s.bulk(env)
	env.WaitIdle()
	env.OffloadAll()
	s.waitForOffloading(env)

	qpr, _, _, err := env.Search(
		"", len(allDocs),
		setup.NoFetch(),
		setup.WithTimeRange(from, to.Add(time.Hour*24)),
		setup.WithOrder(seq.DocsOrderAsc),
		setup.WithAggQuery(search.AggQuery{
			Field: "service",
			Func:  seq.AggFuncCount,
		}),
	)
	s.Require().NoError(err)

	s.Assert().Equal(len(allDocs), len(qpr.Aggs[0].SamplesByBin))
	for _, samples := range qpr.Aggs[0].SamplesByBin {
		s.Assert().Equal(int64(1), samples.Total)
	}
}

func (s *OffloadingSuite) bulk(env *setup.TestingEnv) (time.Time, time.Time, []string) {
	tpl := `{ "service": "yet-another-microservice-%d", "complexity": 10, "timestamp": "%s" }`

	var (
		allDocs   []string
		from      = time.Now().Add(time.Second * 10)
		to        = from
		batchSize = 1024
		batches   = 1000
		counter   int
	)

	for range batches {
		var docs []string

		for range batchSize {
			docs = append(docs, fmt.Sprintf(tpl, counter, to.Format(time.RFC3339Nano)))
			to = to.Add(time.Millisecond)
			counter++
		}

		setup.Bulk(s.T(), env.IngestorBulkAddr(), docs)
		allDocs = append(allDocs, docs...)
	}

	return from, to, allDocs
}

func (s *OffloadingSuite) waitForOffloading(env *setup.TestingEnv) {
	// TODO(dkharms): I am ashamed of this workaround but I need somehow find a way to be sure
	// that all sealings are done and offloadings are finished.
	//
	// Integration tests for such cases are not really convenient because we need invasive tools
	// to accomplish these goals (it will just pollute code with some weird conditions).
	//
	// So it will be fixed later.
	s.Require().EventuallyWithTf(func(collect *assert.CollectT) {
		p := filepath.Join(env.Store(true).Config.FracManager.DataDir, "*"+consts.RemoteFractionSuffix)

		m, err := filepath.Glob(p)
		s.Require().NoError(err)

		s.Require().True(len(m) > 0)
	}, 30*time.Second, time.Second, "fraction were not offloaded")
}
