package integration_tests

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/proxy/stores"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tests/setup"
	"github.com/ozontech/seq-db/tests/suites"
)

type SingleTestSuite struct {
	suites.Single
}

func NewSingleTestSuite(cfg *setup.TestingEnvConfig) *SingleTestSuite {
	return &SingleTestSuite{
		Single: *suites.NewSingle(cfg),
	}
}

func simpleCases(startTS time.Time) []setup.ExampleDoc {
	docs := []setup.ExampleDoc{
		{
			Service:   "service_a",
			Message:   "first message some text",
			TraceID:   "abcdef",
			Source:    "prod01",
			Level:     1,
			Timestamp: startTS.Add(time.Millisecond * 10),
		},
		{
			Service:   "service_b",
			Message:   "second message other text",
			TraceID:   "abcdef",
			Source:    "prod01",
			Level:     1,
			Timestamp: startTS.Add(time.Millisecond * 30),
		},
		{
			Service:   "service_c",
			Message:   "third message other text",
			TraceID:   "aaaaaa",
			Source:    "prod02",
			Level:     2,
			Timestamp: startTS.Add(time.Millisecond * 40),
		},
		{
			Service:   "service_a",
			Message:   "fourth message some text",
			TraceID:   "bbbbbb",
			Source:    "prod01",
			Level:     1,
			Timestamp: startTS.Add(time.Millisecond * 20),
		},
	}
	return docs
}

func (s *SingleTestSuite) TestBasicSearchHotRead() {
	startTS := time.Now()
	docs := simpleCases(startTS)
	docStrs := setup.DocsToStrings(docs)
	// order of docs is "2, 1, 3, 0"
	// first: order is reversed
	// second: doc #3 has smaller timestamp, than #1 and #2,
	// so it will be reordered
	s.Bulk(docStrs)

	// query hot read stores
	s.Ingestor().Config.Search.HotReadStores = s.Ingestor().Config.Search.HotStores
	s.Ingestor().Config.Search.HotStores = &stores.Stores{
		Shards: [][]string{},
		Vers:   []string{},
	}
}

func (s *SingleTestSuite) TestSearchAgg() {
	startTS := time.Now()
	docs := simpleCases(startTS)
	docStrs := setup.DocsToStrings(docs)
	s.Bulk(docStrs)

	assertAgg := func(query string, aggQ []any, expected []map[string]uint64) {
		r := s.Require()
		qpr, _, _, err := s.Env.Search(query, math.MaxInt32, setup.WithAggQuery(aggQ...), setup.WithTotal(false))
		r.NoError(err)
		r.Equal(len(expected), len(qpr.Aggs))
		for i := range expected {
			for bin, hist := range qpr.Aggs[i].SamplesByBin {
				r.Equalf(int64(expected[i][bin.Token]), hist.Total, "failed for token %s", bin)
			}
		}
	}
	s.RunFracEnvs(suites.AllFracEnvs, true, func() {
		assertAgg("message:message", []any{"service"}, []map[string]uint64{
			{"service_a": 2, "service_b": 1, "service_c": 1},
		})
		assertAgg("message:message", []any{"level"}, []map[string]uint64{
			{"1": 3, "2": 1},
		})
		assertAgg("message:message", []any{"service", "level"},
			[]map[string]uint64{
				{"service_a": 2, "service_b": 1, "service_c": 1},
				{"1": 3, "2": 1},
			})
	})
}

func (s *SingleTestSuite) assertSearch(docStrs []string) {
	tests := []struct {
		query   string
		indexes []int
	}{
		{`service: service_a`, []int{3, 0}},
		{`traceID:abcdef`, []int{1, 0}},
		{`level: 1`, []int{1, 3, 0}},
		{`message: "message text"`, []int{2, 1, 3, 0}},
		{`message: "other text"`, []int{2, 1}},
		{`traceID: abcd*`, []int{1, 0}},
		{`traceID: a*`, []int{2, 1, 0}},
		{`traceID: a*f`, []int{1, 0}},
		{`traceID: a*a`, []int{2}},
		{`service: service*a`, []int{3, 0}},
		{`message: "message\ som*"`, []int{3, 0}},
	}

	s.RunFracEnvs(suites.AllFracEnvs, true, func() {
		for _, test := range tests {
			s.AssertSearch(test.query, docStrs, test.indexes)
		}
		// test limit
		s.AssertDocsEqual(docStrs, []int{2, 1}, s.SearchDocs(`message:other`, 2, seq.DocsOrderAsc))
		s.AssertDocsEqual(docStrs, []int{2, 1}, s.SearchDocs(`message:other`, 2, seq.DocsOrderDesc))
	})
}

// Test AND tree (sorting issue)
func (s *SingleTestSuite) TestSearchNestedWithAND() {
	const (
		numSpans  = 10
		numTraces = 50000 // here it is important to have a large enough number of documents to reproduce the sorting problem
		span      = `{"span_id": "%d"}`
		doc       = `{"timestamp":%q, "trace_id": "%d", "spans": [%s]}`
	)
	docs := make([]string, 0, numTraces)
	getNextTs := getAutoTsGenerator(time.Now(), time.Second)
	for i := range numTraces {
		spans := make([]string, 0, numSpans)
		for j := range numSpans {
			spans = append(spans, fmt.Sprintf(span, j))
		}
		docs = append(docs, fmt.Sprintf(doc, getNextTs(), i, strings.Join(spans, ", ")))
	}

	tmp := docs
	bulkSize := len(docs) / 5
	for len(tmp) > 0 {
		l := min(bulkSize, len(tmp))
		chunk := tmp[:l]
		tmp = tmp[l:]
		s.Bulk(chunk)
	}

	s.RunFracEnvs(suites.AllFracEnvs, true, func() {
		for i := 0; i < 20; i++ {
			traceID := rand.IntN(numTraces)
			spanID := rand.IntN(numSpans)
			q := fmt.Sprintf("trace_id:%d AND spans.span_id:%d", traceID, spanID)
			s.Assert().Equal(docs[traceID:traceID+1], s.SearchDocs(q, 10, seq.DocsOrderDesc), q)
		}
	})
}

type ExampleDocSorting struct {
	sample  []setup.ExampleDoc
	docStrs []string
}

func (ds *ExampleDocSorting) Len() int {
	return len(ds.sample)
}

func (ds *ExampleDocSorting) Less(i, j int) bool {
	return ds.sample[i].Timestamp.After(ds.sample[j].Timestamp)
}

func (ds *ExampleDocSorting) Swap(i, j int) {
	ds.sample[i], ds.sample[j] = ds.sample[j], ds.sample[i]
	ds.docStrs[i], ds.docStrs[j] = ds.docStrs[j], ds.docStrs[i]
}

func (s *SingleTestSuite) TestFetchHints() {
	docsSample := simpleCases(time.Now())
	docStrs := setup.DocsToStrings(docsSample)
	s.Bulk(docStrs)

	sort.Sort(&ExampleDocSorting{sample: docsSample, docStrs: docStrs})

	qpr, _, _, err := s.Env.Search("_all_:*", math.MaxInt32, setup.WithTotal(true), setup.NoFetch())
	s.Assert().NoError(err)
	origIDs := qpr.IDs

	s.RunFracEnvs(suites.AllFracEnvs, true, func() {
		ids := make(seq.IDSources, len(origIDs))
		copy(ids, origIDs)

		docsStreamWithHints, err := s.Ingestor().SearchIngestor.FetchDocsStream(context.TODO(), ids, false, search.FetchFieldsFilter{})
		s.Require().NoError(err)

		var fetched []string
		for doc, err := docsStreamWithHints.Next(); err == nil; doc, err = docsStreamWithHints.Next() {
			fetched = append(fetched, string(doc.Data))
		}
		s.Require().Equal(docStrs, fetched)

		// no hints:
		for i := range ids {
			ids[i].Hint = ""
		}

		docsStreamNoHints, err := s.Ingestor().SearchIngestor.FetchDocsStream(context.TODO(), ids, false, search.FetchFieldsFilter{})
		s.Require().NoError(err)

		fetched = []string{}
		for doc, err := docsStreamNoHints.Next(); err == nil; doc, err = docsStreamNoHints.Next() {
			fetched = append(fetched, string(doc.Data))
		}
		s.Require().Equal(docStrs, fetched)

		// break hints:
		for i := range ids {
			ids[i].Hint = "foobar"
		}

		docsStreamBrokenHints, err := s.Ingestor().SearchIngestor.FetchDocsStream(context.TODO(), ids, false, search.FetchFieldsFilter{})
		s.Assert().NoError(err)

		fetched = []string{}
		for doc, err := docsStreamBrokenHints.Next(); err == nil; doc, err = docsStreamBrokenHints.Next() {
			if !doc.Empty() {
				fetched = append(fetched, string(doc.Data))
			}
		}
		s.Assert().Empty(fetched)
	})
}

func (s *SingleTestSuite) TestIndexingAllFields() {
	defer func(m seq.Mapping, enabled bool) {
		s.Config.Mapping = m
		s.Config.IndexAllFields = enabled
	}(s.Config.Mapping, s.Config.IndexAllFields)

	// Reset mappings and explicitly set all fields indexing option.
	// We need to restart both store and ingestor to apply new config.
	s.Config.Mapping = nil
	s.Config.IndexAllFields = true
	s.Restart()

	var (
		docsCount = 5
		now       = time.Now()
		docs      []setup.ExampleDoc
	)

	for i := 1; i < docsCount+1; i++ {
		now = now.Add(time.Second)
		docs = append(docs, setup.ExampleDoc{
			Service:   fmt.Sprintf("service-%d", i),
			Message:   fmt.Sprintf("I am tired of repeating the same message for the %d-th time!", i),
			Level:     4130134,
			Timestamp: now,
		})
	}

	docStrs := setup.DocsToStrings(docs)
	// Just make sure that mapping is not overridden by something.
	require.Empty(s.T(), s.Ingestor().Config.Bulk.MappingProvider.GetMapping(), "mapping is not empty")

	s.Bulk(docStrs)
	tests := []struct {
		query   string
		indexes []int
	}{
		{`service:"service-1"`, []int{0}},
		{`service:"service-*"`, []int{4, 3, 2, 1, 0}},
		{`level:"4130134"`, []int{4, 3, 2, 1, 0}},
		{`unknown:"foobarbaz"`, nil},
	}

	s.RunFracEnvs(suites.AllFracEnvs, true, func() {
		for _, test := range tests {
			s.AssertSearch(test.query, docStrs, test.indexes)
		}
	})
}

func (s *SingleTestSuite) TestSealedMultiFetch() {
	docs := make([]setup.ExampleDoc, 0, consts.IDsPerBlock*2)
	nextTs := getAutoTimeGenerator(time.Now(), time.Millisecond*10)

	// this docs LID will be in the first Params Block; index = 0
	docs = append(docs, setup.ExampleDoc{Service: "service_a", Timestamp: nextTs()})
	// fill first block with service_b
	for len(docs) < consts.IDsPerBlock {
		docs = append(docs, setup.ExampleDoc{Service: "service_b", Timestamp: nextTs()})
	}
	// this docs LID will be in the second Params Block; index = len(docs) - 1
	docs = append(docs, setup.ExampleDoc{Service: "service_a", Timestamp: nextTs()})

	docStrs := setup.DocsToStrings(docs)

	s.Bulk(docStrs)

	indexes := []int{
		len(docs) - 1, // last service_a
		0,             // first service_a
	}
	sealedOnly := map[suites.FractionEnv]bool{suites.SealedEnv: true}
	s.RunFracEnvs(sealedOnly, true, func() {
		s.AssertSearch(`service:service_a`, docStrs, indexes)
	})
}

func TestSingleSuite(t *testing.T) {
	for _, cfg := range suites.SingleEnvs() {
		t.Run(cfg.Name, func(t *testing.T) {
			t.Parallel()
			suite.Run(t, NewSingleTestSuite(cfg))
		})
	}
}
