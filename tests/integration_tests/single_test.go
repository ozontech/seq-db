package integration_tests

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
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
	"github.com/ozontech/seq-db/tests/common"
	"github.com/ozontech/seq-db/tests/setup"
	"github.com/ozontech/seq-db/tests/suites"
)

const (
	good = "good"
	bad  = "bad"
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

func (s *SingleTestSuite) TestBasicSearch() {
	startTS := time.Now()
	docs := simpleCases(startTS)
	docStrs := setup.DocsToStrings(docs)
	// order of docs is "2, 1, 3, 0"
	// first: order is reversed
	// second: doc #3 has smaller timestamp, than #1 and #2,
	// so it will be reordered
	s.Bulk(docStrs)

	s.assertSearch(docStrs)
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
	s.assertSearch(docStrs)
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

func (s *SingleTestSuite) TestSearchNestedIndexOneFraction() {
	const numDocs = 100
	const doc = `{"trace_id": "1", "spans": [{"span_id": "1"}, {"span_id": "2"}]}`

	docs := []string{}
	for range numDocs {
		docs = append(docs, doc)
		s.Bulk([]string{doc})
	}

	assertSearch := func(q string, size int) {
		s.Assert().Equal(size, len(s.SearchDocs(q, size, seq.DocsOrderAsc)))
		s.Assert().Equal(docs[:size], s.SearchDocs(q, size, seq.DocsOrderAsc))
	}

	assertSearch(`spans.span_id:*`, 1)
	assertSearch(`spans.span_id:*`, 5)
	assertSearch(`spans.span_id:*`, numDocs)
	assertSearch(`trace_id:*`, numDocs)

	s.Assert().Equal(0, len(s.SearchDocs(`spans.span_id:*`, 0, seq.DocsOrderAsc)))
	s.Assert().Equal(numDocs, len(s.SearchDocs(`spans.span_id:*`, numDocs+1, seq.DocsOrderAsc)))
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

func (s *SingleTestSuite) TestSearchNot() {
	docs := setup.GenerateDocs(6, func(i int, doc *setup.ExampleDoc) {
		doc.Message = good
		if i%2 == 0 {
			doc.Message = bad
		}
		doc.Level = i + 1 // zero will not write
		doc.Service = fmt.Sprintf("srv_%d", i+1)
	})
	docStrs := setup.DocsToStrings(docs)
	s.Bulk(docStrs)

	tests := []struct {
		query   string
		indexes []int
	}{
		{`NOT level:1`, []int{5, 4, 3, 2, 1}},
		{`NOT level:2`, []int{5, 4, 3, 2, 0}},
		{`NOT level:5`, []int{5, 3, 2, 1, 0}},
		{`NOT level:6`, []int{4, 3, 2, 1, 0}},
		{`NOT message:notfound`, []int{5, 4, 3, 2, 1, 0}},
		{`NOT service:srv_*`, []int{}},
		{`NOT message:bad`, []int{5, 3, 1}},
		{`NOT message:good`, []int{4, 2, 0}},
		{`NOT message:"good bad"`, []int{5, 4, 3, 2, 1, 0}},
		{`NOT (message:good AND message:bad)`, []int{5, 4, 3, 2, 1, 0}},
		{`NOT (message:good OR message:bad)`, []int{}},
		{`NOT message:bad AND message:bad`, []int{}},
		{`NOT message:bad AND message:good`, []int{5, 3, 1}},
		{`message:good AND NOT message:good`, []int{}},
		{`message:bad AND NOT message:good`, []int{4, 2, 0}},
	}

	s.RunFracEnvs(suites.AllFracEnvs, true, func() {
		for _, test := range tests {
			s.AssertSearch(test.query, docStrs, test.indexes)
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

func (s *SingleTestSuite) TestSearchFromTo() {
	docs := setup.GenerateDocs(8, func(i int, doc *setup.ExampleDoc) {
		doc.Message = good
		if i%2 == 0 {
			doc.Message = bad
		}
		doc.Level = i + 1 // zero will not write
		doc.TraceID = fmt.Sprintf("%d", i/3)
		doc.Service = fmt.Sprintf("%d", i%3)
	})
	start := docs[0].Timestamp
	docStrs := setup.DocsToStrings(docs)
	s.Bulk(docStrs)

	assertSearch := func(query string, from int, to int, indexes []int) {
		fromMID := seq.TimeToMID(start.Add(time.Millisecond * time.Duration(from)))
		toMID := seq.TimeToMID(start.Add(time.Millisecond * time.Duration(to)))

		for _, withTotal := range []bool{true, false} {
			for _, o := range []seq.DocsOrder{seq.DocsOrderAsc, seq.DocsOrderDesc} {
				_, docsStream, _, err := s.Ingestor().SearchIngestor.Search(
					context.Background(),
					&search.SearchRequest{
						Explain:     false,
						Q:           []byte(query),
						Offset:      0,
						Size:        math.MaxUint32,
						Interval:    0,
						From:        fromMID,
						To:          toMID,
						WithTotal:   withTotal,
						ShouldFetch: true,
						Order:       o,
					},
					nil,
				)
				s.Require().NoError(err)
				foundDocs := common.ToStringSlice(search.ReadAll(docsStream))
				if o.IsReverse() {
					slices.Reverse(foundDocs)
				}
				s.AssertDocsEqual(docStrs, indexes, foundDocs)
			}
		}
	}

	s.RunFracEnvs(suites.AllFracEnvs, true, func() {
		assertSearch(`message:good`, 0, 7, []int{7, 5, 3, 1})
		assertSearch(`message:bad`, 0, 7, []int{6, 4, 2, 0})
		assertSearch(`message:good`, 0, 6, []int{5, 3, 1})
		assertSearch(`message:bad`, 1, 7, []int{6, 4, 2})

		assertSearch(`message:good OR message:bad`, 2, 6, []int{6, 5, 4, 3, 2})
		assertSearch(`message:good OR message:bad`, 3, 3, []int{3})
		assertSearch(`NOT message:notexists`, 0, 7, []int{7, 6, 5, 4, 3, 2, 1, 0})
		assertSearch(`NOT message:notexists`, 0, 6, []int{6, 5, 4, 3, 2, 1, 0})

		assertSearch(`NOT message:notexists`, 1, 7, []int{7, 6, 5, 4, 3, 2, 1})
		assertSearch(`NOT message:notexists`, 1, 6, []int{6, 5, 4, 3, 2, 1})
		assertSearch(`NOT message:notexists AND message:*`, 1, 6, []int{6, 5, 4, 3, 2, 1})
		assertSearch(`NOT message:notexists AND (message:* OR message:*)`, 1, 6, []int{6, 5, 4, 3, 2, 1})

		assertSearch(`NOT message:notexists AND (message:good OR message:bad)`, 1, 6, []int{6, 5, 4, 3, 2, 1})
		assertSearch(`NOT message:notexists AND message:good`, 1, 6, []int{5, 3, 1})
		assertSearch(`NOT (message:good OR message:bad)`, 0, 7, []int{})
		assertSearch(`NOT (message:good OR message:bad)`, 1, 6, []int{})

		assertSearch(`NOT traceID:0`, 0, 2, []int{})
		assertSearch(`NOT traceID:0`, 0, 3, []int{3})
		assertSearch(`NOT traceID:1`, 3, 5, []int{})
		assertSearch(`NOT traceID:1`, 2, 6, []int{6, 2})
		assertSearch(`NOT traceID:0 AND NOT traceID:2`, 0, 10, []int{5, 4, 3})
		assertSearch(`NOT traceID:0 AND NOT traceID:2`, 3, 5, []int{5, 4, 3})
	})
}

func (s *SingleTestSuite) TestFetch() {
	n := 8
	docs := setup.GenerateDocs(n, func(i int, doc *setup.ExampleDoc) {
		doc.Message = good
		if i%2 == 0 {
			doc.Message = bad
		}
		doc.Level = i + 1 // zero will not write
		doc.TraceID = fmt.Sprintf("%d", i/3)
		doc.Service = fmt.Sprintf("%d", i%3)
	})
	docStrs := setup.DocsToStrings(docs)
	s.Bulk(docStrs)

	qpr, _, _, err := s.Env.Search("_all_:*", math.MaxInt32, setup.WithTotal(false), setup.NoFetch())
	s.Assert().NoError(err)
	s.Assert().Equal(n, len(qpr.IDs))

	ids := qpr.IDs.IDs()
	s.RunFracEnvs(suites.AllFracEnvs, true, func() {
		docs, err := s.Env.Fetch(ids)
		s.Assert().NoError(err)
		s.Assert().Equal(len(ids), len(docs))
	})
}

func (s *SingleTestSuite) TestWildcardSymbols() {
	startTS := time.Now()
	docs := []setup.ExampleDoc{
		{
			Message:   "first value:****",
			Timestamp: startTS.Add(time.Millisecond * 10),
		},
		{
			Message:   "second value:*******",
			Timestamp: startTS.Add(time.Millisecond * 20),
		},
		{
			Message:   "third value****",
			Timestamp: startTS.Add(time.Millisecond * 30),
		},
		{
			Message:   "fourth ****",
			Timestamp: startTS.Add(time.Millisecond * 40),
		},
	}
	docStrs := setup.DocsToStrings(docs)
	s.Bulk(docStrs)

	tests := []struct {
		query   string
		indexes []int
	}{
		{`message:*`, []int{3, 2, 1, 0}},
		{`message:value`, []int{1, 0}},
		{`message:value*`, []int{2, 1, 0}},
		{`message:"value\*"`, []int{}},
		{`message:"value\**"`, []int{2}},
		{`message:"*\**"`, []int{3, 2, 1, 0}},
		{`message:"*e\**"`, []int{2}},
		{`message:"\**"`, []int{3, 1, 0}},
		{`message:"\*\*\*\*"`, []int{3, 0}},
		{`message:"\*\*\*\**"`, []int{3, 1, 0}},
		{`message:value* AND message:"\*\**"`, []int{1, 0}},
		{`message:value* OR message:"\*\**"`, []int{3, 2, 1, 0}},
	}

	s.RunFracEnvs(suites.AllFracEnvs, true, func() {
		for _, test := range tests {
			s.AssertSearch(test.query, docStrs, test.indexes)
		}
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
