package frac

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/sealing"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/tokenizer"
	"github.com/stretchr/testify/suite"
)

type FractionTestSuite struct {
	suite.Suite
	tmpDir      string
	sortCache   *cache.Cache[[]byte]
	indexCache  *IndexCache
	readLimiter *storage.ReadLimiter
	config      *Config
	mapping     seq.Mapping

	fraction Fraction

	insertDocuments func(docs ...string)
}

func (s *FractionTestSuite) SetupSuite() {
	s.config = &Config{
		Search: SearchConfig{
			AggLimits: AggLimits{
				MaxFieldTokens:     1000,
				MaxGroupTokens:     1000,
				MaxTIDsPerFraction: 1000,
			},
		},
		SkipSortDocs: true, // TODO enabling sorting will fail tests
		KeepMetaFile: false,
	}
	s.mapping = seq.Mapping{
		"k8s_pod":       seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"k8s_namespace": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"k8s_container": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"message":       seq.NewSingleType(seq.TokenizerTypeText, "", 0),
		"level":         seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"service":       seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"status":        seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"source":        seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"trace_id":      seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"spans":         seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
	}
}

func (s *FractionTestSuite) SetupTest() {
	// TODO doesn't work. check
	// var err error
	// s.tmpDir, err = os.MkdirTemp("", "fraction_test_*")
	// s.Require().NoError(err)
}

func (s *FractionTestSuite) InsertIntoActive(active *Active, docs ...string) {
	tokenizers := map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeKeyword: tokenizer.NewKeywordTokenizer(512, false, true),
		seq.TokenizerTypeText:    tokenizer.NewTextTokenizer(20, false, true, 4096),
	}

	// drift and futureDrift are 0, we can process docs at any timestamps
	p := indexer.NewProcessor(s.mapping, tokenizers, 0, 0, 0)

	idx := 0
	readNext := func() ([]byte, error) {
		if idx >= len(docs) {
			return nil, nil
		}
		d := []byte(docs[idx])
		idx++
		return d, nil
	}

	_, binaryDocs, binaryMeta, err := p.ProcessBulk(time.Now(), nil, nil, readNext)
	s.Require().NoError(err, "processing bulk failed")

	compressor := indexer.GetDocsMetasCompressor(3, 3)
	defer indexer.PutDocMetasCompressor(compressor)
	compressor.CompressDocsAndMetas(binaryDocs, binaryMeta)
	docsBlock, metasBlock := compressor.DocsMetas()

	var wg sync.WaitGroup
	wg.Add(1)
	err = active.Append(docsBlock, metasBlock, &wg)
	s.Require().NoError(err, "append to active failed")

	wg.Wait()
}

func (s *FractionTestSuite) TestSearchKeyword() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00Z", "message":"first test document","level":"info","service":"test-service","status":"ok"}`,
		`{"timestamp":"2000-01-01T13:00:01Z", "message":"second test document","level":"error","service":"test-service","status":"fail"}`,
		`{"timestamp":"2000-01-01T13:00:02Z", "message":"third test document","level":"debug","service":"another-service","status":"ok"}`,
		`{"timestamp":"2000-01-01T13:00:03Z", "message":"fourth test document","level":"info","service":"another-service","status":"ok"}`,
	}

	s.insertDocuments(docs...)

	s.AssertSearch("level:info", docs, []int{3, 0})
	s.AssertSearch("level:error", docs, []int{1})
	s.AssertSearch("level:debug", docs, []int{2})

	s.AssertSearch("service:test-service", docs, []int{1, 0})
	s.AssertSearch("service:another-service", docs, []int{3, 2})

	s.AssertSearch("status:ok", docs, []int{3, 2, 0})
	s.AssertSearch("status:fail", docs, []int{1})
}

func (s *FractionTestSuite) TestBasicSearch() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:25Z","service":"service_a","message":"first message some text","trace_id":"abcdef","source":"prod01","level":"1"}`,
		`{"timestamp":"2000-01-01T13:00:32Z","service":"service_b","message":"second message other text","trace_id":"abcdef","source":"prod01","level":"1"}`,
		`{"timestamp":"2000-01-01T13:00:43Z","service":"service_c","message":"third message other text","trace_id":"aaaaaa","source":"prod02","level":"2"}`,
		`{"timestamp":"2000-01-01T13:00:53Z","service":"service_a","message":"fourth message some text","trace_id":"bbbbbb","source":"prod01","level":"1"}`,
	}
	s.insertDocuments(docs...)

	s.AssertSearch("service:service_a", docs, []int{3, 0})
	s.AssertSearch("trace_id:abcdef", docs, []int{1, 0})
	s.AssertSearch("level:1", docs, []int{3, 1, 0})
	s.AssertSearch("source:prod01", docs, []int{3, 1, 0})
	s.AssertSearch("source:prod02", docs, []int{2})

	s.AssertSearch("trace_id:abcd*", docs, []int{1, 0})
	s.AssertSearch("trace_id:a*", docs, []int{2, 1, 0})
	s.AssertSearch("trace_id:a*f", docs, []int{1, 0})
	s.AssertSearch("trace_id:a*a", docs, []int{2})
	s.AssertSearch("service:service*a", docs, []int{3, 0})
}

func (s *FractionTestSuite) TestSearchNot() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:25Z","message":"bad","level":"1","service":"srv_1","status":"ok"}`,
		`{"timestamp":"2000-01-01T13:00:26Z","message":"good","level":"2","service":"srv_2","status":"ok"}`,
		`{"timestamp":"2000-01-01T13:00:27Z","message":"bad","level":"3","service":"srv_3","status":"ok"}`,
		`{"timestamp":"2000-01-01T13:00:28Z","message":"good","level":"4","service":"srv_4","status":"ok"}`,
		`{"timestamp":"2000-01-01T13:00:29Z","message":"bad","level":"5","service":"srv_5","status":"ok"}`,
		`{"timestamp":"2000-01-01T13:00:30Z","message":"good","level":"6","service":"srv_6","status":"ok"}`,
	}

	s.insertDocuments(docs...)

	s.AssertSearch("NOT level:1", docs, []int{5, 4, 3, 2, 1})
	s.AssertSearch("NOT level:2", docs, []int{5, 4, 3, 2, 0})
	s.AssertSearch("NOT level:5", docs, []int{5, 3, 2, 1, 0})
	s.AssertSearch("NOT level:6", docs, []int{4, 3, 2, 1, 0})

	s.AssertSearch("NOT message:notfound", docs, []int{5, 4, 3, 2, 1, 0})
	s.AssertSearch("NOT service:srv_*", docs, []int{})

	s.AssertSearch("NOT message:bad", docs, []int{5, 3, 1})
	s.AssertSearch("NOT message:good", docs, []int{4, 2, 0})

	s.AssertSearch("NOT message:\"good bad\"", docs, []int{5, 4, 3, 2, 1, 0})
	s.AssertSearch("NOT (message:good AND message:bad)", docs, []int{5, 4, 3, 2, 1, 0})
	s.AssertSearch("NOT (message:good OR message:bad)", docs, []int{})

	s.AssertSearch("NOT message:bad AND message:bad", docs, []int{})
	s.AssertSearch("NOT message:bad AND message:good", docs, []int{5, 3, 1})
	s.AssertSearch("message:good AND NOT message:good", docs, []int{})
	s.AssertSearch("message:bad AND NOT message:good", docs, []int{4, 2, 0})
}

func (s *FractionTestSuite) TestWildcardSymbols() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.010Z","message":"first value:****"}`,
		`{"timestamp":"2000-01-01T13:00:00.020Z","message":"second value:*******"}`,
		`{"timestamp":"2000-01-01T13:00:00.030Z","message":"third value****"}`,
		`{"timestamp":"2000-01-01T13:00:00.040Z","message":"fourth ****"}`,
	}
	s.insertDocuments(docs...)

	s.AssertSearch(`message:*`, docs, []int{3, 2, 1, 0})
	s.AssertSearch(`message:value`, docs, []int{1, 0})
	s.AssertSearch(`message:value*`, docs, []int{2, 1, 0})
	s.AssertSearch(`message:value\*`, docs, []int{})
	s.AssertSearch(`message:value\**`, docs, []int{2})
	s.AssertSearch(`message:*\**`, docs, []int{3, 2, 1, 0})
	s.AssertSearch(`message:*e\**`, docs, []int{2})
	s.AssertSearch(`message:\**`, docs, []int{3, 1, 0})
	s.AssertSearch(`message:\*\*\*\*`, docs, []int{3, 0})
	s.AssertSearch(`message:\*\*\*\**`, docs, []int{3, 1, 0})
	s.AssertSearch(`message:value* AND message:\*\**`, docs, []int{1, 0})
	s.AssertSearch(`message:value* OR message:\*\**`, docs, []int{3, 2, 1, 0})

}

func (s *FractionTestSuite) TestSearchFullText() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:30Z","message":"first test document","level":"info","service":"test-service","status":"ok"}`,
		`{"timestamp":"2000-01-01T13:00:31Z","message":"second test document","level":"error","service":"test-service","status":"fail"}`,
		`{"timestamp":"2000-01-01T13:00:32Z","message":"third test document","level":"debug","service":"another-service","status":"ok"}`,
		`{"timestamp":"2000-01-01T13:00:33Z","message":"fourth test document","level":"info","service":"another-service","status":"ok"}`,
	}

	s.insertDocuments(docs...)

	s.AssertSearch("message:document", docs, []int{3, 2, 1, 0})
	s.AssertSearch("message:test", docs, []int{3, 2, 1, 0})
	s.AssertSearch("message:first", docs, []int{0})
	s.AssertSearch("message:second", docs, []int{1})
	s.AssertSearch("message:third", docs, []int{2})
	s.AssertSearch("message:fourth", docs, []int{3})
	s.AssertSearch("message:fifth", docs, []int{})
}

func (s *FractionTestSuite) TestSearchFromTo() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","message":"bad","level":"1","trace_id":"0","service":"0"}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","message":"good","level":"2","trace_id":"0","service":"1"}`,
		`{"timestamp":"2000-01-01T13:00:00.002Z","message":"bad","level":"3","trace_id":"0","service":"2"}`,
		`{"timestamp":"2000-01-01T13:00:00.003Z","message":"good","level":"4","trace_id":"1","service":"0"}`,
		`{"timestamp":"2000-01-01T13:00:00.004Z","message":"bad","level":"5","trace_id":"1","service":"1"}`,
		`{"timestamp":"2000-01-01T13:00:00.005Z","message":"good","level":"6","trace_id":"1","service":"2"}`,
		`{"timestamp":"2000-01-01T13:00:00.006Z","message":"bad","level":"7","trace_id":"2","service":"0"}`,
		`{"timestamp":"2000-01-01T13:00:00.007Z","message":"good","level":"8","trace_id":"2","service":"1"}`,
	}

	s.insertDocuments(docs...)

	assertSearch := func(query string, fromOffset, toOffset int, indexes []int) {
		s.AssertSearch(s.query(
			query,
			withFrom(fmt.Sprintf("2000-01-01T13:00:00.%03dZ", fromOffset)),
			withTo(fmt.Sprintf("2000-01-01T13:00:00.%03dZ", toOffset))),
			docs, indexes)
	}

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

	assertSearch(`NOT trace_id:0`, 0, 2, []int{})
	assertSearch(`NOT trace_id:0`, 0, 3, []int{3})
	assertSearch(`NOT trace_id:1`, 3, 5, []int{})
	assertSearch(`NOT trace_id:1`, 2, 6, []int{6, 2})

	assertSearch(`NOT trace_id:0 AND NOT trace_id:2`, 0, 10, []int{5, 4, 3})
	assertSearch(`NOT trace_id:0 AND NOT trace_id:2`, 3, 5, []int{5, 4, 3})
}

type searchOption func(*processor.SearchParams) error

func (s *FractionTestSuite) query(queryString string, options ...searchOption) *processor.SearchParams {
	queryAst, err := parser.ParseQuery(queryString, s.mapping)
	s.Require().NoError(err, "failed to parse query: %s", queryString)

	params := &processor.SearchParams{
		AST:   queryAst,
		From:  seq.MID(0),
		To:    seq.MID(math.MaxUint64),
		Limit: math.MaxInt32,
	}

	for _, option := range options {
		err := option(params)
		s.Require().NoError(err, "option can not be applied")
	}

	return params
}

func withFrom(from string) searchOption {
	return func(p *processor.SearchParams) error {
		t, err := time.Parse(time.RFC3339, from)
		if err != nil {
			return err
		}
		p.From = seq.TimeToMID(t)
		return nil
	}
}

func withTo(to string) searchOption {
	return func(p *processor.SearchParams) error {
		t, err := time.Parse(time.RFC3339, to)
		if err != nil {
			return err
		}
		p.To = seq.TimeToMID(t)
		return nil
	}
}

func withLimit(limit int) searchOption {
	return func(p *processor.SearchParams) error {
		p.Limit = limit
		return nil
	}
}

func (s *FractionTestSuite) AssertSearch(queryObject interface{}, originalDocs []string, indexes []int) {
	switch q := queryObject.(type) {
	case string:
		s.AssertSearchWithSearchParams(s.query(q), originalDocs, indexes)
	case *processor.SearchParams:
		s.AssertSearchWithSearchParams(q, originalDocs, indexes)
	default:
		s.Require().Fail("type for query object not supported")
	}
}

func (s *FractionTestSuite) AssertSearchWithSearchParams(params *processor.SearchParams, originalDocs []string, indexes []int) {
	dp, release := s.fraction.DataProvider(context.Background())
	defer release()

	qpr, err := dp.Search(*params)
	s.Require().NoError(err, "search failed for query")

	s.Require().Equal(len(indexes), qpr.IDs.Len(),
		"expected %d documents but found %d", len(indexes), qpr.IDs.Len())

	docs, err := dp.Fetch(qpr.IDs.IDs())
	s.Require().NoError(err, "failed to fetch documents for IDs: %v", qpr.IDs.IDs())

	fetchedDocs := make([]string, 0, len(docs))
	for _, doc := range docs {
		fetchedDocs = append(fetchedDocs, string(doc))
	}

	for i, fetchedDoc := range fetchedDocs {
		if i < len(indexes) {
			expectedDoc := originalDocs[indexes[i]]
			s.Require().Equal(expectedDoc, fetchedDoc,
				"document at index %d doesn't match expected document at original index %d",
				i, indexes[i])
		}
	}
}

type ActiveFractionSuite struct {
	FractionTestSuite
}

func (s *ActiveFractionSuite) SetupTest() {
	s.sortCache = cache.NewCache[[]byte](cache.NewCleaner(uint64(10*units.MiB), nil), nil)
	s.indexCache = &IndexCache{
		MIDs:       cache.NewCache[[]byte](cache.NewCleaner(uint64(10*units.MiB), nil), nil),
		RIDs:       cache.NewCache[[]byte](cache.NewCleaner(uint64(10*units.MiB), nil), nil),
		Params:     cache.NewCache[seqids.BlockParams](cache.NewCleaner(uint64(10*units.MiB), nil), nil),
		LIDs:       cache.NewCache[*lids.Block](cache.NewCleaner(uint64(10*units.MiB), nil), nil),
		Tokens:     cache.NewCache[*token.Block](cache.NewCleaner(uint64(10*units.MiB), nil), nil),
		TokenTable: cache.NewCache[token.Table](cache.NewCleaner(uint64(10*units.MiB), nil), nil),
		Registry:   cache.NewCache[[]byte](cache.NewCleaner(uint64(10*units.MiB), nil), nil),
	}
	s.readLimiter = storage.NewReadLimiter(2, NopCounter{})

	// TODO setup test
	var err error
	s.tmpDir, err = os.MkdirTemp("", "fraction_test_*")
	s.Require().NoError(err)

	baseName := filepath.Join(s.tmpDir, "test_fraction")
	indexer := NewActiveIndexer(4, 10)
	indexer.Start()

	active := NewActive(
		baseName,
		indexer,
		s.readLimiter,
		cache.NewCache[[]byte](cache.NewCleaner(uint64(10*units.MiB), nil), nil),
		s.sortCache,
		s.config,
	)

	s.fraction = active
	s.insertDocuments = func(docs ...string) {
		s.InsertIntoActive(active, docs...)
	}
}

func (s *ActiveFractionSuite) TearDownTest() {
	if s.fraction != nil {
		active, ok := s.fraction.(*Active)
		if ok {
			active.Release()
		}
		s.fraction.Suicide()
	}

	err := os.RemoveAll(s.tmpDir)
	s.NoError(err, "failed to remove tmp dir")
}

type SealedFractionSuite struct {
	FractionTestSuite
}

func (s *SealedFractionSuite) SetupTest() {
	s.sortCache = cache.NewCache[[]byte](cache.NewCleaner(uint64(10*units.MiB), nil), nil)
	s.indexCache = &IndexCache{
		MIDs:       cache.NewCache[[]byte](cache.NewCleaner(uint64(10*units.MiB), nil), nil),
		RIDs:       cache.NewCache[[]byte](cache.NewCleaner(uint64(10*units.MiB), nil), nil),
		Params:     cache.NewCache[seqids.BlockParams](cache.NewCleaner(uint64(10*units.MiB), nil), nil),
		LIDs:       cache.NewCache[*lids.Block](cache.NewCleaner(uint64(10*units.MiB), nil), nil),
		Tokens:     cache.NewCache[*token.Block](cache.NewCleaner(uint64(10*units.MiB), nil), nil),
		TokenTable: cache.NewCache[token.Table](cache.NewCleaner(uint64(10*units.MiB), nil), nil),
		Registry:   cache.NewCache[[]byte](cache.NewCleaner(uint64(10*units.MiB), nil), nil),
	}
	s.readLimiter = storage.NewReadLimiter(2, NopCounter{})

	// Ensure tmpDir exists
	// TODO here?
	var err error
	s.tmpDir, err = os.MkdirTemp("", "fraction_test_*")
	s.Require().NoError(err)

	s.insertDocuments = func(docs ...string) {
		baseFile := filepath.Join(s.tmpDir, "test_fraction")
		indexer := NewActiveIndexer(4, 10)
		indexer.Start()

		active := NewActive(
			baseFile,
			indexer,
			s.readLimiter,
			cache.NewCache[[]byte](cache.NewCleaner(uint64(10*units.MiB), nil), nil),
			s.sortCache,
			s.config,
		)

		s.InsertIntoActive(active, docs...)

		sealParams := common.SealParams{
			IDsZstdLevel:           3,
			LIDsZstdLevel:          3,
			TokenListZstdLevel:     3,
			DocsPositionsZstdLevel: 3,
			TokenTableZstdLevel:    3,
			DocBlocksZstdLevel:     3,
			DocBlockSize:           1024 * 1024,
		}

		activeSealingSource, err := NewActiveSealingSource(active, sealParams)
		s.Require().NoError(err, "Sealing source creation failed")

		preloaded, err := sealing.Seal(activeSealingSource, sealParams)
		s.Require().NoError(err, "Sealing failed")

		sealed := NewSealedPreloaded(
			baseFile,
			preloaded,
			s.readLimiter,
			s.indexCache,
			cache.NewCache[[]byte](cache.NewCleaner(uint64(10*units.MiB), nil), nil),
			s.config,
		)
		s.fraction = sealed
		active.Release()
	}
}

func (s *SealedFractionSuite) TearDownTest() {
	if s.fraction != nil {
		s.fraction.Suicide()
	}

	err := os.RemoveAll(s.tmpDir)
	s.NoError(err, "Failed to remove tmp dir")
}

func TestFractionSuites(t *testing.T) {
	suite.Run(t, new(ActiveFractionSuite))
	suite.Run(t, new(SealedFractionSuite))
}
