package frac

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/units"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/sealing"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
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

	insertDocuments func(docs ...string) []seq.ID
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
		SkipSortDocs: true, // TODO enabling will fail tests
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

func (s *FractionTestSuite) InsertIntoActive(active *Active, docs ...string) []seq.ID {
	docProvider := NewDocProvider()
	ids := make([]seq.ID, 0, len(docs))

	for i, docStr := range docs {
		docBytes := []byte(docStr)
		root := insaneJSON.Spawn()
		err := root.DecodeBytes(docBytes)
		s.Require().NoError(err, "not a valid JSON", i)

		id := seq.ID{
			MID: seq.MID(time.Now().UnixMilli()) + seq.MID(i*1000), // 1 second apart
			RID: seq.RID(i + 1),
		}
		ids = append(ids, id)
		tokens := s.extractTokens(root)
		docProvider.Append(docBytes, root, id, tokens)
	}

	docsBlock, metasBlock := docProvider.Provide()

	var wg sync.WaitGroup
	wg.Add(1)
	err := active.Append(docsBlock, metasBlock, &wg)
	s.Require().NoError(err, "append to active failed")

	wg.Wait()
	return ids
}

func (s *FractionTestSuite) extractTokens(root *insaneJSON.Root) []seq.Token {
	tokens := make([]seq.Token, 0)

	for fieldName, mappingTypes := range s.mapping {
		fieldValue := root.Dig(fieldName)
		if fieldValue == nil {
			continue
		}

		fieldBytes := fieldValue.AsBytes()
		if len(fieldBytes) == 0 {
			continue
		}

		for _, mappingType := range mappingTypes.All {
			if mappingType.TokenizerType == seq.TokenizerTypeText {
				textTokens := tokenizeText(fieldBytes)
				for _, tokenStr := range textTokens {
					tokens = append(tokens, seq.Token{
						Field: []byte(fieldName),
						Val:   []byte(tokenStr),
					})
				}
			} else {
				tokens = append(tokens, seq.Token{
					Field: []byte(fieldName),
					Val:   fieldBytes,
				})
			}
		}
	}
	tokens = append(tokens, seq.Token{
		Field: []byte("_all_"),
		Val:   []byte(""),
	})

	return tokens
}

// TODO delete this and replace with proper tokenize
func tokenizeText(text []byte) []string {
	if len(text) == 0 {
		return nil
	}

	var tokens []string
	var current strings.Builder

	for i := 0; i < len(text); i++ {
		c := text[i]

		if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '=' {
			if current.Len() > 0 {
				tokens = append(tokens, strings.ToLower(current.String()))
				current.Reset()
			}
		} else if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			current.WriteByte(c)
		}
	}

	if current.Len() > 0 {
		tokens = append(tokens, strings.ToLower(current.String()))
	}

	return tokens
}

func (s *FractionTestSuite) AssertSearch(queryString string, originalDocs []string, indexes []int) {
	s.AssertSearchQuery(query(queryString), originalDocs, indexes)
}

func (s *FractionTestSuite) AssertSearchQuery(query *SearchQuery, originalDocs []string, indexes []int) {
	var queryStr string
	var from, to seq.MID
	var limit int

	queryStr = query.query
	if query.from != nil {
		from = *query.from
	} else {
		from = seq.MID(0)
	}
	if query.to != nil {
		to = *query.to
	} else {
		to = seq.MID(math.MaxUint64)
	}
	if query.limit != nil {
		limit = *query.limit
	} else {
		limit = math.MaxInt32
	}

	seqql, err := parser.ParseSeqQL(queryStr, s.mapping)
	s.Require().NoError(err, "failed to parse query: %s", queryStr)

	dp, release := s.fraction.DataProvider(context.Background())
	defer release()

	params := processor.SearchParams{
		AST:   seqql.Root,
		From:  from,
		To:    to,
		Limit: limit,
	}

	qpr, err := dp.Search(params)
	s.Require().NoError(err, "search failed for query: %s", queryStr)

	s.Require().Equal(len(indexes), qpr.IDs.Len(),
		"expected %d documents but found %d for query: %s", len(indexes), qpr.IDs.Len(), queryStr)

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
				"document at index %d doesn't match expected document at original index %d for query: %s",
				i, indexes[i], queryStr)
		}
	}
}

func (s *FractionTestSuite) TestContainsDocuments() {
	docs := []string{
		`{"time":100, "message":"first test document","level":"info","service":"test-service","status":"ok"}`,
		`{"time":101, "message":"second test document","level":"error","service":"test-service","status":"fail"}`,
		`{"time":102, "message":"third test document","level":"debug","service":"another-service","status":"ok"}`,
	}

	ids := s.insertDocuments(docs...)

	s.Len(ids, 3, "Should return 3 document IDs")
	s.True(s.fraction.Contains(ids[0].MID))
	s.True(s.fraction.Contains(ids[1].MID))
	s.True(s.fraction.Contains(ids[2].MID))
}

func (s *FractionTestSuite) TestSearchKeyword() {
	docs := []string{
		`{"time":100, "message":"first test document","level":"info","service":"test-service","status":"ok"}`,
		`{"time":101, "message":"second test document","level":"error","service":"test-service","status":"fail"}`,
		`{"time":102, "message":"third test document","level":"debug","service":"another-service","status":"ok"}`,
		`{"time":103, "message":"fourth test document","level":"info","service":"another-service","status":"ok"}`,
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
		`{"timestamp":110,"service":"service_a","message":"first message some text","trace_id":"abcdef","source":"prod01","level":"1"}`,
		`{"timestamp":130,"service":"service_b","message":"second message other text","trace_id":"abcdef","source":"prod01","level":"1"}`,
		`{"timestamp":140,"service":"service_c","message":"third message other text","trace_id":"aaaaaa","source":"prod02","level":"2"}`,
		`{"timestamp":120,"service":"service_a","message":"fourth message some text","trace_id":"bbbbbb","source":"prod01","level":"1"}`,
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
		`{"timestamp":100,"message":"bad","level":"1","service":"srv_1","status":"ok"}`,
		`{"timestamp":101,"message":"good","level":"2","service":"srv_2","status":"ok"}`,
		`{"timestamp":102,"message":"bad","level":"3","service":"srv_3","status":"ok"}`,
		`{"timestamp":103,"message":"good","level":"4","service":"srv_4","status":"ok"}`,
		`{"timestamp":104,"message":"bad","level":"5","service":"srv_5","status":"ok"}`,
		`{"timestamp":105,"message":"good","level":"6","service":"srv_6","status":"ok"}`,
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
		`{"timestamp":110,"service":"first_value","level":"info"}`,
		`{"timestamp":120,"service":"second_value","level":"error"}`,
		`{"timestamp":130,"service":"third_value","level":"debug"}`,
		`{"timestamp":140,"service":"fourth","level":"warn"}`,
	}
	s.insertDocuments(docs...)

	s.AssertSearch("service:*", docs, []int{3, 2, 1, 0})
	s.AssertSearch("service:first_value", docs, []int{0})
	s.AssertSearch("service:second_value", docs, []int{1})
	s.AssertSearch("service:third_value", docs, []int{2})
	s.AssertSearch("service:fourth", docs, []int{3})
	s.AssertSearch("level:*", docs, []int{3, 2, 1, 0})
	s.AssertSearch("level:info", docs, []int{0})
	s.AssertSearch("level:error", docs, []int{1})
	s.AssertSearch("level:debug", docs, []int{2})
	s.AssertSearch("level:warn", docs, []int{3})
}

func (s *FractionTestSuite) TestFetch() {
	docs := []string{
		`{"timestamp":100,"message":"bad","level":"1","trace_id":"0","service":"0","status":"ok"}`,
		`{"timestamp":101,"message":"good","level":"2","trace_id":"0","service":"1","status":"ok"}`,
		`{"timestamp":102,"message":"bad","level":"3","trace_id":"0","service":"2","status":"ok"}`,
		`{"timestamp":103,"message":"good","level":"4","trace_id":"1","service":"0","status":"ok"}`,
		`{"timestamp":104,"message":"bad","level":"5","trace_id":"1","service":"1","status":"ok"}`,
		`{"timestamp":105,"message":"good","level":"6","trace_id":"1","service":"2","status":"ok"}`,
		`{"timestamp":106,"message":"bad","level":"7","trace_id":"2","service":"0","status":"ok"}`,
		`{"timestamp":107,"message":"good","level":"8","trace_id":"2","service":"1","status":"ok"}`,
	}

	ids := s.insertDocuments(docs...)
	s.Require().Equal(8, len(ids))

	// Test fetching all documents using a simple query
	dp, release := s.fraction.DataProvider(context.Background())
	defer release()

	// Use a simple query that matches all documents
	seqql, err := parser.ParseSeqQL("_all_:*", s.mapping)
	s.Require().NoError(err)

	params := processor.SearchParams{
		AST:   seqql.Root,
		From:  seq.MID(0),
		To:    seq.MID(math.MaxUint64),
		Limit: math.MaxInt32,
	}

	qpr, err := dp.Search(params)
	s.Require().NoError(err)
	s.Require().Equal(8, qpr.IDs.Len())

	// Test fetching documents by IDs
	fetchedDocs, err := dp.Fetch(qpr.IDs.IDs())
	s.Require().NoError(err)
	s.Require().Equal(len(qpr.IDs.IDs()), len(fetchedDocs))
}

func (s *FractionTestSuite) TestSearchFullText() {
	docs := []string{
		`{"timestamp":100,"message":"first test document","level":"info","service":"test-service","status":"ok"}`,
		`{"timestamp":101,"message":"second test document","level":"error","service":"test-service","status":"fail"}`,
		`{"timestamp":102,"message":"third test document","level":"debug","service":"another-service","status":"ok"}`,
		`{"timestamp":103,"message":"fourth test document","level":"info","service":"another-service","status":"ok"}`,
	}

	ids := s.insertDocuments(docs...)
	s.Require().Equal(4, len(ids))

	s.AssertSearch("message:document", docs, []int{3, 2, 1, 0})
	s.AssertSearch("message:test", docs, []int{3, 2, 1, 0})
	s.AssertSearch("message:first", docs, []int{0})
	s.AssertSearch("message:second", docs, []int{1})
	s.AssertSearch("message:third", docs, []int{2})
	s.AssertSearch("message:fourth", docs, []int{3})
	s.AssertSearch("message:fivth", docs, []int{})
}

func (s *FractionTestSuite) TestSearchFromTo() {
	docs := []string{
		`{"timestamp":100,"message":"first test document","level":"info","service":"test-service","status":"ok"}`,
		`{"timestamp":101,"message":"second test document","level":"error","service":"test-service","status":"fail"}`,
		`{"timestamp":102,"message":"third test document","level":"debug","service":"another-service","status":"ok"}`,
		`{"timestamp":103,"message":"fourth test document","level":"info","service":"another-service","status":"ok"}`,
	}

	ids := s.insertDocuments(docs...)
	s.Require().Equal(4, len(ids))

	s.AssertSearchQuery(query("level:info").From(0).To(200), docs, []int{3, 0})
}

type SearchQuery struct {
	query  string
	from   *seq.MID
	to     *seq.MID
	offset *int
	limit  *int
}

func query(q string) *SearchQuery {
	return &SearchQuery{query: q}
}

func (sq *SearchQuery) From(timestamp uint64) *SearchQuery {
	mid := seq.MID(timestamp)
	sq.from = &mid
	return sq
}

func (sq *SearchQuery) To(timestamp uint64) *SearchQuery {
	mid := seq.MID(timestamp)
	sq.to = &mid
	return sq
}

func (sq *SearchQuery) Offset(offset int) *SearchQuery {
	sq.offset = &offset
	return sq
}

func (sq *SearchQuery) Limit(limit int) *SearchQuery {
	sq.limit = &limit
	return sq
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
	s.insertDocuments = func(docs ...string) []seq.ID {
		return s.InsertIntoActive(active, docs...)
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
	s.NoError(err, "Failed to remove tmp dir")
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

	s.insertDocuments = func(docs ...string) []seq.ID {
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

		ids := s.InsertIntoActive(active, docs...)

		if len(ids) == 0 {
			// TODO fail test?
			active.Release()
			return ids
		}

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
		return ids
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
