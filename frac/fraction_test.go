package frac

import (
	"context"
	"math"
	"os"
	"path/filepath"
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

		for _, _ = range mappingTypes.All {
			tokens = append(tokens, seq.Token{
				Field: []byte(fieldName),
				Val:   fieldBytes,
			})
		}
	}
	tokens = append(tokens, seq.Token{
		Field: []byte("_all_"),
		Val:   []byte(""),
	})

	return tokens
}

func (s *FractionTestSuite) AssertSearch(query string, originalDocs []string, indexes []int) {
	seqql, err := parser.ParseSeqQL(query, s.mapping)
	s.Require().NoError(err, "failed to parse query: %s", query)

	dp, release := s.fraction.DataProvider(context.Background())
	defer release()

	params := processor.SearchParams{
		AST:   seqql.Root,
		From:  seq.MID(0),
		To:    seq.MID(math.MaxUint64),
		Limit: math.MaxInt32,
	}

	qpr, err := dp.Search(params)
	s.Require().NoError(err, "search failed for query: %s", query)

	s.Require().Equal(len(indexes), qpr.IDs.Len(),
		"expected %d documents but found %d for query: %s", len(indexes), qpr.IDs.Len(), query)

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
				i, indexes[i], query)
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

/*
TODO not working now because we must properly tokenize message
func (s *FractionTestSuite) TestSearchFullText() {
	docs := []string{
		`{"time":100, "message":"first test document","level":"info","service":"test-service","status":"ok"}`,
		`{"time":101, "message":"second test document","level":"error","service":"test-service","status":"fail"}`,
		`{"time":102, "message":"third test document","level":"debug","service":"another-service","status":"ok"}`,
		`{"time":103, "message":"fourth test document","level":"info","service":"another-service","status":"ok"}`,
	}

	s.insertDocuments(docs...)

	s.AssertSearch("message:document", docs, []int{3, 2, 1, 0})
}*/

func (s *FractionTestSuite) checkContains(fraction Fraction, ids []seq.ID) {
	info := fraction.Info()
	s.Equal(uint32(len(ids)), info.DocsTotal, "Fraction should contain %d documents", len(ids))

	if len(ids) > 0 {
		s.True(fraction.Contains(ids[0].MID), "Fraction should contain first document")
		s.True(fraction.Contains(ids[len(ids)-1].MID), "Fraction should contain last document")

		s.True(fraction.IsIntersecting(ids[0].MID, ids[len(ids)-1].MID),
			"Fraction should intersect with document range")
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
