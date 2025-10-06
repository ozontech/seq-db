package frac

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/units"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/sealing"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/stretchr/testify/suite"
)

type FractionTestSuite struct {
	suite.Suite
	tmpDir      string
	docsCache   *cache.Cache[[]byte]
	sortCache   *cache.Cache[[]byte]
	indexCache  *IndexCache
	readLimiter *storage.ReadLimiter
	config      *Config
	mapping     seq.Mapping

	fraction Fraction

	insertDocuments func(docs ...string) []seq.ID
}

func (s *FractionTestSuite) SetupSuite() {
	var err error
	s.tmpDir, err = os.MkdirTemp("", "fraction_test_*")
	s.Require().NoError(err)

	s.config = &Config{
		Search: SearchConfig{
			AggLimits: AggLimits{
				MaxFieldTokens:     1000,
				MaxGroupTokens:     1000,
				MaxTIDsPerFraction: 1000,
			},
		},
		SkipSortDocs: false,
		KeepMetaFile: false,
	}

	s.docsCache = cache.NewCache[[]byte](cache.NewCleaner(uint64(10*units.MiB), nil), nil)
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

func (s *FractionTestSuite) TearDownTest() {
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
	s.Require().NoError(err, "Append should succeed")

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

	return tokens
}

func (s *FractionTestSuite) TestInsertSingleDocument() {
	doc := `{"time":14589329034, "message":"single test document","level":"info","service":"test-service","status":"ok"}`

	ids := s.insertDocuments(doc)

	s.True(s.fraction.Contains(ids[0].MID))
}

/*
func (s *FractionTestSuite) TestInsertMultipleDocuments() {
	docs := []string{
		`{"time":14589329034, "message":"first test document","level":"info","service":"test-service","status":"ok"}`,
		`{"time":14589329035, "message":"second test document","level":"error","service":"test-service","status":"fail"}`,
		`{"time":14589329036, "message":"third test document","level":"debug","service":"another-service","status":"ok"}`,
	}

	ids := s.insertDocuments(docs...)

	s.Len(ids, 3, "Should return 3 document IDs")
	s.True(s.fraction.Contains(ids[0].MID), "Fraction should contain first document")
	s.True(s.fraction.Contains(ids[1].MID), "Fraction should contain second document")
	s.True(s.fraction.Contains(ids[2].MID), "Fraction should contain third document")
}
*/

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
	// TODO setup test
	err := os.MkdirAll(s.tmpDir, 0755)
	s.Require().NoError(err, "Failed to create tmp dir")

	baseName := filepath.Join(s.tmpDir, "test_fraction")
	indexer := NewActiveIndexer(4, 10)
	indexer.Start()

	active := NewActive(
		baseName,
		indexer,
		s.readLimiter,
		s.docsCache,
		s.sortCache,
		s.config,
	)

	s.fraction = active
	s.insertDocuments = func(docs ...string) []seq.ID {
		return s.InsertIntoActive(active, docs...)
	}
}

func (s *ActiveFractionSuite) TearDownTest() {
	s.FractionTestSuite.TearDownTest()
}

type SealedFractionSuite struct {
	FractionTestSuite
}

func (s *SealedFractionSuite) SetupTest() {
	// Ensure tmpDir exists
	err := os.MkdirAll(s.tmpDir, 0755)
	s.Require().NoError(err, "Failed to create tmp dir")

	s.insertDocuments = func(docs ...string) []seq.ID {
		baseFile := filepath.Join(s.tmpDir, "test_fraction")
		indexer := NewActiveIndexer(4, 10)
		indexer.Start()

		active := NewActive(
			baseFile,
			indexer,
			s.readLimiter,
			s.docsCache,
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

		time.Sleep(100 * time.Millisecond)

		activeSealingSource, err := NewActiveSealingSource(active, sealParams)
		s.Require().NoError(err, "Sealing source creation failed")
		preloaded, err := sealing.Seal(activeSealingSource, sealParams)
		s.Require().NoError(err, "Sealing failed")

		sealed := NewSealedPreloaded(
			baseFile,
			preloaded,
			s.readLimiter,
			s.indexCache,
			s.docsCache,
			s.config,
		)
		s.fraction = sealed
		// active.Release()
		return ids
	}
}

func (s *SealedFractionSuite) TearDownTest() {
	s.FractionTestSuite.TearDownTest()
}

func TestFractionSuites(t *testing.T) {
	suite.Run(t, new(ActiveFractionSuite))
	suite.Run(t, new(SealedFractionSuite))
}
