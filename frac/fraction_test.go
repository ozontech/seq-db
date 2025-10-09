package frac

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
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
	tokenizers  map[seq.TokenizerType]tokenizer.Tokenizer

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
	s.tokenizers = map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeKeyword: tokenizer.NewKeywordTokenizer(20, false, true),
		seq.TokenizerTypeText:    tokenizer.NewTextTokenizer(20, false, true, 100),
		seq.TokenizerTypePath:    tokenizer.NewPathTokenizer(512, false, true),
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
		"request_uri":   seq.NewSingleType(seq.TokenizerTypePath, "", 0),
		"spans":         seq.NewSingleType(seq.TokenizerTypeNested, "", 0),
		"spans.span_id": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"process":       seq.NewSingleType(seq.TokenizerTypeObject, "", 0),
		"process.tags":  seq.NewSingleType(seq.TokenizerTypeTags, "", 0),
		"tags":          seq.NewSingleType(seq.TokenizerTypeTags, "", 0),
		"v":             seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
	}
}

func (s *FractionTestSuite) SetupTestCommon() {
	var err error
	s.tmpDir, err = os.MkdirTemp("", "fraction_test_*")
	s.Require().NoError(err)

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
}

func (s *FractionTestSuite) InsertIntoActive(active *Active, docs ...string) {

	// drift and futureDrift are 0, we can process docs at any timestamps
	processor := indexer.NewProcessor(s.mapping, s.tokenizers, 0, 0, 0)

	idx := 0
	readNext := func() ([]byte, error) {
		if idx >= len(docs) {
			return nil, nil
		}
		d := []byte(docs[idx])
		idx++
		return d, nil
	}

	_, binaryDocs, binaryMeta, err := processor.ProcessBulk(time.Now(), nil, nil, readNext)
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
		`{"timestamp":"2000-01-01T13:00:00Z", "message":"first test document","level":"info","service":"test","status":"ok"}`,
		`{"timestamp":"2000-01-01T13:00:01Z", "message":"second test document","level":"error","service":"test","status":"fail"}`,
		`{"timestamp":"2000-01-01T13:00:02Z", "message":"third test document","level":"debug","service":"prod","status":"ok"}`,
		`{"timestamp":"2000-01-01T13:00:03Z", "message":"fourth test document","level":"info","status":"ok"}`,
	}

	s.insertDocuments(docs...)

	s.AssertSearch("level:info", docs, []int{3, 0})
	s.AssertSearch("level:error", docs, []int{1})
	s.AssertSearch("level:debug", docs, []int{2})

	s.AssertSearch("service:test", docs, []int{1, 0})
	s.AssertSearch("service:prod", docs, []int{2})
	s.AssertSearch("_exists_:service", docs, []int{2, 1, 0})

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
	s.AssertSearch("_all_:*", docs, []int{3, 2, 1, 0})
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

func (s *FractionTestSuite) TestWildcardSymbolsSearch() {
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

func (s *FractionTestSuite) TestSearchPath() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"a","request_uri":"/one"}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","service":"a","request_uri":"/one/two"}`,
		`{"timestamp":"2000-01-01T13:00:00.002Z","service":"a","request_uri":"/one/two/three"}`,
		`{"timestamp":"2000-01-01T13:00:00.003Z","service":"a","request_uri":"/one/two.three/four"}`,
		`{"timestamp":"2000-01-01T13:00:00.004Z","service":"a","request_uri":"/one/two.three/five"}`,
		`{"timestamp":"2000-01-01T13:00:00.005Z","service":"a","request_uri":"/one/two/three/"}`,
		`{"timestamp":"2000-01-01T13:00:00.006Z","service":"a","request_uri":"/one/two/three/1"}`,
		`{"timestamp":"2000-01-01T13:00:00.007Z","service":"a","request_uri":"/one/two/three/2"}`,
		`{"timestamp":"2000-01-01T13:00:00.008Z","service":"a","request_uri":"/one/two/three/3/four/"}`,
		`{"timestamp":"2000-01-01T13:00:00.009Z","service":"a","request_uri":"/one/four/three/3/"}`,
		`{"timestamp":"2000-01-01T13:00:00.010Z","service":"a","request_uri":"/two/one/three/2"}`,
	}

	s.insertDocuments(docs...)

	s.AssertSearch("request_uri:/one", docs, []int{9, 8, 7, 6, 5, 4, 3, 2, 1, 0})
	s.AssertSearch("request_uri:/two", docs, []int{10})
	s.AssertSearch("request_uri:/one/two", docs, []int{8, 7, 6, 5, 2, 1})
	s.AssertSearch("request_uri:/one/two/three", docs, []int{8, 7, 6, 5, 2})
	s.AssertSearch("request_uri:/one/two/three/1", docs, []int{6})
	s.AssertSearch("request_uri:/one/two.three", docs, []int{4, 3})
	s.AssertSearch("request_uri:/one/two.three/four", docs, []int{3})
	s.AssertSearch("request_uri:/one/*/three", docs, []int{9, 8, 7, 6, 5, 2})
	s.AssertSearch("request_uri:/two/*/three", docs, []int{10})
	s.AssertSearch("request_uri:*/three/", docs, []int{5})
	s.AssertSearch("request_uri:*/three", docs, []int{10, 9, 8, 7, 6, 5, 2})
}

func (s *FractionTestSuite) TestSearchANDOR() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","message":"apple","level":"info","service":"svc_a","status":"ok"}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","message":"apple","level":"error","service":"svc_b","status":"fail"}`,
		`{"timestamp":"2000-01-01T13:00:00.002Z","message":"banana","level":"info","service":"svc_a","status":"ok"}`,
		`{"timestamp":"2000-01-01T13:00:00.003Z","message":"banana","level":"error","service":"svc_b","status":"fail"}`,
		`{"timestamp":"2000-01-01T13:00:00.004Z","message":"cherry","level":"info","service":"svc_c","status":"ok"}`,
		`{"timestamp":"2000-01-01T13:00:00.005Z","message":"cherry","level":"warn","service":"svc_c","status":"ok"}`,
	}

	s.insertDocuments(docs...)

	s.AssertSearch("message:apple AND level:info", docs, []int{0})
	s.AssertSearch("message:banana AND service:svc_a", docs, []int{2})
	s.AssertSearch("message:cherry AND level:warn", docs, []int{5})
	s.AssertSearch("level:info AND status:ok", docs, []int{4, 2, 0})
	s.AssertSearch("service:svc_a AND status:ok", docs, []int{2, 0})

	s.AssertSearch("message:apple OR message:banana", docs, []int{3, 2, 1, 0})
	s.AssertSearch("level:error OR level:warn", docs, []int{5, 3, 1})
	s.AssertSearch("service:svc_a OR service:svc_b", docs, []int{3, 2, 1, 0})
	s.AssertSearch("status:fail OR level:warn", docs, []int{5, 3, 1})

	s.AssertSearch("(message:apple OR message:banana) AND level:info", docs, []int{2, 0})
	s.AssertSearch("message:cherry AND (level:info OR level:warn)", docs, []int{5, 4})
	s.AssertSearch("(service:svc_a OR service:svc_b) AND level:info", docs, []int{2, 0})
	s.AssertSearch("(service:svc_a OR service:svc_b) AND (level:info OR level:error)", docs, []int{3, 2, 1, 0})

	s.AssertSearch("(message:apple AND level:info) OR (message:banana AND level:error)", docs, []int{3, 0})
	s.AssertSearch("(message:apple OR message:cherry) AND (level:info OR level:error)", docs, []int{4, 1, 0})
	s.AssertSearch("message:* AND (level:info OR level:error) AND status:ok", docs, []int{4, 2, 0})

	s.AssertSearch("message:apple OR message:notfound", docs, []int{1, 0})
	s.AssertSearch("message:notfound OR message:banana", docs, []int{3, 2})

	s.AssertSearch("message:apple AND message:banana", docs, []int{})
	s.AssertSearch("level:info AND level:error", docs, []int{})
	s.AssertSearch("service:svc_a AND service:svc_b", docs, []int{})
}

func (s *FractionTestSuite) TestSearchRange() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"test-service","level":"1"}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","service":"test-service","level":"3"}`,
		`{"timestamp":"2000-01-01T13:00:00.002Z","service":"test-service","level":"7"}`,
		`{"timestamp":"2000-01-01T13:00:00.003Z","service":"test-service","level":"15"}`,
		`{"timestamp":"2000-01-01T13:00:00.004Z","service":"test-service","level":"31"}`,
		`{"timestamp":"2000-01-01T13:00:00.005Z","service":"test-service","level":"63"}`,
		`{"timestamp":"2000-01-01T13:00:00.006Z","service":"test-service","level":"127"}`,
	}

	s.insertDocuments(docs...)

	s.AssertSearch("level:[1 TO 3]", docs, []int{1, 0})
	s.AssertSearch(s.seqql("level:[1, 3]"), docs, []int{1, 0})
	s.AssertSearch("level:[0 TO 63]", docs, []int{5, 4, 3, 2, 1, 0})
	s.AssertSearch(s.seqql("level:[0, 63]"), docs, []int{5, 4, 3, 2, 1, 0})

	s.AssertSearch("level:{0 TO 3}", docs, []int{0})
	s.AssertSearch("level:{-100 TO 100}", docs, []int{5, 4, 3, 2, 1, 0})

	s.AssertSearch("level:{0 TO 3]", docs, []int{1, 0})
	s.AssertSearch(s.seqql("level:(0, 3]"), docs, []int{1, 0})
	s.AssertSearch("level:[0 TO 3}", docs, []int{0})

	s.AssertSearch("level:[-100 TO 100]", docs, []int{5, 4, 3, 2, 1, 0})

	s.AssertSearch("level:[0 TO *]", docs, []int{6, 5, 4, 3, 2, 1, 0})
	s.AssertSearch(s.seqql("level:[0, *]"), docs, []int{6, 5, 4, 3, 2, 1, 0})
	s.AssertSearch("level:[0 TO *}", docs, []int{6, 5, 4, 3, 2, 1, 0})
	s.AssertSearch("level:[31 TO *]", docs, []int{6, 5, 4})
	s.AssertSearch("level:{31 TO *]", docs, []int{6, 5})

	s.AssertSearch("level:[200 TO 300]", docs, []int{})
	s.AssertSearch("level:{127 TO 200]", docs, []int{})
}

func (s *FractionTestSuite) TestSearchIn() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","message":"starting pod","level":"info","k8s_namespace":"prod","k8s_pod":"proxy-node1"}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","message":"api call failed","level":"error","k8s_namespace":"prod","k8s_pod":"apiserver-master1"}`,
		`{"timestamp":"2000-01-01T13:00:00.002Z","message":"scheduling task","level":"info","k8s_namespace":"test","k8s_pod":"scheduler-master1"}`,
		`{"timestamp":"2000-01-01T13:00:00.003Z","message":"authentication error","level":"error","k8s_namespace":"test","k8s_pod":"apiserver-master2"}`,
		`{"timestamp":"2000-01-01T13:00:00.004Z","message":"network policy applied","level":"info","k8s_namespace":"prod","k8s_pod":"proxy-node2"}`,
		`{"timestamp":"2000-01-01T13:00:00.005Z","message":"scheduling completed","level":"info","k8s_namespace":"staging","k8s_pod":"scheduler-master2"}`,
		`{"timestamp":"2000-01-01T13:00:00.006Z","message":"connection timeout","level":"error","k8s_namespace":"staging","k8s_pod":"app-backend-1"}`,
		`{"timestamp":"2000-01-01T13:00:00.007Z","message":"health check passed","level":"info","k8s_namespace":"prod","k8s_pod":"app-frontend-1"}`,
		`{"timestamp":"2000-01-01T13:00:00.008Z","message":"database query slow","level":"warn","k8s_namespace":"prod","k8s_pod":"app-backend-2"}`,
		`{"timestamp":"2000-01-01T13:00:00.009Z","message":"cache miss","level":"warn","k8s_namespace":"test","k8s_pod":"app-cache-1"}`,
	}

	s.insertDocuments(docs...)

	s.AssertSearch(s.seqql("k8s_namespace:in(prod)"), docs, []int{8, 7, 4, 1, 0})
	s.AssertSearch(s.seqql("k8s_namespace:in(test)"), docs, []int{9, 3, 2})
	s.AssertSearch(s.seqql("k8s_namespace:in(staging)"), docs, []int{6, 5})
	s.AssertSearch(s.seqql("k8s_namespace:in(prod,test)"), docs, []int{9, 8, 7, 4, 3, 2, 1, 0})
	s.AssertSearch(s.seqql("k8s_namespace:in(prod,test,staging)"), docs, []int{9, 8, 7, 6, 5, 4, 3, 2, 1, 0})

	s.AssertSearch(s.seqql("k8s_pod:in(proxy-*)"), docs, []int{4, 0})
	s.AssertSearch(s.seqql("k8s_pod:in(apiserver-*)"), docs, []int{3, 1})
	s.AssertSearch(s.seqql("k8s_pod:in(scheduler-*)"), docs, []int{5, 2})
	s.AssertSearch(s.seqql("k8s_pod:in(proxy-*,apiserver-*)"), docs, []int{4, 3, 1, 0})
	s.AssertSearch(s.seqql("k8s_pod:in(proxy-*,apiserver-*,scheduler-*)"), docs, []int{5, 4, 3, 2, 1, 0})

	s.AssertSearch(s.seqql("level:error AND k8s_namespace:in(prod,test)"), docs, []int{3, 1})
	s.AssertSearch(s.seqql("level:error AND k8s_namespace:in(prod,test) AND k8s_pod:in(apiserver-*)"), docs, []int{3, 1})

	s.AssertSearch(
		s.seqql(`level:error AND k8s_namespace:in(prod,test) AND k8s_pod:in(proxy-*,apiserver-*,scheduler-*)`),
		docs,
		[]int{3, 1})
}

func (s *FractionTestSuite) TestSearchNested() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","spans":[{"span_id":"1"},{"span_id":"2"}]}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","spans":[{"span_id":"2"},{"span_id":"3"}]}`,
		`{"timestamp":"2000-01-01T13:00:00.002Z","spans":[{"span_id":"1"},{"span_id":"3"}]}`,
		`{"timestamp":"2000-01-01T13:00:00.003Z","spans":[{"span_id":"4"},{"span_id":"5"}]}`,
	}

	s.insertDocuments(docs...)

	// Each AssertSearch now tests both desc and asc order
	s.AssertSearch("spans.span_id:*", docs, []int{3, 2, 1, 0})
	s.AssertSearch("spans.span_id:1", docs, []int{2, 0})
	s.AssertSearch("spans.span_id:2", docs, []int{1, 0})
	s.AssertSearch("spans.span_id:3", docs, []int{2, 1})
	s.AssertSearch("spans.span_id:4", docs, []int{3})
	s.AssertSearch("spans.span_id:5", docs, []int{3})
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

	assertSearch := func(query string, fromOffset, toOffset int, expectedIndexes []int) {
		s.AssertSearch(s.query(
			query,
			withFrom(fmt.Sprintf("2000-01-01T13:00:00.%03dZ", fromOffset)),
			withTo(fmt.Sprintf("2000-01-01T13:00:00.%03dZ", toOffset))),
			docs, expectedIndexes)
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

func (s *FractionTestSuite) TestBasicAggregation() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","message":"bad","level":"1","trace_id":"0","service":"proxy"}`,
		`{"timestamp":"2000-01-01T13:00:01.000Z","message":"good","level":"2","trace_id":"0","service":"gateway"}`,
		`{"timestamp":"2000-01-01T13:00:01.000Z","message":"bad","level":"3","trace_id":"0","service":"scheduler"}`,
		`{"timestamp":"2000-01-01T13:00:03.000Z","message":"good","level":"1","trace_id":"1","service":"proxy"}`,
		`{"timestamp":"2000-01-01T13:00:04.000Z","message":"bad","level":"1","trace_id":"1","service":"gateway"}`,
		`{"timestamp":"2000-01-01T13:00:05.000Z","message":"good","level":"1","trace_id":"1","service":"gateway"}`,
	}

	s.insertDocuments(docs...)

	assertAggSearch := func(searchParams *processor.SearchParams, expected []map[string]uint64) {
		dp, release := s.fraction.DataProvider(context.Background())
		defer release()

		qpr, err := dp.Search(*searchParams)
		s.Require().NoError(err, "search failed")

		s.Require().Equal(len(expected), len(qpr.Aggs))
		for i := range expected {
			for bin, hist := range qpr.Aggs[i].SamplesByBin {
				s.Require().Equalf(int64(expected[i][bin.Token]), hist.Total, "failed for token %s", bin)
			}
		}
	}

	assertAggSearch(
		s.query(
			"message:*",
			withAggQuery(processor.AggQuery{GroupBy: aggField("service")})),
		[]map[string]uint64{
			{"gateway": 3, "proxy": 2, "scheduler": 1},
		})
	assertAggSearch(
		s.query(
			"message:good",
			withAggQuery(processor.AggQuery{GroupBy: aggField("service")})),
		[]map[string]uint64{
			{"gateway": 2, "proxy": 1},
		})
	assertAggSearch(
		s.query(
			"message:*",
			withAggQuery(processor.AggQuery{GroupBy: aggField("level")})),
		[]map[string]uint64{
			{"1": 4, "2": 1, "3": 1},
		})
	assertAggSearch(
		s.query(
			"message:*",
			withAggQuery(processor.AggQuery{GroupBy: aggField("service")}),
			withAggQuery(processor.AggQuery{GroupBy: aggField("level")})),
		[]map[string]uint64{
			{"gateway": 3, "proxy": 2, "scheduler": 1},
			{"1": 4, "2": 1, "3": 1},
		})
}

func (s *FractionTestSuite) TestAggSum() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"sum1","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","service":"some_log","v":2}`,
		`{"timestamp":"2000-01-01T13:00:00.002Z","service":"sum1","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.003Z","service":"sum1","v":-1}`,
		`{"timestamp":"2000-01-01T13:00:00.004Z","service":"sum1","v":-0}`,
		`{"timestamp":"2000-01-01T13:00:00.005Z","service":"sum1","v":+0}`,
		`{"timestamp":"2000-01-01T13:00:00.006Z","service":"sum1","v":0}`,
		`{"timestamp":"2000-01-01T13:00:00.007Z","service":"sum1"}`,
		`{"timestamp":"2000-01-01T13:00:00.008Z","service":"sum2","v":-1}`,
		`{"timestamp":"2000-01-01T13:00:00.009Z","service":"sum2","v":-3}`,
		`{"timestamp":"2000-01-01T13:00:00.010Z","service":"sum2","v":-4}`,
		`{"timestamp":"2000-01-01T13:00:00.011Z","service":"sum3","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.012Z","service":"sum4","v":99}`,
		`{"timestamp":"2000-01-01T13:00:00.013Z","service":"sum4","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.014Z","service":"sum4","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.015Z","service":"sum4","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.016Z","service":"sum4","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.017Z","service":"sum4","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.018Z","service":"sum5","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.019Z","service":"sum5"}`,
	}

	s.insertDocuments(docs...)

	dp, release := s.fraction.DataProvider(context.Background())
	defer release()

	searchParams := s.query(
		"service:sum*",
		withAggQuery(processor.AggQuery{
			Field:   aggField("v"),
			GroupBy: aggField("service"),
			Func:    seq.AggFuncSum,
		}))

	qpr, err := dp.Search(*searchParams)
	s.Require().NoError(err, "search failed")

	aggResults := qpr.Aggregate([]seq.AggregateArgs{{Func: seq.AggFuncSum}})
	s.Require().Equal(1, len(aggResults))

	expectedBuckets := []seq.AggregationBucket{
		{Name: "sum4", Value: 104, NotExists: 0},
		{Name: "sum1", Value: 1, NotExists: 1},
		{Name: "sum3", Value: 1, NotExists: 0},
		{Name: "sum5", Value: 1, NotExists: 1},
		{Name: "sum2", Value: -8, NotExists: 0},
	}

	s.Require().Equal(len(expectedBuckets), len(aggResults[0].Buckets), "wrong number of buckets")

	for _, expectedBucket := range expectedBuckets {
		found := false
		for _, gotBucket := range aggResults[0].Buckets {
			if gotBucket.Name == expectedBucket.Name {
				s.Require().Equal(expectedBucket.Value, gotBucket.Value, "wrong value for bucket %s", expectedBucket.Name)
				s.Require().Equal(expectedBucket.NotExists, gotBucket.NotExists, "wrong NotExists for bucket %s", expectedBucket.Name)
				found = true
				break
			}
		}
		s.Require().True(found, "bucket %s not found in results", expectedBucket.Name)
	}
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

func (s *FractionTestSuite) seqql(queryString string, options ...searchOption) *processor.SearchParams {
	queryAst, err := parser.ParseSeqQL(queryString, s.mapping)
	s.Require().NoError(err, "failed to parse query: %s", queryString)

	params := &processor.SearchParams{
		AST:   queryAst.Root,
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

func withAgg(aggQueries ...any) searchOption {
	aggs := make([]processor.AggQuery, 0, len(aggQueries))
	for _, aggQuery := range aggQueries {
		switch aggQuery := aggQuery.(type) {
		case string:
			searchAll := []parser.Term{{
				Kind: parser.TermSymbol, Data: "*",
			}}
			groupBy := &parser.Literal{
				Field: aggQuery,
				Terms: searchAll,
			}
			aggs = append(aggs, processor.AggQuery{GroupBy: groupBy, Func: seq.AggFuncCount})
		case processor.AggQuery:
			aggs = append(aggs, aggQuery)
		default:
			panic("unknown query type")
		}
	}
	return func(sp *processor.SearchParams) error {
		sp.AggQ = append(sp.AggQ, aggs...)
		return nil
	}
}

func aggField(field string) *parser.Literal {
	searchAll := []parser.Term{{
		Kind: parser.TermSymbol, Data: "*",
	}}
	return &parser.Literal{
		Field: field,
		Terms: searchAll,
	}
}

func withAggQuery(aggQuery processor.AggQuery) searchOption {
	return func(sp *processor.SearchParams) error {
		sp.AggQ = append(sp.AggQ, aggQuery)
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
	for _, order := range []seq.DocsOrder{seq.DocsOrderDesc, seq.DocsOrderAsc} {
		params.Order = order

		dp, release := s.fraction.DataProvider(context.Background())

		qpr, err := dp.Search(*params)
		s.Require().NoError(err, "search failed for query with order=%v", order)

		s.Require().Equal(len(indexes), qpr.IDs.Len(),
			"expected %d docs but found %d with order=%v", len(indexes), qpr.IDs.Len(), order)

		docs, err := dp.Fetch(qpr.IDs.IDs())
		s.Require().NoError(err, "failed to fetch docs for IDs: %v", qpr.IDs.IDs())

		if order.IsReverse() {
			slices.Reverse(docs)
		}

		fetchedDocs := make([]string, 0, len(docs))
		for _, doc := range docs {
			fetchedDocs = append(fetchedDocs, string(doc))
		}

		for i, fetchedDoc := range fetchedDocs {
			if i < len(indexes) {
				expectedDoc := originalDocs[indexes[i]]
				s.Require().Equal(expectedDoc, fetchedDoc,
					"doc at index %d doesn't match expected doc at original index %d with order=%v",
					i, indexes[i], order)
			}
		}

		release()
	}
}

type ActiveFractionSuite struct {
	FractionTestSuite
}

func (s *ActiveFractionSuite) SetupTest() {
	s.SetupTestCommon()

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
	s.SetupTestCommon()

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
	// TODO if tear down is same as in active, then move it to FractionSuite
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
