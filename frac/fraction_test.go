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
	tmpDir     string
	config     *Config
	mapping    seq.Mapping
	tokenizers map[seq.TokenizerType]tokenizer.Tokenizer

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
		SkipSortDocs: false,
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
		"client_ip":     seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"service":       seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"status":        seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"source":        seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"trace_id":      seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"request_uri":   seq.NewSingleType(seq.TokenizerTypePath, "", 0),
		"spans":         seq.NewSingleType(seq.TokenizerTypeNested, "", 0),
		"spans.span_id": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"v":             seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
	}
}

func (s *FractionTestSuite) SetupTestCommon() {
	var err error
	s.tmpDir, err = os.MkdirTemp("", "fraction_test_*")
	s.Require().NoError(err)
}

func newSmallCache[V any]() *cache.Cache[V] {
	return cache.NewCache[V](cache.NewCleaner(uint64(units.KiB), nil), nil)
}

func (s *FractionTestSuite) TearDownTestCommon() {
	err := os.RemoveAll(s.tmpDir)
	s.NoError(err, "Failed to remove tmp dir")
}

func (s *FractionTestSuite) TestSearchKeyword() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:25Z","service":"service_a","message":"first message some text","trace_id":"abcdef","source":"prod01","level":"1"}`,
		`{"timestamp":"2000-01-01T13:00:32Z","service":"service_b","message":"second message other text","trace_id":"abcdef","source":"prod01","level":"1"}`,
		`{"timestamp":"2000-01-01T13:00:43Z","service":"service_c","message":"third message other text","trace_id":"aaaaaa","source":"prod02","level":"2"}`,
		`{"timestamp":"2000-01-01T13:00:53Z","service":"service_a","message":"fourth message some text","trace_id":"bbbbbb","source":"prod01","level":"1"}`,
		`{"timestamp":"2000-01-01T13:00:54Z","service":"service_c","message":"apple","source":"prod03"}`,
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
	s.AssertSearch("_all_:*", docs, []int{4, 3, 2, 1, 0})

	s.AssertSearch("_exists_:message", docs, []int{4, 3, 2, 1, 0})
	s.AssertSearch("_exists_:level", docs, []int{3, 2, 1, 0})
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

func (s *FractionTestSuite) TestSearchAndOr() {
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

	s.AssertSearch(`message:"first test"`, docs, []int{0})
	s.AssertSearch(`message:"first document"`, docs, []int{0})
	s.AssertSearch(`message:"test document"`, docs, []int{3, 2, 1, 0})
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

func (s *FractionTestSuite) TestSearchIPRange() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"gateway-0","level":"1","client_ip":"192.168.31.0"}`,
		`{"timestamp":"2000-01-01T13:00:01.000Z","service":"gateway-1","level":"1","client_ip":"192.168.0.1"}`,
		`{"timestamp":"2000-01-01T13:00:02.000Z","service":"gateway-2","level":"1","client_ip":"192.168.0.2"}`,
		`{"timestamp":"2000-01-01T13:00:03.000Z","service":"gateway-3","level":"1","client_ip":"192.168.0.3"}`,
		`{"timestamp":"2000-01-01T13:00:04.000Z","service":"gateway-0","level":"1","client_ip":"192.168.1.0"}`,
		`{"timestamp":"2000-01-01T13:00:05.000Z","service":"gateway-1","level":"1","client_ip":"192.168.1.1"}`,
		`{"timestamp":"2000-01-01T13:00:06.000Z","service":"gateway-0","level":"1","client_ip":"192.168.1.2"}`,
		`{"timestamp":"2000-01-01T13:00:07.000Z","service":"gateway-1","level":"1","client_ip":"192.168.1.255"}`,
		`{"timestamp":"2000-01-01T13:00:08.000Z","service":"gateway-3","level":"1","client_ip":"192.168.31.0"}`,
		`{"timestamp":"2000-01-01T13:00:09.000Z","service":"api-0","level":"2","client_ip":"172.10.0.1"}`,
		`{"timestamp":"2000-01-01T13:00:10.000Z","service":"api-1","level":"2","client_ip":"172.10.0.100"}`,
		`{"timestamp":"2000-01-01T13:00:11.000Z","service":"api-2","level":"2","client_ip":"172.10.1.50"}`,
		`{"timestamp":"2000-01-01T13:00:12.000Z","service":"api-3","level":"2","client_ip":"172.10.1.200"}`,
		`{"timestamp":"2000-01-01T13:00:13.000Z","service":"api-4","level":"2","client_ip":"172.10.2.1"}`,
		`{"timestamp":"2000-01-01T13:00:14.000Z","service":"backend-0","level":"3","client_ip":"10.53.0.10"}`,
		`{"timestamp":"2000-01-01T13:00:15.000Z","service":"backend-1","level":"3","client_ip":"10.53.0.20"}`,
		`{"timestamp":"2000-01-01T13:00:16.000Z","service":"backend-2","level":"3","client_ip":"10.53.1.30"}`,
		`{"timestamp":"2000-01-01T13:00:17.000Z","service":"backend-3","level":"3","client_ip":"10.53.1.40"}`,
		`{"timestamp":"2000-01-01T13:00:18.000Z","service":"backend-4","level":"3","client_ip":"10.53.2.50"}`,
	}

	s.insertDocuments(docs...)

	s.AssertSearch(s.seqql("client_ip:ip_range(192.168.0.0,192.168.0.255)"), docs, []int{3, 2, 1})
	s.AssertSearch(s.seqql("client_ip:ip_range(192.168.1.0,192.168.1.255)"), docs, []int{7, 6, 5, 4})
	s.AssertSearch(s.seqql("client_ip:ip_range(172.10.0.0,172.10.0.255)"), docs, []int{10, 9})
	s.AssertSearch(s.seqql("client_ip:ip_range(172.10.0.0,172.10.255.255)"), docs, []int{13, 12, 11, 10, 9})
	s.AssertSearch(s.seqql("client_ip:ip_range(10.53.0.0,10.53.0.255)"), docs, []int{15, 14})
	s.AssertSearch(s.seqql("client_ip:ip_range(10.53.0.0,10.53.255.255)"), docs, []int{18, 17, 16, 15, 14})

	s.AssertSearch(s.seqql("client_ip:ip_range(192.168.0.0/24)"), docs, []int{3, 2, 1})
	s.AssertSearch(s.seqql("client_ip:ip_range(192.168.1.0/24)"), docs, []int{7, 6, 5, 4})
	s.AssertSearch(s.seqql("client_ip:ip_range(172.10.0.0/24)"), docs, []int{10, 9})
	s.AssertSearch(s.seqql("client_ip:ip_range(10.53.0.0/24)"), docs, []int{15, 14})

	s.AssertSearch(s.seqql("client_ip:ip_range(172.10.0.0/16)"), docs, []int{13, 12, 11, 10, 9})
	s.AssertSearch(s.seqql("client_ip:ip_range(10.53.0.0/16)"), docs, []int{18, 17, 16, 15, 14})

	s.AssertSearch(s.seqql("client_ip:ip_range(192.168.31.0/32)"), docs, []int{8, 0})
	s.AssertSearch(s.seqql("client_ip:ip_range(172.10.0.1/32)"), docs, []int{9})
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

	s.AssertSearchIgnoreTotal("spans.span_id:*", docs, []int{3, 2, 1, 0})
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

func (s *FractionTestSuite) TestSearchWithLimit() {
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

	s.AssertSearch(s.query("message:good"), docs, []int{7, 5, 3, 1})
	s.AssertSearch(s.query("message:good", withLimit(3)), docs, []int{7, 5, 3})
	s.AssertSearch(s.query(
		"message:good",
		withLimit(2),
		withFrom("2000-01-01T13:00:00.000Z"),
		withTo("2000-01-01T13:00:00.005Z")),
		docs,
		[]int{5, 3})
}

func (s *FractionTestSuite) TestSearchHist() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:01.549Z","message": "apple banana smoothie"}`,
		`{"timestamp":"2000-01-01T13:00:02.690Z","message": "apple banana salad"}`,
		`{"timestamp":"2000-01-01T13:00:03.102Z","message": "apple banana pineapple smoothie"}`,
		`{"timestamp":"2000-01-01T13:00:03.052Z","message": "apple juice"}`,
		`{"timestamp":"2000-01-01T13:00:04.999Z","message": "banana"}`,
		`{"timestamp":"2000-01-01T13:00:05.000Z","message": "apple juice"}`,
		`{"timestamp":"2000-01-01T13:00:10.777Z","message": "apple banana"}`,
		`{"timestamp":"2000-01-01T13:00:15.100Z","message": "apple pie"}`,
		`{"timestamp":"2000-01-01T13:00:15.200Z","message": "apple tart"}`,
		`{"timestamp":"2000-01-01T13:00:15.300Z","message": "apple crisp"}`,
		`{"timestamp":"2000-01-01T13:00:20.500Z","message": "orange juice"}`,
		`{"timestamp":"2000-01-01T13:00:25.600Z","message": "apple cider"}`,
	}

	s.insertDocuments(docs...)

	s.AssertHist(s.query("message:apple", withHist(1000)), map[string]uint64{
		"2000-01-01T13:00:01.000Z": 1,
		"2000-01-01T13:00:02.000Z": 1,
		"2000-01-01T13:00:03.000Z": 2,
		"2000-01-01T13:00:05.000Z": 1,
		"2000-01-01T13:00:10.000Z": 1,
		"2000-01-01T13:00:15.000Z": 3,
		"2000-01-01T13:00:25.000Z": 1,
	})
	s.AssertHist(s.query("message:apple", withHist(3000)), map[string]uint64{
		"2000-01-01T13:00:00.000Z": 2,
		"2000-01-01T13:00:03.000Z": 3,
		"2000-01-01T13:00:09.000Z": 1,
		"2000-01-01T13:00:15.000Z": 3,
		"2000-01-01T13:00:24.000Z": 1,
	})
	s.AssertHist(s.query("message:*", withHist(1000)), map[string]uint64{
		"2000-01-01T13:00:01.000Z": 1,
		"2000-01-01T13:00:02.000Z": 1,
		"2000-01-01T13:00:03.000Z": 2,
		"2000-01-01T13:00:04.000Z": 1,
		"2000-01-01T13:00:05.000Z": 1,
		"2000-01-01T13:00:10.000Z": 1,
		"2000-01-01T13:00:15.000Z": 3,
		"2000-01-01T13:00:20.000Z": 1,
		"2000-01-01T13:00:25.000Z": 1,
	})
	s.AssertHist(s.query("message:*", withHist(2000)), map[string]uint64{
		"2000-01-01T13:00:00.000Z": 1,
		"2000-01-01T13:00:02.000Z": 3,
		"2000-01-01T13:00:04.000Z": 2,
		"2000-01-01T13:00:10.000Z": 1,
		"2000-01-01T13:00:14.000Z": 3,
		"2000-01-01T13:00:20.000Z": 1,
		"2000-01-01T13:00:24.000Z": 1,
	})
	s.AssertHist(s.query(
		"message:*",
		withFrom("2000-01-01T13:00:03.000Z"),
		withTo("2000-01-01T13:00:15.000Z"),
		withHist(1000)),
		map[string]uint64{
			"2000-01-01T13:00:03.000Z": 2,
			"2000-01-01T13:00:04.000Z": 1,
			"2000-01-01T13:00:05.000Z": 1,
			"2000-01-01T13:00:10.000Z": 1,
		})
	s.AssertHist(s.query(
		"message:*",
		withFrom("2000-01-01T13:00:03.000Z"),
		withTo("2000-01-01T13:00:15.000Z"),
		withHist(1000)),
		map[string]uint64{
			"2000-01-01T13:00:03.000Z": 2,
			"2000-01-01T13:00:04.000Z": 1,
			"2000-01-01T13:00:05.000Z": 1,
			"2000-01-01T13:00:10.000Z": 1,
		})
	// Limit doesn't `limit` histogram but only query results
	s.AssertHist(s.query(
		"message:*",
		withFrom("2000-01-01T13:00:03.000Z"),
		withTo("2000-01-01T13:00:15.000Z"),
		withLimit(1),
		withHist(1000)),
		map[string]uint64{
			"2000-01-01T13:00:03.000Z": 2,
			"2000-01-01T13:00:04.000Z": 1,
			"2000-01-01T13:00:05.000Z": 1,
			"2000-01-01T13:00:10.000Z": 1,
		})
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

		qpr, err := s.fraction.Search(context.Background(), *searchParams)
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

	searchParams := s.query(
		"service:sum*",
		withAggQuery(processor.AggQuery{
			Field:   aggField("v"),
			GroupBy: aggField("service"),
			Func:    seq.AggFuncSum,
		}))
	expectedBuckets := []seq.AggregationBucket{
		{Name: "sum4", Value: 104, NotExists: 0},
		{Name: "sum1", Value: 1, NotExists: 1},
		{Name: "sum3", Value: 1, NotExists: 0},
		{Name: "sum5", Value: 1, NotExists: 1},
		{Name: "sum2", Value: -8, NotExists: 0},
	}
	s.AssertAggregation(searchParams, seq.AggregateArgs{Func: seq.AggFuncSum}, expectedBuckets)
}

func (s *FractionTestSuite) TestAggMin() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"min1","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","service":"min1","v":2}`,
		`{"timestamp":"2000-01-01T13:00:00.002Z","service":"min2","v":3}`,
		`{"timestamp":"2000-01-01T13:00:00.003Z","service":"min2","v":"-10"}`,
		`{"timestamp":"2000-01-01T13:00:00.004Z","service":"min4"}`,
		`{"timestamp":"2000-01-01T13:00:00.005Z","service":"min4"}`,
		`{"timestamp":"2000-01-01T13:00:00.006Z","service":"min4"}`,
		`{"timestamp":"2000-01-01T13:00:00.007Z","service":"min4"}`,
		`{"timestamp":"2000-01-01T13:00:00.008Z","service":"min4"}`,
		`{"timestamp":"2000-01-01T13:00:00.009Z","service":"min4"}`,
		`{"timestamp":"2000-01-01T13:00:00.010Z","service":"min4"}`,
		`{"timestamp":"2000-01-01T13:00:00.011Z","service":null,"v":null}`,
		`{"timestamp":"2000-01-01T13:00:00.012Z","v":null}`,
	}

	s.insertDocuments(docs...)

	searchParams := s.query(
		"service:min*",
		withAggQuery(processor.AggQuery{
			Field:   aggField("v"),
			GroupBy: aggField("service"),
			Func:    seq.AggFuncMin,
		}))
	expectedBuckets := []seq.AggregationBucket{
		{Name: "min4", Value: math.NaN(), NotExists: 7},
		{Name: "min2", Value: -10, NotExists: 0},
		{Name: "min1", Value: 1, NotExists: 0},
	}
	s.AssertAggregation(searchParams, seq.AggregateArgs{Func: seq.AggFuncMin}, expectedBuckets)
}

func (s *FractionTestSuite) TestAggMax() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"max1","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","service":"max1","v":2}`,
		`{"timestamp":"2000-01-01T13:00:00.002Z","service":"max2","v":3}`,
		`{"timestamp":"2000-01-01T13:00:00.003Z","service":"max2","v":"-10"}`,
		`{"timestamp":"2000-01-01T13:00:00.004Z","service":"max4"}`,
		`{"timestamp":"2000-01-01T13:00:00.005Z","service":"max4"}`,
		`{"timestamp":"2000-01-01T13:00:00.006Z","service":null,"v":null}`,
		`{"timestamp":"2000-01-01T13:00:00.007Z","v":null}`,
	}

	s.insertDocuments(docs...)

	searchParams := s.query(
		"service:max*",
		withAggQuery(processor.AggQuery{
			Field:   aggField("v"),
			GroupBy: aggField("service"),
			Func:    seq.AggFuncMax,
		}))
	expectedBuckets := []seq.AggregationBucket{
		{Name: "max2", Value: 3, NotExists: 0},
		{Name: "max1", Value: 2, NotExists: 0},
		{Name: "max4", Value: math.NaN(), NotExists: 2},
	}
	s.AssertAggregation(searchParams, seq.AggregateArgs{Func: seq.AggFuncMax}, expectedBuckets)
}

func (s *FractionTestSuite) TestAggQuantile() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"quantile1","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","service":"quantile1","v":2}`,
		`{"timestamp":"2000-01-01T13:00:00.002Z","service":"quantile1","v":3}`,
		`{"timestamp":"2000-01-01T13:00:00.003Z","service":"quantile1","v":4}`,
		`{"timestamp":"2000-01-01T13:00:00.004Z","service":"quantile1","v":5}`,
		`{"timestamp":"2000-01-01T13:00:00.005Z","service":"quantile1","v":6}`,
		`{"timestamp":"2000-01-01T13:00:00.006Z","service":"quantile1","v":7}`,
		`{"timestamp":"2000-01-01T13:00:00.007Z","service":"quantile1","v":8}`,
		`{"timestamp":"2000-01-01T13:00:00.008Z","service":"quantile1","v":9}`,
		`{"timestamp":"2000-01-01T13:00:00.009Z","service":"quantile1","v":10}`,
	}

	s.insertDocuments(docs...)

	searchParams := s.query(
		"service:quantile*",
		withAggQuery(processor.AggQuery{
			Field:     aggField("v"),
			GroupBy:   aggField("service"),
			Func:      seq.AggFuncQuantile,
			Quantiles: []float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 0.99, 0.999, 0.99999999},
		}))
	expectedBuckets := []seq.AggregationBucket{
		{
			Name:      "quantile1",
			Value:     1,
			Quantiles: []float64{1, 2, 3, 4, 5, 6, 6, 7, 8, 8, 9, 10, 10, 10},
			NotExists: 0,
		},
	}
	s.AssertAggregation(searchParams, seq.AggregateArgs{
		Func:      seq.AggFuncQuantile,
		Quantiles: []float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 0.99, 0.999, 0.99999999},
	}, expectedBuckets)
}

func (s *FractionTestSuite) TestAggUnique() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"some_log","level":2}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","service":"unique1","level":3}`,
		`{"timestamp":"2000-01-01T13:00:00.002Z","service":"unique2","level":3}`,
		`{"timestamp":"2000-01-01T13:00:00.003Z","service":"unique2","level":3}`,
		`{"timestamp":"2000-01-01T13:00:00.004Z","service":"unique3","level":3}`,
		`{"timestamp":"2000-01-01T13:00:00.005Z","service":"unique3","level":2}`,
		`{"timestamp":"2000-01-01T13:00:00.006Z","service":"unique4","level":3}`,
		`{"timestamp":"2000-01-01T13:00:00.007Z","service":"unique4","level":2}`,
		`{"timestamp":"2000-01-01T13:00:00.008Z","service":"unique4","level":3}`,
		`{"timestamp":"2000-01-01T13:00:00.009Z","service":"unique5","level":3}`,
		`{"timestamp":"2000-01-01T13:00:00.010Z","level":3}`,
	}

	s.insertDocuments(docs...)

	searchParams := s.query(
		"level:3",
		withAggQuery(processor.AggQuery{
			GroupBy: aggField("service"),
			Func:    seq.AggFuncUnique,
		}))
	expectedBuckets := []seq.AggregationBucket{
		{Name: "unique1", Value: 0, NotExists: 0},
		{Name: "unique2", Value: 0, NotExists: 0},
		{Name: "unique3", Value: 0, NotExists: 0},
		{Name: "unique4", Value: 0, NotExists: 0},
		{Name: "unique5", Value: 0, NotExists: 0},
	}
	s.AssertAggregation(searchParams, seq.AggregateArgs{Func: seq.AggFuncUnique}, expectedBuckets)
}

func (s *FractionTestSuite) TestAggSumWithoutGroupBy() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","v":1,"service":"sum_without_group_by"}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","v":1,"service":"sum_without_group_by"}`,
		`{"timestamp":"2000-01-01T13:00:00.002Z","v":2,"service":"sum_without_group_by"}`,
		`{"timestamp":"2000-01-01T13:00:00.003Z","v":1,"service":"sum_without_group_by"}`,
		`{"timestamp":"2000-01-01T13:00:00.004Z","v":1,"service":"sum_without_group_by"}`,
		`{"timestamp":"2000-01-01T13:00:00.005Z","v":1,"service":"sum_without_group_by"}`,
		`{"timestamp":"2000-01-01T13:00:00.006Z","v":1,"service":"sum_without_group_by"}`,
		`{"timestamp":"2000-01-01T13:00:00.007Z","v":2,"service":"sum_without_group_by"}`,
		`{"timestamp":"2000-01-01T13:00:00.008Z","v":-0,"service":"sum_without_group_by"}`,
		`{"timestamp":"2000-01-01T13:00:00.009Z","v":+0,"service":"sum_without_group_by"}`,
		`{"timestamp":"2000-01-01T13:00:00.010Z","v":0,"service":"sum_without_group_by"}`,
	}

	s.insertDocuments(docs...)

	searchParams := s.query(
		`service:"sum_without_group_by"`,
		withAggQuery(processor.AggQuery{
			Field: aggField("v"),
			Func:  seq.AggFuncSum,
		}))
	expectedBuckets := []seq.AggregationBucket{
		{Name: "", Value: 10, NotExists: 0},
	}
	s.AssertAggregation(searchParams, seq.AggregateArgs{Func: seq.AggFuncSum}, expectedBuckets)
}

func (s *FractionTestSuite) TestAggMaxWithoutGroupBy() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","v":100,"service":"max_without_group_by"}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","v":-200,"service":"max_without_group_by"}`,
		`{"timestamp":"2000-01-01T13:00:00.002Z","v":300,"service":"max_without_group_by"}`,
		`{"timestamp":"2000-01-01T13:00:00.003Z","v":-300,"service":"max_without_group_by"}`,
	}

	s.insertDocuments(docs...)

	searchParams := s.query(
		`service:"max_without_group_by"`,
		withAggQuery(processor.AggQuery{
			Field: aggField("v"),
			Func:  seq.AggFuncMax,
		}))
	expectedBuckets := []seq.AggregationBucket{
		{Name: "", Value: 300, NotExists: 0},
	}
	s.AssertAggregation(searchParams, seq.AggregateArgs{Func: seq.AggFuncMax}, expectedBuckets)
}

func (s *FractionTestSuite) TestAggNotExists() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"not_exists"}`,
	}

	s.insertDocuments(docs...)

	searchParams := s.query(
		`service:"not_exists"`,
		withAggQuery(processor.AggQuery{
			Field: aggField("v"),
			Func:  seq.AggFuncAvg,
		}))
	expectedBuckets := []seq.AggregationBucket{
		{Name: "", Value: math.NaN(), NotExists: 1},
	}
	s.AssertAggregation(searchParams, seq.AggregateArgs{Func: seq.AggFuncAvg}, expectedBuckets)
}

func (s *FractionTestSuite) TestAggAvgWithoutGroupBy() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","v":200,"service":"avg_without_group_by"}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","v":500,"service":"avg_without_group_by"}`,
	}

	s.insertDocuments(docs...)

	searchParams := s.query(
		`service:"avg_without_group_by"`,
		withAggQuery(processor.AggQuery{
			Field: aggField("v"),
			Func:  seq.AggFuncAvg,
		}))
	expectedBuckets := []seq.AggregationBucket{
		{Name: "", Value: 350, NotExists: 0},
	}
	s.AssertAggregation(searchParams, seq.AggregateArgs{Func: seq.AggFuncAvg}, expectedBuckets)
}

func (s *FractionTestSuite) TestFractionInfo() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:25Z","service":"service_a","message":"first message some text", "service":"gateway"}`,
		`{"timestamp":"2000-01-01T13:00:32Z","service":"service_b","message":"second message other text", "service":"kube-proxy"}`,
		`{"timestamp":"2000-01-01T13:00:43Z","service":"service_c","message":"third message other text", "service":"gateway"}`,
		`{"timestamp":"2000-01-01T13:00:53Z","service":"service_a","message":"fourth message some text", "service":"kube-proxy"}`,
		`{"timestamp":"2000-01-01T13:00:54Z","service":"service_c","message":"apple","service":"kube-scheduler"}`,
	}

	s.insertDocuments(docs...)

	info := s.fraction.Info()

	// these checks should not break without a reason
	// but if compression/marshalling has changed, expected values can be updated accordingly
	s.Require().Equal(uint32(5), info.DocsTotal, "doc total doesn't match")
	s.Require().True(info.DocsOnDisk > uint64(230) && info.DocsOnDisk < uint64(240),
		"doc raw doesn't match. actual value: %d", info.DocsOnDisk)
	s.Require().Equal(uint64(573), info.DocsRaw, "doc raw doesn't match")
	s.Require().Equal(seq.MID(946731625000), info.From, "from doesn't match")
	s.Require().Equal(seq.MID(946731654000), info.To, "from doesn't match")

	switch s.fraction.(type) {
	case *Active:
		s.Require().True(info.MetaOnDisk >= uint64(320) && info.MetaOnDisk <= uint64(350),
			"meta on disk doesn't match. actual value: %d", info.MetaOnDisk)
		s.Require().Equal(uint64(0), info.IndexOnDisk, "index on disk doesn't match")
	case *Sealed:
		s.Require().Equal(uint64(0), info.MetaOnDisk, "meta on disk doesn't match. actual value")
		s.Require().True(info.IndexOnDisk > uint64(1450) && info.IndexOnDisk < uint64(1500),
			"index on disk doesn't match. actual value: %d", info.MetaOnDisk)
	default:
		s.Require().Fail("unsupported fraction type")
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

func withHist(histInterval uint64) searchOption {
	return func(p *processor.SearchParams) error {
		p.HistInterval = histInterval
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

func (s *FractionTestSuite) AssertSearchIgnoreTotal(query string, originalDocs []string, expectedIndexes []int) {
	s.AssertSearchWithSearchParams(s.query(query), originalDocs, expectedIndexes, false)
}

func (s *FractionTestSuite) AssertSearch(queryObject interface{}, originalDocs []string, expectedIndexes []int) {
	switch q := queryObject.(type) {
	case string:
		s.AssertSearchWithSearchParams(s.query(q), originalDocs, expectedIndexes, true)
	case *processor.SearchParams:
		s.AssertSearchWithSearchParams(q, originalDocs, expectedIndexes, true)
	default:
		s.Require().Fail("type for query object not supported")
	}
}

func (s *FractionTestSuite) AssertSearchWithSearchParams(
	params *processor.SearchParams,
	originalDocs []string,
	expectedIndexes []int,
	checkTotal bool) {

	var withTotals = []bool{false}

	// We can check total only if limit is not set. Otherwise, total returns a count
	// of all docs which match the query
	if checkTotal && params.Limit == math.MaxInt32 {
		withTotals = append(withTotals, true)
	}

	var sortOrders = []seq.DocsOrder{seq.DocsOrderDesc}
	if params.Limit == math.MaxInt32 {
		sortOrders = append(sortOrders, seq.DocsOrderAsc)
	}

	for _, order := range sortOrders {
		for _, withTotal := range withTotals {
			params.Order = order
			params.WithTotal = withTotal

			qpr, err := s.fraction.Search(context.Background(), *params)
			s.Require().NoError(err, "search failed for query with order=%v", order)

			if withTotal {
				s.Require().Equal(uint64(len(expectedIndexes)), qpr.Total, "qpr.total doesn't match")
			} else {
				s.Require().Equal(uint64(0), qpr.Total, "qpr has total but not expected to have")
			}

			s.Require().Equal(len(expectedIndexes), qpr.IDs.Len(), "doc count doesn't match")

			docs, err := s.fraction.Fetch(context.Background(), qpr.IDs.IDs())
			s.Require().NoError(err, "failed to fetch docs")

			if order.IsReverse() {
				slices.Reverse(docs)
			}

			fetchedDocs := make([]string, 0, len(docs))
			for _, doc := range docs {
				fetchedDocs = append(fetchedDocs, string(doc))
			}

			for i, fetchedDoc := range fetchedDocs {
				if i < len(expectedIndexes) {
					expectedDoc := originalDocs[expectedIndexes[i]]
					s.Require().Equal(expectedDoc, fetchedDoc, "doc at index %d doesn't match", i)
				}
			}
		}
	}
}

func (s *FractionTestSuite) AssertAggregation(
	searchParams *processor.SearchParams,
	aggregate seq.AggregateArgs,
	expectedBuckets []seq.AggregationBucket) {

	qpr, err := s.fraction.Search(context.Background(), *searchParams)
	s.Require().NoError(err, "search failed")

	aggResults := qpr.Aggregate([]seq.AggregateArgs{aggregate})
	s.Require().Equal(1, len(aggResults))
	s.Require().Equal(len(expectedBuckets), len(aggResults[0].Buckets), "bucket count doesn't match")

	for _, expectedBucket := range expectedBuckets {
		found := false
		for _, gotBucket := range aggResults[0].Buckets {
			if gotBucket.Name == expectedBucket.Name {
				if math.IsNaN(expectedBucket.Value) || math.IsNaN(gotBucket.Value) {
					s.Require().Truef(math.IsNaN(expectedBucket.Value) && math.IsNaN(gotBucket.Value),
						"wrong value for bucket %s: expected NaN=%v, got NaN=%v",
						expectedBucket.Name, math.IsNaN(expectedBucket.Value), math.IsNaN(gotBucket.Value))
				} else {
					s.Require().Equal(expectedBucket.Value, gotBucket.Value, "wrong value for bucket %s", expectedBucket.Name)
				}
				s.Require().Equal(expectedBucket.NotExists, gotBucket.NotExists, "wrong NotExists for bucket %s", expectedBucket.Name)
				found = true
				break
			}
		}
		s.Require().True(found, "bucket %s not found in results", expectedBucket.Name)
	}
}

func (s *FractionTestSuite) AssertHist(
	searchParams *processor.SearchParams,
	expectedHist map[string]uint64) {

	qpr, err := s.fraction.Search(context.Background(), *searchParams)
	s.Require().NoError(err, "search failed")
	s.Require().Equal(len(expectedHist), len(qpr.Histogram), "histogram count doesn't match")

	for ts, expectedCount := range expectedHist {
		timestamp, err := time.Parse(time.RFC3339, ts)
		s.Require().NoError(err, "timestamp parsing failed")
		expectedMID := seq.TimeToMID(timestamp)

		actualCount, ok := qpr.Histogram[expectedMID]
		if ok {
			s.Require().Equal(expectedCount, actualCount, "count at bucket %s doesn't match", ts)
		} else {
			s.Require().Fail("bucket not found", "bucket %s was not found in qpr.hist", ts)
		}
	}
}

func (s *FractionTestSuite) newActive(docs ...string) *Active {
	baseName := filepath.Join(s.tmpDir, "test_fraction")
	activeIndexer := NewActiveIndexer(4, 10)
	activeIndexer.Start()

	active := NewActive(
		baseName,
		activeIndexer,
		storage.NewReadLimiter(1, nil),
		newSmallCache[[]byte](),
		newSmallCache[[]byte](),
		s.config,
	)

	proc := indexer.NewProcessor(s.mapping, s.tokenizers, 0, 0, 0)

	idx := 0
	readNext := func() ([]byte, error) {
		if idx >= len(docs) {
			return nil, nil
		}
		d := []byte(docs[idx])
		idx++
		return d, nil
	}

	_, binaryDocs, binaryMeta, err := proc.ProcessBulk(time.Now(), nil, nil, readNext)
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
	return active
}

func (s *FractionTestSuite) newSealed(docs ...string) *Sealed {
	active := s.newActive(docs...)

	sealParams := common.SealParams{
		IDsZstdLevel:           1, // min comression level
		LIDsZstdLevel:          1,
		TokenListZstdLevel:     1,
		DocsPositionsZstdLevel: 1,
		TokenTableZstdLevel:    1,
		DocBlocksZstdLevel:     1,
		DocBlockSize:           128 * int(units.KiB),
	}

	activeSealingSource, err := NewActiveSealingSource(active, sealParams)
	s.Require().NoError(err, "Sealing source creation failed")

	preloaded, err := sealing.Seal(activeSealingSource, sealParams)
	s.Require().NoError(err, "Sealing failed")

	indexCache := &IndexCache{
		MIDs:       newSmallCache[[]byte](),
		RIDs:       newSmallCache[[]byte](),
		Params:     newSmallCache[seqids.BlockParams](),
		LIDs:       newSmallCache[*lids.Block](),
		Tokens:     newSmallCache[*token.Block](),
		TokenTable: newSmallCache[token.Table](),
		Registry:   newSmallCache[[]byte](),
	}

	sealed := NewSealedPreloaded(
		active.BaseFileName,
		preloaded,
		storage.NewReadLimiter(1, nil),
		indexCache,
		newSmallCache[[]byte](),
		s.config,
	)
	active.Release()
	return sealed
}

/*
ActiveFractionTestSuite run tests for active fraction
*/
type ActiveFractionTestSuite struct {
	FractionTestSuite
}

func (s *ActiveFractionTestSuite) SetupTest() {
	s.SetupTestCommon()

	s.insertDocuments = func(docs ...string) {
		if s.fraction != nil {
			s.Require().Fail("can insert docs only once")
		}
		s.fraction = s.newActive(docs...)
	}
}

func (s *ActiveFractionTestSuite) TearDownTest() {
	if s.fraction != nil {
		active, ok := s.fraction.(*Active)
		if ok {
			active.Release()
		} else {
			s.Require().Fail("fraction is not of Active type")
		}
		s.fraction.Suicide()
		s.fraction = nil
	}

	s.TearDownTestCommon()
}

/*
SealedFractionTestSuite run tests for sealed fraction. Active fraction is created first and then sealed.
*/
type SealedFractionTestSuite struct {
	FractionTestSuite
}

func (s *SealedFractionTestSuite) SetupTest() {
	s.SetupTestCommon()

	s.insertDocuments = func(docs ...string) {
		if s.fraction != nil {
			s.Require().Fail("can insert docs only once")
		}
		s.fraction = s.newSealed(docs...)
	}
}

func (s *SealedFractionTestSuite) TearDownTest() {
	if s.fraction != nil {
		s.fraction.Suicide()
		s.fraction = nil
	}
	s.TearDownTestCommon()
}

/*
SealedLoadedFractionTestSuite run tests for sealed fraction. Active fraction is created first and then sealed.
Sealed fraction is then loaded with sealed.NewSealed call
*/
type SealedLoadedFractionTestSuite struct {
	FractionTestSuite
}

func (s *SealedLoadedFractionTestSuite) SetupTest() {
	s.SetupTestCommon()

	s.insertDocuments = func(docs ...string) {
		if s.fraction != nil {
			s.Require().Fail("can insert docs only once")
		}
		s.fraction = s.newSealedLoaded(docs...)
	}
}

func (s *SealedLoadedFractionTestSuite) newSealedLoaded(docs ...string) *Sealed {
	sealed := s.newSealed(docs...)
	sealed.close("closed")

	indexCache := &IndexCache{
		MIDs:       newSmallCache[[]byte](),
		RIDs:       newSmallCache[[]byte](),
		Params:     newSmallCache[seqids.BlockParams](),
		LIDs:       newSmallCache[*lids.Block](),
		Tokens:     newSmallCache[*token.Block](),
		TokenTable: newSmallCache[token.Table](),
		Registry:   newSmallCache[[]byte](),
	}

	sealed = NewSealed(
		sealed.BaseFileName,
		storage.NewReadLimiter(1, nil),
		indexCache,
		newSmallCache[[]byte](),
		nil,
		s.config)
	s.fraction = sealed
	return sealed
}

func (s *SealedLoadedFractionTestSuite) TearDownTest() {
	if s.fraction != nil {
		s.fraction.Suicide()
		s.fraction = nil
	}
	s.TearDownTestCommon()
}

func TestFractionSuites(t *testing.T) {
	suite.Run(t, new(ActiveFractionTestSuite))
	suite.Run(t, new(SealedFractionTestSuite))
	suite.Run(t, new(SealedLoadedFractionTestSuite))
}
