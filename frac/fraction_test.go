package frac

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/suite"

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
	"github.com/ozontech/seq-db/storage/s3"
	"github.com/ozontech/seq-db/tokenizer"
)

type FractionTestSuite struct {
	suite.Suite
	tmpDir        string
	config        *Config
	mapping       seq.Mapping
	tokenizers    map[seq.TokenizerType]tokenizer.Tokenizer
	activeIndexer *ActiveIndexer
	stopIndexer   func()
	sealParams    common.SealParams

	fraction Fraction

	insertDocuments func(docs ...[]string)
}

func (s *FractionTestSuite) SetupSuiteCommon() {
	s.activeIndexer, s.stopIndexer = NewActiveIndexer(4, 10)
}

func (s *FractionTestSuite) TearDownSuiteCommon() {
	s.stopIndexer()
}

func (s *FractionTestSuite) SetupTestCommon() {
	s.config = &Config{}
	s.tokenizers = map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeKeyword: tokenizer.NewKeywordTokenizer(20, false, true),
		seq.TokenizerTypeText:    tokenizer.NewTextTokenizer(20, false, true, 100),
		seq.TokenizerTypePath:    tokenizer.NewPathTokenizer(512, false, true),
	}
	s.mapping = seq.Mapping{
		"id":            seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"k8s_pod":       seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"k8s_namespace": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"k8s_container": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"message":       seq.NewSingleType(seq.TokenizerTypeText, "", 0),
		"level":         seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"client_ip":     seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"service":       seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"pod":           seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"status":        seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"source":        seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"trace_id":      seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"request_uri":   seq.NewSingleType(seq.TokenizerTypePath, "", 0),
		"spans":         seq.NewSingleType(seq.TokenizerTypeNested, "", 0),
		"spans.span_id": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"v":             seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
	}
	s.sealParams = common.SealParams{
		IDsZstdLevel:           1,
		LIDsZstdLevel:          1,
		TokenListZstdLevel:     1,
		DocsPositionsZstdLevel: 1,
		TokenTableZstdLevel:    1,
		DocBlocksZstdLevel:     1,
		DocBlockSize:           128 * int(units.KiB),
	}

	var err error
	s.tmpDir, err = os.MkdirTemp(os.TempDir(), "fraction_test_*")
	s.Require().NoError(err)
}

func (s *FractionTestSuite) TearDownTestCommon() {
	if s.fraction != nil {
		s.fraction = nil
	}
	err := os.RemoveAll(s.tmpDir)
	s.NoError(err, "Failed to remove tmp dir")
}

func (s *FractionTestSuite) TestSearchKeyword() {
	docs := []string{
		/*0*/ `{"timestamp":"2000-01-01T13:00:25Z","service":"service_a","message":"first message some text","trace_id":"abcdef","source":"prod01","level":"1"}`,
		/*1*/ `{"timestamp":"2000-01-01T13:00:32Z","service":"service_b","message":"second message other text","trace_id":"abcdef","source":"prod01","level":"1"}`,
		/*2*/ `{"timestamp":"2000-01-01T13:00:43Z","service":"service_c","message":"third message other text","trace_id":"aaaaaa","source":"prod02","level":"2"}`,
		/*3*/ `{"timestamp":"2000-01-01T13:00:53Z","service":"service_a","message":"fourth message some text","trace_id":"bbbbbb","source":"prod01","level":"1"}`,
		/*4*/ `{"timestamp":"2000-01-01T13:00:54Z","service":"service_c","message":"apple","source":"prod03"}`,
	}

	s.insertDocuments(docs)

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
		/*0*/ `{"timestamp":"2000-01-01T13:00:25Z","message":"bad","level":"1","service":"srv_1","status":"ok"}`,
		/*1*/ `{"timestamp":"2000-01-01T13:00:26Z","message":"good","level":"2","service":"srv_2","status":"ok"}`,
		/*2*/ `{"timestamp":"2000-01-01T13:00:27Z","message":"bad","level":"3","service":"srv_3","status":"ok"}`,
		/*3*/ `{"timestamp":"2000-01-01T13:00:28Z","message":"good","level":"4","service":"srv_4","status":"ok"}`,
		/*4*/ `{"timestamp":"2000-01-01T13:00:29Z","message":"bad","level":"5","service":"srv_5","status":"ok"}`,
		/*5*/ `{"timestamp":"2000-01-01T13:00:30Z","message":"good","level":"6","service":"srv_6","status":"ok"}`,
	}

	s.insertDocuments(docs)

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
		/*0*/ `{"timestamp":"2000-01-01T13:00:00.000Z","message":"apple","level":"info","service":"svc_a","status":"ok"}`,
		/*1*/ `{"timestamp":"2000-01-01T13:00:00.001Z","message":"apple","level":"error","service":"svc_b","status":"fail"}`,
		/*2*/ `{"timestamp":"2000-01-01T13:00:00.002Z","message":"banana","level":"info","service":"svc_a","status":"ok"}`,
		/*3*/ `{"timestamp":"2000-01-01T13:00:00.003Z","message":"banana","level":"error","service":"svc_b","status":"fail"}`,
		/*4*/ `{"timestamp":"2000-01-01T13:00:00.004Z","message":"cherry","level":"info","service":"svc_c","status":"ok"}`,
		/*5*/ `{"timestamp":"2000-01-01T13:00:00.005Z","message":"cherry","level":"warn","service":"svc_c","status":"ok"}`,
	}

	s.insertDocuments(docs)

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
		/*0*/ `{"timestamp":"2000-01-01T13:00:00.010Z","message":"first value:****"}`,
		/*1*/ `{"timestamp":"2000-01-01T13:00:00.020Z","message":"second value:*******"}`,
		/*2*/ `{"timestamp":"2000-01-01T13:00:00.030Z","message":"third value****"}`,
		/*3*/ `{"timestamp":"2000-01-01T13:00:00.040Z","message":"fourth ****"}`,
	}

	s.insertDocuments(docs)

	s.AssertSearch(`message:*`, docs, []int{3, 2, 1, 0})
	s.AssertSearch(`message:value`, docs, []int{1, 0})
	s.AssertSearch(`message:value*`, docs, []int{2, 1, 0})
	s.AssertSearch(`message:"value\*"`, docs, []int{})
	s.AssertSearch(`message:"value\**"`, docs, []int{2})
	s.AssertSearch(`message:"*\**"`, docs, []int{3, 2, 1, 0})
	s.AssertSearch(`message:"*e\**"`, docs, []int{2})
	s.AssertSearch(`message:"\**"`, docs, []int{3, 1, 0})
	s.AssertSearch(`message:"\*\*\*\*"`, docs, []int{3, 0})
	s.AssertSearch(`message:"\*\*\*\**"`, docs, []int{3, 1, 0})
	s.AssertSearch(`message:value* AND message:"\*\**"`, docs, []int{1, 0})
	s.AssertSearch(`message:value* OR message:"\*\**"`, docs, []int{3, 2, 1, 0})
}

func (s *FractionTestSuite) TestSearchFullText() {
	docs := []string{
		/*0*/ `{"timestamp":"2000-01-01T13:00:30Z","message":"first test document","level":"info","service":"test-service","status":"ok"}`,
		/*1*/ `{"timestamp":"2000-01-01T13:00:31Z","message":"second test document","level":"error","service":"test-service","status":"fail"}`,
		/*2*/ `{"timestamp":"2000-01-01T13:00:32Z","message":"third test document","level":"debug","service":"another-service","status":"ok"}`,
		/*3*/ `{"timestamp":"2000-01-01T13:00:33Z","message":"fourth test document","level":"info","service":"another-service","status":"ok"}`,
	}

	s.insertDocuments(docs)

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
		/*0*/ `{"timestamp":"2000-01-01T13:00:00.000Z","service":"a","request_uri":"/one"}`,
		/*1*/ `{"timestamp":"2000-01-01T13:00:00.001Z","service":"a","request_uri":"/one/two"}`,
		/*2*/ `{"timestamp":"2000-01-01T13:00:00.002Z","service":"a","request_uri":"/one/two/three"}`,
		/*3*/ `{"timestamp":"2000-01-01T13:00:00.003Z","service":"a","request_uri":"/one/two.three/four"}`,
		/*4*/ `{"timestamp":"2000-01-01T13:00:00.004Z","service":"a","request_uri":"/one/two.three/five"}`,
		/*5*/ `{"timestamp":"2000-01-01T13:00:00.005Z","service":"a","request_uri":"/one/two/three/"}`,
		/*6*/ `{"timestamp":"2000-01-01T13:00:00.006Z","service":"a","request_uri":"/one/two/three/1"}`,
		/*7*/ `{"timestamp":"2000-01-01T13:00:00.007Z","service":"a","request_uri":"/one/two/three/2"}`,
		/*8*/ `{"timestamp":"2000-01-01T13:00:00.008Z","service":"a","request_uri":"/one/two/three/3/four/"}`,
		/*9*/ `{"timestamp":"2000-01-01T13:00:00.009Z","service":"a","request_uri":"/one/four/three/3/"}`,
		/*10*/ `{"timestamp":"2000-01-01T13:00:00.010Z","service":"a","request_uri":"/two/one/three/2"}`,
	}

	s.insertDocuments(docs)

	s.AssertSearch(`request_uri:"/one"`, docs, []int{9, 8, 7, 6, 5, 4, 3, 2, 1, 0})
	s.AssertSearch(`request_uri:"/two"`, docs, []int{10})
	s.AssertSearch(`request_uri:"/one/two"`, docs, []int{8, 7, 6, 5, 2, 1})
	s.AssertSearch(`request_uri:"/one/two/three"`, docs, []int{8, 7, 6, 5, 2})
	s.AssertSearch(`request_uri:"/one/two/three/1"`, docs, []int{6})
	s.AssertSearch(`request_uri:"/one/two.three"`, docs, []int{4, 3})
	s.AssertSearch(`request_uri:"/one/two.three/four"`, docs, []int{3})
	s.AssertSearch(`request_uri:"/one/*/three"`, docs, []int{9, 8, 7, 6, 5, 2})
	s.AssertSearch(`request_uri:"/two/*/three"`, docs, []int{10})
	s.AssertSearch(`request_uri:"*/three/"`, docs, []int{5})
	s.AssertSearch(`request_uri:"*/three"`, docs, []int{10, 9, 8, 7, 6, 5, 2})
}

func (s *FractionTestSuite) TestSearchRange() {
	docs := []string{
		/*0*/ `{"timestamp":"2000-01-01T13:00:00.000Z","service":"test-service","level":"1"}`,
		/*1*/ `{"timestamp":"2000-01-01T13:00:00.001Z","service":"test-service","level":"3"}`,
		/*2*/ `{"timestamp":"2000-01-01T13:00:00.002Z","service":"test-service","level":"7"}`,
		/*3*/ `{"timestamp":"2000-01-01T13:00:00.003Z","service":"test-service","level":"15"}`,
		/*4*/ `{"timestamp":"2000-01-01T13:00:00.004Z","service":"test-service","level":"31"}`,
		/*5*/ `{"timestamp":"2000-01-01T13:00:00.005Z","service":"test-service","level":"63"}`,
		/*6*/ `{"timestamp":"2000-01-01T13:00:00.006Z","service":"test-service","level":"127"}`,
	}

	s.insertDocuments(docs)

	s.AssertSearch("level:[1, 3]", docs, []int{1, 0})
	s.AssertSearch("level:[1 TO 3]", docs, []int{1, 0})
	s.AssertSearch("level:[0, 63]", docs, []int{5, 4, 3, 2, 1, 0})
	s.AssertSearch("level:[-100, 100]", docs, []int{5, 4, 3, 2, 1, 0})
	s.AssertSearch("level:(0, 3]", docs, []int{1, 0})
	s.AssertSearch("level:(0 TO 3]", docs, []int{1, 0})

	s.AssertSearch("level:[0, *]", docs, []int{6, 5, 4, 3, 2, 1, 0})
	s.AssertSearch("level:[31, *]", docs, []int{6, 5, 4})
	s.AssertSearch("level:(31, *]", docs, []int{6, 5})

	s.AssertSearch("level:[200, 300]", docs, []int{})
	s.AssertSearch("level:(127, 200]", docs, []int{})
}

func (s *FractionTestSuite) TestSearchRe() {
	docs := []string{
		/*0*/ `{"timestamp":"2000-01-01T13:00:00.000Z", "k8s_pod": "foo-1", "v": "[ERROR] Oopsie!"}`,
		/*1*/ `{"timestamp":"2000-01-01T13:00:01.000Z", "k8s_pod": "foo-42", "v": "[INFO] Oopsie!"}`,
		/*2*/ `{"timestamp":"2000-01-01T13:00:02.000Z", "k8s_pod": "bar-1", "v": "[WARN] Oopsie!"}`,
		/*3*/ `{"timestamp":"2000-01-01T13:00:03.000Z", "k8s_pod": "bar-42", "v": "[INFO] Oopsie!"}`,
		/*4*/ `{"timestamp":"2000-01-01T13:00:04.000Z", "k8s_pod": "baz-1", "v": "[DEBUG] Oopsie!"}`,
		/*5*/ `{"timestamp":"2000-01-01T13:00:05.000Z", "k8s_pod": "baz-42","v": "[FATAL] Oopsie!"}`,
		/*6*/ `{"timestamp":"2000-01-01T13:00:06.000Z", "k8s_pod": "baz-42","v": "[FATAL]"}`,
	}

	s.insertDocuments(docs)

	s.AssertSearch(`k8s_pod:re("^(foo|bar)-[\d]+$")`, docs, []int{3, 2, 1, 0})
	s.AssertSearch(`k8s_pod:re("^ba[a-z]-[\d]{1}$")`, docs, []int{4, 2})
	s.AssertSearch(`v:re("\[(ERROR|FATAL)\].*")`, docs, []int{6, 5, 0})
	s.AssertSearch(`v:re("^\[(ERROR|FATAL)\]$")`, docs, []int{6})
	// In tests we transform keyword token to lower-case.
	// So case-sensitive expression will always yield nothing.
	s.AssertSearch(`v:re("(?-i)^\[(ERROR|FATAL)\]$")`, docs, []int{})
}

func (s *FractionTestSuite) TestSearchIPRange() {
	docs := []string{
		/*0*/ `{"timestamp":"2000-01-01T13:00:00.000Z","service":"gateway-0","level":"1","client_ip":"192.168.31.0"}`,
		/*1*/ `{"timestamp":"2000-01-01T13:00:01.000Z","service":"gateway-1","level":"1","client_ip":"192.168.0.1"}`,
		/*2*/ `{"timestamp":"2000-01-01T13:00:02.000Z","service":"gateway-2","level":"1","client_ip":"192.168.0.2"}`,
		/*3*/ `{"timestamp":"2000-01-01T13:00:03.000Z","service":"gateway-3","level":"1","client_ip":"192.168.0.3"}`,
		/*4*/ `{"timestamp":"2000-01-01T13:00:04.000Z","service":"gateway-0","level":"1","client_ip":"192.168.1.0"}`,
		/*5*/ `{"timestamp":"2000-01-01T13:00:05.000Z","service":"gateway-1","level":"1","client_ip":"192.168.1.1"}`,
		/*6*/ `{"timestamp":"2000-01-01T13:00:06.000Z","service":"gateway-0","level":"1","client_ip":"192.168.1.2"}`,
		/*7*/ `{"timestamp":"2000-01-01T13:00:07.000Z","service":"gateway-1","level":"1","client_ip":"192.168.1.255"}`,
		/*8*/ `{"timestamp":"2000-01-01T13:00:08.000Z","service":"gateway-3","level":"1","client_ip":"192.168.31.0"}`,
		/*9*/ `{"timestamp":"2000-01-01T13:00:09.000Z","service":"api-0","level":"2","client_ip":"172.10.0.1"}`,
		/*10*/ `{"timestamp":"2000-01-01T13:00:10.000Z","service":"api-1","level":"2","client_ip":"172.10.0.100"}`,
		/*11*/ `{"timestamp":"2000-01-01T13:00:11.000Z","service":"api-2","level":"2","client_ip":"172.10.1.50"}`,
		/*12*/ `{"timestamp":"2000-01-01T13:00:12.000Z","service":"api-3","level":"2","client_ip":"172.10.1.200"}`,
		/*13*/ `{"timestamp":"2000-01-01T13:00:13.000Z","service":"api-4","level":"2","client_ip":"172.10.2.1"}`,
		/*14*/ `{"timestamp":"2000-01-01T13:00:14.000Z","service":"backend-0","level":"3","client_ip":"10.53.0.10"}`,
		/*15*/ `{"timestamp":"2000-01-01T13:00:15.000Z","service":"backend-1","level":"3","client_ip":"10.53.0.20"}`,
		/*16*/ `{"timestamp":"2000-01-01T13:00:16.000Z","service":"backend-2","level":"3","client_ip":"10.53.1.30"}`,
		/*17*/ `{"timestamp":"2000-01-01T13:00:17.000Z","service":"backend-3","level":"3","client_ip":"10.53.1.40"}`,
		/*18*/ `{"timestamp":"2000-01-01T13:00:18.000Z","service":"backend-4","level":"3","client_ip":"10.53.2.50"}`,
	}

	s.insertDocuments(docs)

	s.AssertSearch("client_ip:ip_range(192.168.0.0,192.168.0.255)", docs, []int{3, 2, 1})
	s.AssertSearch("client_ip:ip_range(192.168.1.0,192.168.1.255)", docs, []int{7, 6, 5, 4})
	s.AssertSearch("client_ip:ip_range(172.10.0.0,172.10.0.255)", docs, []int{10, 9})
	s.AssertSearch("client_ip:ip_range(172.10.0.0,172.10.255.255)", docs, []int{13, 12, 11, 10, 9})
	s.AssertSearch("client_ip:ip_range(10.53.0.0,10.53.0.255)", docs, []int{15, 14})
	s.AssertSearch("client_ip:ip_range(10.53.0.0,10.53.255.255)", docs, []int{18, 17, 16, 15, 14})

	s.AssertSearch("client_ip:ip_range(192.168.0.0/24)", docs, []int{3, 2, 1})
	s.AssertSearch("client_ip:ip_range(192.168.1.0/24)", docs, []int{7, 6, 5, 4})
	s.AssertSearch("client_ip:ip_range(172.10.0.0/24)", docs, []int{10, 9})
	s.AssertSearch("client_ip:ip_range(10.53.0.0/24)", docs, []int{15, 14})

	s.AssertSearch("client_ip:ip_range(172.10.0.0/16)", docs, []int{13, 12, 11, 10, 9})
	s.AssertSearch("client_ip:ip_range(10.53.0.0/16)", docs, []int{18, 17, 16, 15, 14})

	s.AssertSearch("client_ip:ip_range(192.168.31.0/32)", docs, []int{8, 0})
	s.AssertSearch("client_ip:ip_range(172.10.0.1/32)", docs, []int{9})
}

func (s *FractionTestSuite) TestSearchIn() {
	docs := []string{
		/*0*/ `{"timestamp":"2000-01-01T13:00:00.000Z","message":"starting pod","level":"info","k8s_namespace":"prod","k8s_pod":"proxy-node1"}`,
		/*1*/ `{"timestamp":"2000-01-01T13:00:00.001Z","message":"api call failed","level":"error","k8s_namespace":"prod","k8s_pod":"apiserver-master1"}`,
		/*2*/ `{"timestamp":"2000-01-01T13:00:00.002Z","message":"scheduling task","level":"info","k8s_namespace":"test","k8s_pod":"scheduler-master1"}`,
		/*3*/ `{"timestamp":"2000-01-01T13:00:00.003Z","message":"authentication error","level":"error","k8s_namespace":"test","k8s_pod":"apiserver-master2"}`,
		/*4*/ `{"timestamp":"2000-01-01T13:00:00.004Z","message":"network policy applied","level":"info","k8s_namespace":"prod","k8s_pod":"proxy-node2"}`,
		/*5*/ `{"timestamp":"2000-01-01T13:00:00.005Z","message":"scheduling completed","level":"info","k8s_namespace":"staging","k8s_pod":"scheduler-master2"}`,
		/*6*/ `{"timestamp":"2000-01-01T13:00:00.006Z","message":"connection timeout","level":"error","k8s_namespace":"staging","k8s_pod":"app-backend-1"}`,
		/*7*/ `{"timestamp":"2000-01-01T13:00:00.007Z","message":"health check passed","level":"info","k8s_namespace":"prod","k8s_pod":"app-frontend-1"}`,
		/*8*/ `{"timestamp":"2000-01-01T13:00:00.008Z","message":"database query slow","level":"warn","k8s_namespace":"prod","k8s_pod":"app-backend-2"}`,
		/*9*/ `{"timestamp":"2000-01-01T13:00:00.009Z","message":"cache miss","level":"warn","k8s_namespace":"test","k8s_pod":"app-cache-1"}`,
	}

	s.insertDocuments(docs)

	s.AssertSearch("k8s_namespace:in(prod)", docs, []int{8, 7, 4, 1, 0})
	s.AssertSearch("k8s_namespace:in(test)", docs, []int{9, 3, 2})
	s.AssertSearch("k8s_namespace:in(staging)", docs, []int{6, 5})
	s.AssertSearch("k8s_namespace:in(prod,test)", docs, []int{9, 8, 7, 4, 3, 2, 1, 0})
	s.AssertSearch("k8s_namespace:in(prod,test,staging)", docs, []int{9, 8, 7, 6, 5, 4, 3, 2, 1, 0})

	s.AssertSearch("k8s_pod:in(proxy-*)", docs, []int{4, 0})
	s.AssertSearch("k8s_pod:in(apiserver-*)", docs, []int{3, 1})
	s.AssertSearch("k8s_pod:in(scheduler-*)", docs, []int{5, 2})
	s.AssertSearch("k8s_pod:in(proxy-*,apiserver-*)", docs, []int{4, 3, 1, 0})
	s.AssertSearch("k8s_pod:in(proxy-*,apiserver-*,scheduler-*)", docs, []int{5, 4, 3, 2, 1, 0})

	s.AssertSearch("level:error AND k8s_namespace:in(prod,test)", docs, []int{3, 1})
	s.AssertSearch("level:error AND k8s_namespace:in(prod,test) AND k8s_pod:in(apiserver-*)", docs, []int{3, 1})

	s.AssertSearch(
		`level:error AND k8s_namespace:in(prod,test) AND k8s_pod:in(proxy-*,apiserver-*,scheduler-*)`,
		docs,
		[]int{3, 1})
}

func (s *FractionTestSuite) TestSearchNested() {
	docs := []string{
		/*0*/ `{"timestamp":"2000-01-01T13:00:00.000Z","spans":[{"span_id":"1"},{"span_id":"2"}]}`,
		/*1*/ `{"timestamp":"2000-01-01T13:00:00.001Z","spans":[{"span_id":"2"},{"span_id":"3"}]}`,
		/*2*/ `{"timestamp":"2000-01-01T13:00:00.002Z","spans":[{"span_id":"1"},{"span_id":"3"}]}`,
		/*3*/ `{"timestamp":"2000-01-01T13:00:00.003Z","spans":[{"span_id":"4"},{"span_id":"5"}]}`,
	}

	s.insertDocuments(docs)

	s.AssertSearch("spans.span_id:*", docs, []int{3, 2, 1, 0})
	s.AssertSearch("spans.span_id:1", docs, []int{2, 0})
	s.AssertSearch("spans.span_id:2", docs, []int{1, 0})
	s.AssertSearch("spans.span_id:3", docs, []int{2, 1})
	s.AssertSearch("spans.span_id:4", docs, []int{3})
	s.AssertSearch("spans.span_id:5", docs, []int{3})
}

func (s *FractionTestSuite) TestSearchFromTo() {
	docs := []string{
		/*0*/ `{"timestamp":"2000-01-01T13:00:00.000Z","message":"bad","level":"1","trace_id":"0","service":"0"}`,
		/*1*/ `{"timestamp":"2000-01-01T13:00:00.001Z","message":"good","level":"2","trace_id":"0","service":"1"}`,
		/*2*/ `{"timestamp":"2000-01-01T13:00:00.002Z","message":"bad","level":"3","trace_id":"0","service":"2"}`,
		/*3*/ `{"timestamp":"2000-01-01T13:00:00.003Z","message":"good","level":"4","trace_id":"1","service":"0"}`,
		/*4*/ `{"timestamp":"2000-01-01T13:00:00.004Z","message":"bad","level":"5","trace_id":"1","service":"1"}`,
		/*5*/ `{"timestamp":"2000-01-01T13:00:00.005Z","message":"good","level":"6","trace_id":"1","service":"2"}`,
		/*6*/ `{"timestamp":"2000-01-01T13:00:00.006Z","message":"bad","level":"7","trace_id":"2","service":"0"}`,
		/*7*/ `{"timestamp":"2000-01-01T13:00:00.007Z","message":"good","level":"8","trace_id":"2","service":"1"}`,
	}

	s.insertDocuments(docs)

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

// TestSearchFromToNanoseconds tests if SearchParams "from" and "to" params can be specified up to nanoseconds since they are of seq.MID type.
// However, seq-db API doesn't support searching with queries with "from" and "to" specified in nanos. Only millis are supported.
func (s *FractionTestSuite) TestSearchFromToNanoseconds() {
	docs := []string{
		/*0*/ `{"timestamp":"2000-01-01T13:00:00.000000000Z","message":"bad","level":"1","trace_id":"0","service":"0"}`,
		/*1*/ `{"timestamp":"2000-01-01T13:00:00.000000001Z","message":"good","level":"2","trace_id":"0","service":"1"}`,
		/*2*/ `{"timestamp":"2000-01-01T13:00:00.000000002Z","message":"bad","level":"3","trace_id":"0","service":"2"}`,
		/*3*/ `{"timestamp":"2000-01-01T13:00:00.000000003Z","message":"good","level":"4","trace_id":"1","service":"0"}`,
		/*4*/ `{"timestamp":"2000-01-01T13:00:00.000000004Z","message":"bad","level":"5","trace_id":"1","service":"1"}`,
		/*5*/ `{"timestamp":"2000-01-01T13:00:00.000000005Z","message":"good","level":"6","trace_id":"1","service":"2"}`,
		/*6*/ `{"timestamp":"2000-01-01T13:00:00.000000006Z","message":"bad","level":"7","trace_id":"2","service":"0"}`,
		/*7*/ `{"timestamp":"2000-01-01T13:00:00.000000007Z","message":"good","level":"8","trace_id":"2","service":"1"}`,
	}

	s.insertDocuments(docs)

	assertSearch := func(query string, fromOffset, toOffset int, expectedIndexes []int) {
		s.AssertSearch(s.query(
			query,
			withFrom(fmt.Sprintf("2000-01-01T13:00:00.000000%03dZ", fromOffset)),
			withTo(fmt.Sprintf("2000-01-01T13:00:00.000000%03dZ", toOffset))),
			docs, expectedIndexes)
	}

	assertSearch(`message:good`, 0, 7, []int{7, 5, 3, 1})
	assertSearch(`message:bad`, 0, 7, []int{6, 4, 2, 0})
	assertSearch(`message:good`, 0, 6, []int{5, 3, 1})
	assertSearch(`message:bad`, 1, 7, []int{6, 4, 2})
}

func (s *FractionTestSuite) TestSearchWithLimit() {
	docs := []string{
		/*0*/ `{"timestamp":"2000-01-01T13:00:00.000Z","message":"bad","level":"1","trace_id":"0","service":"0"}`,
		/*1*/ `{"timestamp":"2000-01-01T13:00:00.001Z","message":"good","level":"2","trace_id":"0","service":"1"}`,
		/*2*/ `{"timestamp":"2000-01-01T13:00:00.002Z","message":"bad","level":"3","trace_id":"0","service":"2"}`,
		/*3*/ `{"timestamp":"2000-01-01T13:00:00.003Z","message":"good","level":"4","trace_id":"1","service":"0"}`,
		/*4*/ `{"timestamp":"2000-01-01T13:00:00.004Z","message":"bad","level":"5","trace_id":"1","service":"1"}`,
		/*5*/ `{"timestamp":"2000-01-01T13:00:00.005Z","message":"good","level":"6","trace_id":"1","service":"2"}`,
		/*6*/ `{"timestamp":"2000-01-01T13:00:00.006Z","message":"bad","level":"7","trace_id":"2","service":"0"}`,
		/*7*/ `{"timestamp":"2000-01-01T13:00:00.007Z","message":"good","level":"8","trace_id":"2","service":"1"}`,
	}

	s.insertDocuments(docs)

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

func (s *FractionTestSuite) TestSearchWithOffsetId() {
	docs := []string{
		`{"timestamp":"2000-01-01T12:59:59.999Z","message":"outsider1"}`,
		`{"timestamp":"2000-01-01T12:59:59.999Z","message":"outsider2"}`,
		`{"timestamp":"2000-01-01T13:00:00.000Z","message":"bad"}`,
		`{"timestamp":"2000-01-01T13:00:00.000Z","message":"good"}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","message":"bad"}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","message":"good"}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","message":"bad"}`,
		`{"timestamp":"2000-01-01T13:00:00.001Z","message":"good"}`,
		`{"timestamp":"2000-01-01T13:00:00.002Z","message":"bad"}`,
		`{"timestamp":"2000-01-01T13:00:00.002Z","message":"good"}`,
		`{"timestamp":"2000-01-01T13:00:00.002Z","message":"bad"}`,
		`{"timestamp":"2000-01-01T13:00:00.003Z","message":"good"}`,
		`{"timestamp":"2000-01-01T13:00:00.003Z","message":"bad"}`,
		`{"timestamp":"2000-01-01T13:00:00.004Z","message":"ugly"}`,
		`{"timestamp":"2000-01-01T13:00:00.004Z","message":"ugly"}`,
	}

	s.insertDocuments(docs)

	// validate that we can page through fraction using offset id in both orders.
	// every message must appear exactly once. some docs have same MID

	for _, order := range []seq.DocsOrder{seq.DocsOrderDesc, seq.DocsOrderAsc} {
		searchParams := s.query("message:*",
			withFrom("2000-01-01T13:00:00.000Z"),
			withTo("2000-01-01T13:00:00.003Z"),
			withLimit(2))
		searchParams.Order = order

		ids := make(map[seq.ID]bool)

		for {
			qpr, err := s.fraction.Search(context.Background(), *searchParams)
			s.Require().NoError(err, "search failed")
			if len(qpr.IDs) == 0 {
				break
			}

			qprIDs := qpr.IDs.IDs()
			for _, id := range qprIDs {
				ids[id] = true
			}
			// switch to the next page
			searchParams.OffsetId = qprIDs[len(qprIDs)-1]
		}

		s.Require().Equal(11, len(ids), "duplicate IDs found")
	}
}

func (s *FractionTestSuite) TestSearchWithTotal() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:01.549Z","message": "apple banana smoothie"}`,
		`{"timestamp":"2000-01-01T13:00:02.690Z","message": "fruit salad"}`,
		`{"timestamp":"2000-01-01T13:00:03.102Z","message": "banana pineapple smoothie"}`,
		`{"timestamp":"2000-01-01T13:00:03.052Z","message": "apple juice"}`,
		`{"timestamp":"2000-01-01T13:00:04.999Z","message": "banana"}`,
		`{"timestamp":"2000-01-01T13:00:05.000Z","message": "apple juice"}`,
		`{"timestamp":"2000-01-01T13:00:10.777Z","message": "apple banana"}`,
		`{"timestamp":"2000-01-01T13:00:15.100Z","message": "cherry pie"}`,
		`{"timestamp":"2000-01-01T13:00:15.200Z","message": "apple tart"}`,
		`{"timestamp":"2000-01-01T13:00:15.300Z","message": "bread crisp"}`,
		`{"timestamp":"2000-01-01T13:00:20.500Z","message": "orange juice"}`,
		`{"timestamp":"2000-01-01T13:00:25.600Z","message": "apple cider"}`,
	}

	s.insertDocuments(docs)

	qpr, err := s.fraction.Search(context.Background(), *s.query("message:apple", withLimit(3), withTotal()))
	s.Require().NoError(err, "search failed")
	s.Require().Equal(uint64(6), qpr.Total)
	s.Require().Equal(3, qpr.IDs.Len())

	qpr, err = s.fraction.Search(context.Background(), *s.query("message:*", withLimit(4), withTotal()))
	s.Require().NoError(err, "search failed")
	s.Require().Equal(uint64(12), qpr.Total)
	s.Require().Equal(4, qpr.IDs.Len())
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

	s.insertDocuments(docs)

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

	s.insertDocuments(docs)

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
			{gateway: 3, proxy: 2, scheduler: 1},
		})
	assertAggSearch(
		s.query(
			"message:good",
			withAggQuery(processor.AggQuery{GroupBy: aggField("service")})),
		[]map[string]uint64{
			{gateway: 2, proxy: 1},
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
			{gateway: 3, proxy: 2, scheduler: 1},
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

	s.insertDocuments(docs)

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

func (s *FractionTestSuite) TestAggSumTimeSeries() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"sum1","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"sum1","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"sum1","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"sum2","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"sum2","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"sum3","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"sum4","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"sum4","v":1}`,
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"sum4"}`,
		`{"timestamp":"2000-01-01T13:00:00.000Z","service":"sum5","v":1}`,
	}

	s.insertDocuments(docs)

	searchParams := s.query(
		"service:sum*",
		withAggQuery(processor.AggQuery{
			Field:    aggField("v"),
			GroupBy:  aggField("service"),
			Func:     seq.AggFuncSum,
			Interval: 1000,
		}))
	expectedBuckets := []seq.AggregationBucket{
		// all NotExists go to a dedicated bucket with MID=0 in time series mode
		{Name: "sum4", MID: seq.MID(0), Value: math.NaN(), NotExists: 1},
		{Name: "sum4", MID: seq.TimeToMID(mustParseTime("2000-01-01T13:00:00.000Z")), Value: 2, NotExists: 0},
		{Name: "sum1", MID: seq.TimeToMID(mustParseTime("2000-01-01T13:00:00.000Z")), Value: 3, NotExists: 0},
		{Name: "sum3", MID: seq.TimeToMID(mustParseTime("2000-01-01T13:00:00.000Z")), Value: 1, NotExists: 0},
		{Name: "sum5", MID: seq.TimeToMID(mustParseTime("2000-01-01T13:00:00.000Z")), Value: 1, NotExists: 0},
		{Name: "sum2", MID: seq.TimeToMID(mustParseTime("2000-01-01T13:00:00.000Z")), Value: 2, NotExists: 0},
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

	s.insertDocuments(docs)

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

	s.insertDocuments(docs)

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

	s.insertDocuments(docs)

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

	s.insertDocuments(docs)

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

	s.insertDocuments(docs)

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

	s.insertDocuments(docs)

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

	s.insertDocuments(docs)

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

	s.insertDocuments(docs)

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

func (s *FractionTestSuite) TestAggUniqueCountTimeSeries() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:01.000Z","service":"service1","level":1}`,
		`{"timestamp":"2000-01-01T13:00:01.000Z","service":"service1","level":2}`,
		`{"timestamp":"2000-01-01T13:00:01.000Z","service":"service1","level":3}`,
		`{"timestamp":"2000-01-01T13:00:01.000Z","service":"service2","level":1}`,
		`{"timestamp":"2000-01-01T13:00:01.000Z","service":"service2","level":2}`,
		`{"timestamp":"2000-01-01T13:00:01.000Z","service":"service3","level":1}`,
		`{"timestamp":"2000-01-01T13:00:01.000Z","service":"service4","level":1}`,
		`{"timestamp":"2000-01-01T13:00:01.000Z","service":"service4","level":1}`,
		`{"timestamp":"2000-01-01T13:00:01.000Z","service":"service4","level":2}`,
		`{"timestamp":"2000-01-01T13:00:01.000Z","service":"service4"}`,
	}

	s.insertDocuments(docs)
	searchParams := s.query(
		"service:service*",
		withAggQuery(processor.AggQuery{
			Field:    aggField("level"),
			GroupBy:  aggField("service"),
			Func:     seq.AggFuncUniqueCount,
			Interval: 1000,
		}))
	expectedBuckets := []seq.AggregationBucket{
		{Name: "service1", MID: seq.TimeToMID(mustParseTime("2000-01-01T13:00:01.000Z")), Value: 3, NotExists: 0},
		{Name: "service2", MID: seq.TimeToMID(mustParseTime("2000-01-01T13:00:01.000Z")), Value: 2, NotExists: 0},
		{Name: "service3", MID: seq.TimeToMID(mustParseTime("2000-01-01T13:00:01.000Z")), Value: 1, NotExists: 0},
		{Name: "service4", MID: seq.TimeToMID(mustParseTime("2000-01-01T13:00:01.000Z")), Value: 2, NotExists: 0},
		{Name: "service4", MID: seq.MID(0), Value: math.NaN(), NotExists: 1},
	}
	s.AssertAggregation(searchParams, seq.AggregateArgs{Func: seq.AggFuncUniqueCount}, expectedBuckets)
}

func (s *FractionTestSuite) TestAggUniqueCount() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.002Z","service":"service1","level":1}`,
		`{"timestamp":"2000-01-01T13:00:00.003Z","service":"service1","level":2}`,
		`{"timestamp":"2000-01-01T13:00:00.007Z","service":"service1","level":3}`,
		`{"timestamp":"2000-01-01T13:00:00.009Z","service":"service2","level":1}`,
		`{"timestamp":"2000-01-01T13:00:00.010Z","service":"service2","level":2}`,
		`{"timestamp":"2000-01-01T13:00:00.011Z","service":"service3","level":1}`,
		`{"timestamp":"2000-01-01T13:00:00.012Z","service":"service4","level":1}`,
		`{"timestamp":"2000-01-01T13:00:00.013Z","service":"service4","level":1}`,
		`{"timestamp":"2000-01-01T13:00:00.017Z","service":"service4","level":2}`,
		`{"timestamp":"2000-01-01T13:00:00.017Z","service":"service4"}`,
	}

	s.insertDocuments(docs)
	searchParams := s.query(
		"service:service*",
		withAggQuery(processor.AggQuery{
			Field:   aggField("level"),
			GroupBy: aggField("service"),
			Func:    seq.AggFuncUniqueCount,
		}))
	expectedBuckets := []seq.AggregationBucket{
		{Name: "service1", Value: 3, NotExists: 0},
		{Name: "service2", Value: 2, NotExists: 0},
		{Name: "service3", Value: 1, NotExists: 0},
		{Name: "service4", Value: 2, NotExists: 1},
	}
	s.AssertAggregation(searchParams, seq.AggregateArgs{Func: seq.AggFuncUniqueCount}, expectedBuckets)
}

func (s *FractionTestSuite) TestSearchMultipleBulks() {
	docs := []string{
		/*0*/ `{"timestamp":"2000-01-01T13:00:01Z","service":"service_a","message":"request started","source":"prod01","level":"1"}`,
		/*1*/ `{"timestamp":"2000-01-01T13:00:02Z","service":"service_b","message":"processing data","source":"prod03","level":"1"}`,
		/*2*/ `{"timestamp":"2000-01-01T13:00:03Z","service":"service_c","message":"database query","source":"prod02","level":"2"}`,
		/*3*/ `{"timestamp":"2000-01-01T13:00:04Z","service":"service_a","message":"request completed","source":"prod01","level":"1"}`,
		/*4*/ `{"timestamp":"2000-01-01T13:00:05Z","service":"service_c","message":"cache hit","source":"prod03","level":"3"}`,
		/*5*/ `{"timestamp":"2000-01-01T13:00:06Z","service":"service_c","message":"processing request","source":"prod01","level":"2"}`,
		/*6*/ `{"timestamp":"2000-01-01T13:00:07Z","service":"service_a","message":"request failed","source":"prod02","level":"1"}`,
		/*7*/ `{"timestamp":"2000-01-01T13:00:08Z","service":"service_b","message":"processing failed","source":"prod03","level":"4"}`,
		/*8*/ `{"timestamp":"2000-01-01T13:00:09Z","service":"service_b","message":"processing retry","source":"prod03","level":"3"}`,
	}
	var bulk1 []string
	var bulk2 []string
	var bulk3 []string
	docs = append(docs, bulk1...)
	docs = append(docs, bulk2...)
	docs = append(docs, bulk3...)
	for i, doc := range docs {
		switch i % 3 {
		case 0:
			bulk1 = append(bulk1, doc)
		case 1:
			bulk2 = append(bulk2, doc)
		case 2:
			bulk3 = append(bulk3, doc)
		}
	}

	s.insertDocuments(bulk1, bulk2, bulk3)

	s.AssertSearch(s.query("service:service_b"), docs, []int{8, 7, 1})
	s.AssertSearch(s.query("source:prod01"), docs, []int{5, 3, 0})
	s.AssertSearch(s.query("level:4"), docs, []int{7})
	s.AssertSearch(s.query("message:request"), docs, []int{6, 5, 3, 0})
}

// This test checks search on a large frac. Doc count is set to 25000 which results in ~200 kbyte docs file (3 doc blocks)
func (s *FractionTestSuite) TestSearchLargeFrac() {
	testDocs, bulks, fromTime, toTime := generatesMessages(25000, 1000)
	midTime := fromTime.Add(time.Duration(len(testDocs)/2) * time.Millisecond)

	s.insertDocuments(bulks...)

	docJsons := make([]string, len(testDocs))
	for i, td := range testDocs {
		docJsons[i] = td.json
	}

	type docFilter func(doc *testDoc) bool

	searchTestCases := []struct {
		name     string
		query    string
		filter   docFilter
		fromTime time.Time
		toTime   time.Time
		limit    int
	}{
		{
			name:     "message:request",
			query:    "message:request",
			filter:   func(doc *testDoc) bool { return strings.Contains(doc.message, "request") },
			fromTime: fromTime,
			toTime:   toTime,
		},
		{
			name:     "message:request (time range)",
			query:    "message:request",
			filter:   func(doc *testDoc) bool { return strings.Contains(doc.message, "request") },
			fromTime: fromTime,
			toTime:   midTime,
		},
		{
			name:     "message:request (time range + limit)",
			query:    "message:request",
			filter:   func(doc *testDoc) bool { return strings.Contains(doc.message, "request") },
			fromTime: fromTime,
			toTime:   midTime,
			limit:    100,
		},
		{
			name:     "service:bus",
			query:    "service:bus",
			filter:   func(doc *testDoc) bool { return doc.service == "bus" },
			fromTime: fromTime,
			toTime:   toTime,
		},
		{
			name:     "NOT service:bus",
			query:    "NOT service:bus",
			filter:   func(doc *testDoc) bool { return doc.service != bus },
			fromTime: fromTime,
			toTime:   toTime,
		},
		{
			name:     "NOT service:bus (time range)",
			query:    "NOT service:bus",
			filter:   func(doc *testDoc) bool { return doc.service != bus },
			fromTime: fromTime,
			toTime:   midTime,
		},
		{
			name:     "service:proxy (time range)",
			query:    "service:proxy",
			filter:   func(doc *testDoc) bool { return doc.service == proxy },
			fromTime: fromTime,
			toTime:   midTime,
		},
		{
			name:     "service:scheduler (time range + limit)",
			query:    "service:scheduler",
			filter:   func(doc *testDoc) bool { return doc.service == scheduler },
			fromTime: fromTime,
			toTime:   midTime,
			limit:    100,
		},
		{
			name:     "level:5",
			query:    "level:5",
			filter:   func(doc *testDoc) bool { return doc.level == 5 },
			fromTime: fromTime,
			toTime:   toTime,
		},
		{
			name:     "level:228",
			query:    "level:228",
			filter:   func(doc *testDoc) bool { return false }, // no such data, just validate than frac returns empty IDs
			fromTime: fromTime,
			toTime:   toTime,
		},
		{
			name:     "level:5 (time range)",
			query:    "level:5",
			filter:   func(doc *testDoc) bool { return doc.level == 5 },
			fromTime: fromTime,
			toTime:   midTime,
		},
		{
			name:     "trace_id:trace-777",
			query:    "trace_id:trace-777",
			filter:   func(doc *testDoc) bool { return doc.traceId == "trace-777" },
			fromTime: fromTime,
			toTime:   toTime,
		},
		{
			name:     "trace_id:trace-100 (time range)",
			query:    "trace_id:trace-100",
			filter:   func(doc *testDoc) bool { return doc.traceId == "trace-100" },
			fromTime: fromTime,
			toTime:   midTime,
		},
		{
			name:     "NOT trace_id:trace-4999",
			query:    "NOT trace_id:trace-4999",
			filter:   func(doc *testDoc) bool { return doc.traceId != "trace-4999" },
			fromTime: fromTime,
			toTime:   toTime,
		},
		{
			name:     "trace_id:trace-4999",
			query:    "trace_id:trace-4999",
			filter:   func(doc *testDoc) bool { return doc.traceId == "trace-4999" },
			fromTime: fromTime,
			toTime:   toTime,
		},
		{
			name:     "trace_id:trace-2025 (time range)",
			query:    "trace_id:trace-2025",
			filter:   func(doc *testDoc) bool { return doc.traceId == "trace-2025" },
			fromTime: fromTime,
			toTime:   midTime,
		},
		// AND operator queries
		{
			name:  "message:request AND message:failed",
			query: "message:request AND message:failed",
			filter: func(doc *testDoc) bool {
				return strings.Contains(doc.message, "request") && strings.Contains(doc.message, "failed")
			},
			fromTime: fromTime,
			toTime:   toTime,
		},
		{
			name:  "service:gateway AND message:processing AND message:retry AND level:5",
			query: "service:gateway AND message:processing AND message:retry AND level:5",
			filter: func(doc *testDoc) bool {
				return doc.service == gateway && strings.Contains(doc.message, "processing") &&
					strings.Contains(doc.message, "retry") && doc.level == 5
			},
			fromTime: fromTime,
			toTime:   toTime,
		},
		// OR operator queries
		{
			name:  "trace_id OR",
			query: "trace_id:trace-1000 OR trace_id:trace-1500 OR trace_id:trace-2000 OR trace_id:trace-2500 OR trace_id:trace-3000",
			filter: func(doc *testDoc) bool {
				return doc.traceId == "trace-1000" ||
					doc.traceId == "trace-1500" ||
					doc.traceId == "trace-2000" ||
					doc.traceId == "trace-2500" ||
					doc.traceId == "trace-3000"
			},
			fromTime: fromTime,
			toTime:   toTime,
		},

		// mixed AND/OR/NOT
		{
			name:  "message:request AND (level:1 OR level:3 OR level:5) AND trace_id:trace-2*",
			query: "message:request AND (level:1 OR level:3 OR level:5) AND trace_id:trace-2*",
			filter: func(doc *testDoc) bool {
				return strings.Contains(doc.message, "request") && (doc.level == 1 || doc.level == 3 || doc.level == 5) &&
					strings.Contains(doc.traceId, "trace-2")
			},
			fromTime: fromTime,
			toTime:   toTime,
		},
		{
			name: "complex AND+OR",
			query: "(service:gateway OR service:proxy OR service:scheduler) AND " +
				"(message:request OR message:failed) AND level:[1 to 3]",
			filter: func(doc *testDoc) bool {
				return (doc.service == gateway || doc.service == proxy || doc.service == "scheduler") &&
					(strings.Contains(doc.message, "request") || strings.Contains(doc.message, "failed")) &&
					(doc.level >= 1 && doc.level <= 3)
			},
			fromTime: fromTime,
			toTime:   toTime,
		},
		{
			name:  "service:gateway AND NOT (message:request OR message:timed OR level:[0 to 3])",
			query: "service:gateway AND NOT (message:request OR message:timed OR level:[0 to 3])",
			filter: func(doc *testDoc) bool {
				return doc.service == gateway &&
					!(strings.Contains(doc.message, "request") ||
						strings.Contains(doc.message, "timed") ||
						(doc.level >= 0 && doc.level <= 3))
			},
			fromTime: fromTime,
			toTime:   midTime,
		},
		{
			name:  "service:proxy AND NOT level:5 AND NOT pod:pod-2* AND NOT client_ip:ip_range(192.168.19.0,192.168.19.255)",
			query: "service:proxy AND NOT level:5 AND NOT pod:pod-2* AND NOT client_ip:ip_range(192.168.19.0,192.168.19.255)",
			filter: func(doc *testDoc) bool {
				return doc.service == proxy &&
					doc.level != 5 &&
					!strings.Contains(doc.pod, "pod-2") &&
					!strings.Contains(doc.clientIp, "192.168.19")
			},
			fromTime: fromTime,
			toTime:   midTime,
		},

		// other queries
		{
			name:     "trace_id:trace-4*",
			query:    "trace_id:trace-4*",
			filter:   func(doc *testDoc) bool { return strings.Contains(doc.traceId, "trace-4") },
			fromTime: fromTime,
			toTime:   toTime,
		},
	}

	for _, tc := range searchTestCases {
		s.Run(tc.name, func() {
			var expectedIndexes []int
			for i := len(testDocs) - 1; i >= 0; i-- {
				doc := testDocs[i]

				if doc.timestamp.Before(tc.fromTime) {
					continue
				}
				if doc.timestamp.After(tc.toTime) {
					continue
				}

				if tc.filter(doc) {
					expectedIndexes = append(expectedIndexes, i)
					if tc.limit > 0 && len(expectedIndexes) >= tc.limit {
						break
					}
				}
			}

			var options []searchOption
			options = append(options, withFrom(tc.fromTime.Format(time.RFC3339Nano)), withTo(tc.toTime.Format(time.RFC3339Nano)))
			if tc.limit > 0 {
				options = append(options, withLimit(tc.limit))
			}

			s.AssertSearch(s.query(tc.query, options...), docJsons, expectedIndexes)
		})
	}

	s.Run("service:kafka | group by pod unique_count(client_ip)", func() {
		// Check both sort orders simply for aggTree to be iterated in a different order
		orders := []seq.DocsOrder{seq.DocsOrderDesc, seq.DocsOrderAsc}

		for _, ord := range orders {
			ips := make(map[string]map[string]struct{})
			for _, doc := range testDocs {
				if doc.service != kafka {
					continue
				}
				if ips[doc.pod] == nil {
					ips[doc.pod] = make(map[string]struct{})
				}

				ips[doc.pod][doc.clientIp] = struct{}{}
			}

			var expectedBuckets []seq.AggregationBucket
			for pod, podIps := range ips {
				expectedBuckets = append(expectedBuckets, seq.AggregationBucket{
					Name:      pod,
					Value:     float64(len(podIps)),
					NotExists: 0,
				})
			}

			searchParams := s.query(
				"service:kafka",
				withTo(toTime.Format(time.RFC3339Nano)),
				withAggQuery(processor.AggQuery{
					Field:   aggField("client_ip"),
					GroupBy: aggField("pod"),
					Func:    seq.AggFuncUniqueCount,
				}))
			searchParams.Order = ord

			s.AssertAggregation(searchParams, seq.AggregateArgs{Func: seq.AggFuncUniqueCount}, expectedBuckets)
		}
	})

	s.Run("service:scheduler | group by pod avg(level)", func() {
		// Check both sort orders simply for aggTree to be iterated in a different order
		orders := []seq.DocsOrder{seq.DocsOrderDesc, seq.DocsOrderAsc}

		for _, ord := range orders {
			levelsByPod := make(map[string][]int)
			for _, doc := range testDocs {
				if doc.service != "scheduler" {
					continue
				}

				levelsByPod[doc.pod] = append(levelsByPod[doc.pod], doc.level)
			}

			var expectedBuckets []seq.AggregationBucket
			for pod, levels := range levelsByPod {
				sum := 0
				for _, level := range levels {
					sum += level
				}
				avg := float64(sum) / float64(len(levels))
				expectedBuckets = append(expectedBuckets, seq.AggregationBucket{
					Name:      pod,
					Value:     avg,
					NotExists: 0,
				})
			}

			searchParams := s.query(
				"service:scheduler",
				withTo(toTime.Format(time.RFC3339Nano)),
				withAggQuery(processor.AggQuery{
					Field:   aggField("level"),
					GroupBy: aggField("pod"),
					Func:    seq.AggFuncAvg,
				}))
			searchParams.Order = ord

			s.AssertAggregation(searchParams, seq.AggregateArgs{Func: seq.AggFuncAvg}, expectedBuckets)
		}
	})

	// Test large QPR with 25000 groups (all ids are unique)
	s.Run("_exists_:service | group by id count()", func() {
		countById := make(map[string]int)
		for _, doc := range testDocs {
			countById[doc.id]++
		}

		var expectedBuckets []seq.AggregationBucket
		for id, cnt := range countById {
			expectedBuckets = append(expectedBuckets, seq.AggregationBucket{
				Name:      id,
				Value:     float64(cnt),
				NotExists: 0,
			})
		}

		searchParams := s.query(
			"_exists_:service",
			withTo(toTime.Format(time.RFC3339Nano)),
			withAggQuery(processor.AggQuery{
				GroupBy: aggField("id"),
				Func:    seq.AggFuncCount,
			}))

		s.AssertAggregation(searchParams, seq.AggregateArgs{Func: seq.AggFuncCount}, expectedBuckets)
	})

	s.Run("NOT message:retry | group by service avg(level)", func() {
		levelsByService := make(map[string][]int)
		for _, doc := range testDocs {
			// our query for agg will be `NOT message:retry`
			if strings.Contains(doc.message, "retry") {
				continue
			}

			levelsByService[doc.service] = append(levelsByService[doc.service], doc.level)
		}

		var expectedBuckets []seq.AggregationBucket
		for service, levels := range levelsByService {
			sum := 0
			for _, level := range levels {
				sum += level
			}
			avg := float64(sum) / float64(len(levels))
			expectedBuckets = append(expectedBuckets, seq.AggregationBucket{
				Name:      service,
				Value:     avg,
				NotExists: 0,
			})
		}

		searchParams := s.query(
			"NOT message:retry",
			withTo(toTime.Format(time.RFC3339Nano)),
			withAggQuery(processor.AggQuery{
				Field:   aggField("level"),
				GroupBy: aggField("service"),
				Func:    seq.AggFuncAvg,
			}))

		s.AssertAggregation(searchParams, seq.AggregateArgs{Func: seq.AggFuncAvg}, expectedBuckets)
	})

	s.Run("service:database AND level:3 | hist 1s", func() {
		// Check both sort orders simply for lid tree to be iterated in a different order
		orders := []seq.DocsOrder{seq.DocsOrderDesc, seq.DocsOrderAsc}

		for _, ord := range orders {
			histBuckets := make(map[string]uint64)
			for _, doc := range testDocs {
				if doc.service == "database" && doc.level == 3 {
					bucketTime := doc.timestamp.Truncate(time.Second)
					bucketKey := bucketTime.Format(time.RFC3339Nano)
					histBuckets[bucketKey]++
				}
			}

			searchParams := s.query(
				"service:database AND level:3",
				withTo(toTime.Format(time.RFC3339Nano)),
				withHist(1000))
			searchParams.Order = ord

			s.AssertHist(searchParams, histBuckets)
		}
	})

	s.Run("scroll with offset id", func() {
		query := "message:request AND level:4"
		scrollFrom := fromTime
		scrollTo := midTime
		pageSize := 98

		var expectedIndexesAsc []int
		for i := range testDocs {
			doc := testDocs[i]
			if !doc.timestamp.Before(scrollFrom) &&
				!doc.timestamp.After(scrollTo) &&
				strings.Contains(doc.message, "request") &&
				doc.level == 4 {
				expectedIndexesAsc = append(expectedIndexesAsc, i)
			}
		}

		var expectedIndexes []int
		for _, order := range []seq.DocsOrder{seq.DocsOrderDesc, seq.DocsOrderAsc} {
			if order == seq.DocsOrderAsc {
				expectedIndexes = expectedIndexesAsc
			} else {
				expectedIndexes = append([]int{}, expectedIndexesAsc...)
				slices.Reverse(expectedIndexes)
			}

			searchParams := s.query(query,
				withFrom(scrollFrom.Format(time.RFC3339Nano)),
				withTo(scrollTo.Format(time.RFC3339Nano)),
				withLimit(pageSize))
			searchParams.Order = order

			expectedOffset := 0
			totalIDsScrolled := 0

			for {
				qpr, err := s.fraction.Search(context.Background(), *searchParams)

				s.Require().NoError(err, "search failed")

				if len(qpr.IDs) == 0 {
					break
				}

				qprIDs := qpr.IDs.IDs()
				totalIDsScrolled += len(qprIDs)

				docs, err := s.fraction.Fetch(context.Background(), qprIDs)
				s.Require().NoError(err, "fetch failed for order=%v", order)

				for j, doc := range docs {
					idx := expectedOffset + j
					s.Require().Equalf(docJsons[expectedIndexes[idx]], string(doc),
						"doc at scroll position %d (order=%v) doesn't match", idx, order)
				}
				expectedOffset += len(docs)

				searchParams.OffsetId = qprIDs[len(qprIDs)-1]
			}

			s.Require().Equal(totalIDsScrolled, len(expectedIndexesAsc), "total number of docs scrolled mismatch")
		}
	})
}

func (s *FractionTestSuite) TestIntersectingNanoseconds() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000000000Z","message":"bad","level":"1"}`,
		`{"timestamp":"2000-01-01T13:00:00.000000001Z","message":"good","level":"2"}`,
		`{"timestamp":"2000-01-01T13:00:00.000000002Z","message":"ok","level":"1"}`,
		`{"timestamp":"2000-01-01T13:00:00.000000003Z","message":"err","level":"2"}`,
		`{"timestamp":"2000-01-01T13:00:00.000000004Z","message":"success","level":"3"}`,
		`{"timestamp":"2000-01-01T13:00:00.001000000Z","message":"err","level":"2"}`,
		`{"timestamp":"2000-01-01T13:00:00.001000001Z","message":"bad","level":"1"}`,
		`{"timestamp":"2000-01-01T13:00:00.001000002Z","message":"good","level":"2"}`,
		`{"timestamp":"2000-01-01T13:00:00.002000000Z","message":"bad","level":"1"}`,
		`{"timestamp":"2000-01-01T13:00:00.002000000Z","message":"err","level":"1"}`,
	}

	s.insertDocuments(docs)

	s.Require().Equal(uint64(946731600000000000), uint64(s.fraction.Info().From))
	s.Require().Equal(uint64(946731600002000000), uint64(s.fraction.Info().To))

	s.Require().True(s.fraction.IsIntersecting(
		seq.TimeToMID(mustParseTime("2000-01-01T12:59:59.000000000Z")),
		seq.TimeToMID(mustParseTime("2000-01-01T13:00:00.000000000Z"))),
		"must intersect at info.From")
	// 1 ns before the fraction range. Should not overlap, since MID distribution is not built for fractions with short lifetime,
	// and it only covers the last 24h from now
	s.Require().False(s.fraction.IsIntersecting(
		seq.TimeToMID(mustParseTime("2000-01-01T12:59:59.000000000Z")),
		seq.TimeToMID(mustParseTime("2000-01-01T12:59:59.999999999Z"))),
		"must not overlap (outside of range)")
	// overlaps at the only point at info.To
	s.Require().True(s.fraction.IsIntersecting(
		seq.TimeToMID(mustParseTime("2000-01-01T13:00:00.002000000Z")),
		seq.TimeToMID(mustParseTime("2000-01-01T13:00:00.999999999Z"))),
		"must intersect at info.To")
	s.Require().False(s.fraction.IsIntersecting(
		seq.TimeToMID(mustParseTime("2000-01-01T13:00:00.002000001Z")),
		seq.TimeToMID(mustParseTime("2000-01-01T13:00:00.999999999Z"))),
		"must not intersect (1 ns outside of range)")
	s.Require().True(s.fraction.IsIntersecting(
		seq.TimeToMID(mustParseTime("2000-01-01T12:59:59.999999999Z")),
		seq.TimeToMID(mustParseTime("2000-01-01T13:00:00.000000001Z"))),
		"must intersect due to overlapping")
	s.Require().True(s.fraction.IsIntersecting(
		seq.TimeToMID(mustParseTime("2000-01-01T13:00:00.001000000Z")),
		seq.TimeToMID(mustParseTime("2000-01-01T13:00:00.999999999Z"))),
		"must intersect due to overlapping")

	// double check for seq.MID built from raw nanoseconds
	s.Require().True(s.fraction.IsIntersecting(seq.MID(946731500000000000), seq.MID(946731600000000000)))
	s.Require().True(s.fraction.IsIntersecting(seq.MID(946731600002000000), seq.MID(946731699999999999)))
}

func (s *FractionTestSuite) TestContainsWithMIDDistribution() {
	now := time.Now().Truncate(time.Minute)
	docs := []string{
		fmt.Sprintf(`{"timestamp":%q,"message":"apple juice"}`, now.Add(-60*time.Minute).Format(time.RFC3339Nano)),
		fmt.Sprintf(`{"timestamp":%q,"message":"orange juice"}`, now.Add(-61*time.Minute).Format(time.RFC3339Nano)),
		fmt.Sprintf(`{"timestamp":%q,"message":"cider"}`, now.Add(-65*time.Minute).Format(time.RFC3339Nano)),
		fmt.Sprintf(`{"timestamp":%q,"message":"wine"}`, now.Add(-123*time.Minute).Format(time.RFC3339Nano)),
		fmt.Sprintf(`{"timestamp":%q,"message":"cola"}`, now.Add(-365*time.Minute).Format(time.RFC3339Nano)),
		fmt.Sprintf(`{"timestamp":%q,"message":"cola"}`, now.Add(-30*time.Hour).Format(time.RFC3339Nano)),
	}

	s.insertDocuments(docs)

	s.Require().True(s.fraction.Contains(seq.TimeToMID(now.Add(-60 * time.Minute))))
	s.Require().True(s.fraction.Contains(seq.TimeToMID(now.Add(-61 * time.Minute))))
	s.Require().True(s.fraction.Contains(seq.TimeToMID(now.Add(-123 * time.Minute))))
	// also true, MID distribution bucket is 1 minute
	s.Require().True(s.fraction.Contains(seq.TimeToMID(now.Add(-60 * time.Minute).Add(-30 * time.Second))))
	// contains=true: outside MID distribution but within from-to range
	s.Require().True(s.fraction.Contains(seq.TimeToMID(now.Add(-27 * time.Hour))))
	s.Require().True(s.fraction.Contains(seq.TimeToMID(now.Add(-30 * time.Hour))))
	// contains=false: outside MID distribution AND outside from-to range
	s.Require().False(s.fraction.Contains(seq.TimeToMID(now.Add(-30 * time.Hour).Add(-1 * time.Minute))))
}

func (s *FractionTestSuite) TestContainsNanoseconds() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:00.000000000Z","message":"bad","level":"1"}`,
		`{"timestamp":"2000-01-01T13:00:00.000000001Z","message":"good","level":"2"}`,
		`{"timestamp":"2000-01-01T13:00:00.000000004Z","message":"success","level":"3"}`,
		`{"timestamp":"2000-01-01T13:10:00.000000000Z","message":"err","level":"2"}`,
		`{"timestamp":"2000-01-01T13:20:00.000000001Z","message":"bad","level":"1"}`,
		`{"timestamp":"2000-01-01T13:30:00.000000002Z","message":"good","level":"2"}`,
		`{"timestamp":"2000-01-01T13:40:00.000000001Z","message":"bad","level":"1"}`,
		`{"timestamp":"2000-01-01T13:50:00.000000002Z","message":"err","level":"1"}`,
	}

	s.insertDocuments(docs)

	s.Require().True(s.fraction.Contains(seq.TimeToMID(mustParseTime("2000-01-01T13:00:00.000000000Z"))), "frac must contain first doc")
	s.Require().True(s.fraction.Contains(seq.TimeToMID(mustParseTime("2000-01-01T13:00:00.000000001Z"))), "frac must contain second doc")
	s.Require().True(s.fraction.Contains(seq.TimeToMID(mustParseTime("2000-01-01T13:10:00.000000000Z"))), "frac must contain third doc")
	s.Require().True(s.fraction.Contains(seq.TimeToMID(mustParseTime("2000-01-01T13:50:00.000000002Z"))), "frac must contain last doc")

	s.Require().True(s.fraction.Contains(seq.TimeToMID(mustParseTime("2000-01-01T13:30:00.000000002Z"))), "frac must contain sixth doc")
	// round doc nano to milli, still Contains returns true
	s.Require().True(s.fraction.Contains(seq.TimeToMID(mustParseTime("2000-01-01T13:30:00.000000000Z"))), "frac must contain sixth doc (rounded to milli)")

	// still Contains returns true even though the timestamp is 5 minute far from nearest doc
	// MID distribution only covers the last 24h, so Contains return true here
	s.Require().True(s.fraction.Contains(seq.TimeToMID(mustParseTime("2000-01-01T13:15:00.000000000Z"))))
	s.Require().True(s.fraction.Contains(seq.TimeToMID(mustParseTime("2000-01-01T13:25:00.000000000Z"))))
}

func (s *FractionTestSuite) TestMIDDistribution() {
	now := time.Now().Truncate(time.Minute)
	docs := []string{
		fmt.Sprintf(`{"timestamp":%q,"message":"apple juice"}`, now.Add(-60*time.Minute).Format(time.RFC3339Nano)),
		fmt.Sprintf(`{"timestamp":%q,"message":"orange juice"}`, now.Add(-61*time.Minute).Format(time.RFC3339Nano)),
		fmt.Sprintf(`{"timestamp":%q,"message":"cider"}`, now.Add(-65*time.Minute).Format(time.RFC3339Nano)),
		fmt.Sprintf(`{"timestamp":%q,"message":"red wine"}`, now.Add(-120*time.Minute).Format(time.RFC3339Nano)),
		fmt.Sprintf(`{"timestamp":%q,"message":"coca cola"}`, now.Add(-360*time.Minute).Format(time.RFC3339Nano)),
	}

	s.insertDocuments(docs)

	_, ok := s.fraction.(*Active)
	if ok {
		s.Require().Nil(s.fraction.Info().Distribution, "active fraction has MID distribution")
		return
	}

	dist := s.fraction.Info().Distribution.GetDist()
	s.Require().Equal(5, len(dist))
	s.Require().Equal(now.Add(-360*time.Minute).UTC(), dist[0])
	s.Require().Equal(now.Add(-120*time.Minute).UTC(), dist[1])
	s.Require().Equal(now.Add(-65*time.Minute).UTC(), dist[2])
	s.Require().Equal(now.Add(-61*time.Minute).UTC(), dist[3])
	s.Require().Equal(now.Add(-60*time.Minute).UTC(), dist[4])
}

func (s *FractionTestSuite) TestFractionInfo() {
	docs := []string{
		`{"timestamp":"2000-01-01T13:00:25Z","service":"service_a","message":"first message some text", "container":"gateway"}`,
		`{"timestamp":"2000-01-01T13:00:32Z","service":"service_b","message":"second message other text", "container":"kube-proxy"}`,
		`{"timestamp":"2000-01-01T13:00:43Z","service":"service_c","message":"third message other text", "container":"gateway"}`,
		`{"timestamp":"2000-01-01T13:00:53Z","service":"service_a","message":"fourth message some text", "container":"kube-proxy"}`,
		`{"timestamp":"2000-01-01T13:00:54Z","service":"service_c","message":"apple","container":"kube-scheduler"}`,
	}

	s.insertDocuments(docs)

	info := s.fraction.Info()

	// these checks should not break without a reason
	// but if compression/marshalling has changed, expected values can be updated accordingly
	s.Require().Equal(uint32(5), info.DocsTotal, "doc total doesn't match")
	// it varies depending on params and docs shuffled
	s.Require().True(info.DocsOnDisk > uint64(200) && info.DocsOnDisk < uint64(350),
		"doc on disk doesn't match. actual value: %d", info.DocsOnDisk)
	s.Require().Equal(uint64(583), info.DocsRaw, "doc raw doesn't match")
	s.Require().Equal(seq.MID(946731625000000000), info.From, "from doesn't match")
	s.Require().Equal(seq.MID(946731654000000000), info.To, "to doesn't match")

	switch s.fraction.(type) {
	case *Active:
		s.Require().True(info.MetaOnDisk >= uint64(250) && info.MetaOnDisk <= uint64(400),
			"meta on disk doesn't match. actual value: %d", info.MetaOnDisk)
		s.Require().Equal(uint64(0), info.IndexOnDisk, "index on disk doesn't match")
	case *Sealed:
		s.Require().Equal(uint64(0), info.MetaOnDisk, "meta on disk doesn't match. actual value")
		s.Require().True(info.IndexOnDisk > uint64(1400) && info.IndexOnDisk < uint64(1600),
			"index on disk doesn't match. actual value: %d", info.IndexOnDisk)
	case *Remote:
		s.Require().Equal(uint64(0), info.MetaOnDisk, "meta on disk doesn't match. actual value")
		s.Require().True(info.IndexOnDisk > uint64(1400) && info.IndexOnDisk < uint64(1600),
			"index on disk doesn't match. actual value: %d", info.IndexOnDisk)
	default:
		s.Require().Fail("unsupported fraction type")
	}
}

type searchOption func(*processor.SearchParams) error

func (s *FractionTestSuite) query(queryString string, options ...searchOption) *processor.SearchParams {
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
		t, err := time.Parse(time.RFC3339Nano, from)
		if err != nil {
			return err
		}
		p.From = seq.TimeToMID(t)
		return nil
	}
}

func withTo(to string) searchOption {
	return func(p *processor.SearchParams) error {
		t, err := time.Parse(time.RFC3339Nano, to)
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

func withTotal() searchOption {
	return func(p *processor.SearchParams) error {
		p.WithTotal = true
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

func mustParseTime(timeStr string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, timeStr)
	if err != nil {
		panic(fmt.Sprintf("could not parse timestamp %s", timeStr))
	}
	return t
}

func (s *FractionTestSuite) AssertSearch(queryObject any, originalDocs []string, expectedIndexes []int) {
	switch q := queryObject.(type) {
	case string:
		s.AssertSearchWithSearchParams(s.query(q), originalDocs, expectedIndexes)
	case *processor.SearchParams:
		s.AssertSearchWithSearchParams(q, originalDocs, expectedIndexes)
	default:
		s.Require().Fail("type for query object not supported")
	}
}

func (s *FractionTestSuite) AssertSearchWithSearchParams(
	params *processor.SearchParams,
	originalDocs []string,
	expectedIndexes []int,
) {
	sortOrders := []seq.DocsOrder{params.Order}
	if params.Order == seq.DocsOrderDesc && params.Limit == math.MaxInt32 {
		sortOrders = append(sortOrders, seq.DocsOrderAsc)
	}

	for _, order := range sortOrders {
		params.Order = order

		qpr, err := s.fraction.Search(context.Background(), *params)
		s.Require().NoError(err, "search failed for query with order=%v", order)
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
			expectedDoc := originalDocs[expectedIndexes[i]]
			s.Require().Equal(expectedDoc, fetchedDoc, "doc at index %d doesn't match", i)
		}
	}
}

func (s *FractionTestSuite) AssertAggregation(
	searchParams *processor.SearchParams,
	aggregate seq.AggregateArgs,
	expectedBuckets []seq.AggregationBucket,
) {
	qpr, err := s.fraction.Search(context.Background(), *searchParams)
	s.Require().NoError(err, "search failed")

	aggResults := qpr.Aggregate([]seq.AggregateArgs{aggregate})
	s.Require().Equal(1, len(aggResults))
	s.Require().Equal(len(expectedBuckets), len(aggResults[0].Buckets), "bucket count doesn't match")

	for _, expectedBucket := range expectedBuckets {
		found := false
		for _, gotBucket := range aggResults[0].Buckets {
			if gotBucket.Name == expectedBucket.Name && gotBucket.MID == expectedBucket.MID {
				if math.IsNaN(expectedBucket.Value) || math.IsNaN(gotBucket.Value) {
					s.Require().Truef(math.IsNaN(expectedBucket.Value) && math.IsNaN(gotBucket.Value),
						"wrong value for bucket %s: expected NaN=%v, got NaN=%v",
						expectedBucket.Name, math.IsNaN(expectedBucket.Value), math.IsNaN(gotBucket.Value))
				} else {
					s.Require().Equal(expectedBucket.Value, gotBucket.Value, "wrong value for bucket %s-%s", expectedBucket.Name, expectedBucket.MID)
				}
				s.Require().Equal(expectedBucket.NotExists, gotBucket.NotExists, "wrong NotExists for bucket %s-%s", expectedBucket.Name, expectedBucket.MID)
				found = true
				break
			}
		}
		s.Require().True(found, "bucket %s-%s not found in results", expectedBucket.Name, expectedBucket.MID)
	}
}

func (s *FractionTestSuite) AssertHist(
	searchParams *processor.SearchParams,
	expectedHist map[string]uint64,
) {
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

func (s *FractionTestSuite) newActive(bulks ...[]string) *Active {
	baseName := filepath.Join(s.tmpDir, "test_fraction")
	active := NewActive(
		baseName,
		s.activeIndexer,
		storage.NewReadLimiter(1, nil),
		cache.NewCache[[]byte](nil, nil),
		cache.NewCache[[]byte](nil, nil),
		s.config,
	)

	var wg sync.WaitGroup

	for _, docs := range bulks {
		docsCopy := slices.Clone(docs)
		rand.Shuffle(len(docsCopy), func(i, j int) {
			docsCopy[i], docsCopy[j] = docsCopy[j], docsCopy[i]
		})

		idx := 0
		readNext := func() ([]byte, error) {
			if idx >= len(docsCopy) {
				return nil, nil
			}
			d := []byte(docsCopy[idx])
			idx++
			return d, nil
		}

		proc := indexer.NewProcessor(s.mapping, s.tokenizers, 0, 0, 0)
		compressor := indexer.GetDocsMetasCompressor(3, 3)
		_, binaryDocs, binaryMeta, err := proc.ProcessBulk(time.Now(), nil, nil, readNext)
		s.Require().NoError(err, "processing bulk failed")

		compressor.CompressDocsAndMetas(binaryDocs, binaryMeta)
		docsBlock, metasBlock := compressor.DocsMetas()

		wg.Add(1)
		err = active.Append(docsBlock, metasBlock, &wg)
		s.Require().NoError(err, "append to active failed")
	}
	wg.Wait()
	return active
}

func (s *FractionTestSuite) newSealed(bulks ...[]string) *Sealed {
	active := s.newActive(bulks...)

	activeSealingSource, err := NewActiveSealingSource(active, s.sealParams)
	s.Require().NoError(err, "Sealing source creation failed")

	preloaded, err := sealing.Seal(activeSealingSource, s.sealParams)
	s.Require().NoError(err, "Sealing failed")

	indexCache := &IndexCache{
		MIDs:       cache.NewCache[[]byte](nil, nil),
		RIDs:       cache.NewCache[seqids.BlockRIDs](nil, nil),
		Params:     cache.NewCache[seqids.BlockParams](nil, nil),
		LIDs:       cache.NewCache[*lids.Block](nil, nil),
		Tokens:     cache.NewCache[*token.Block](nil, nil),
		TokenTable: cache.NewCache[token.Table](nil, nil),
		InfoRegistry:    cache.NewCache[[]byte](nil, nil),
		TokenRegistry:   cache.NewCache[[]byte](nil, nil),
		OffsetsRegistry: cache.NewCache[[]byte](nil, nil),
		IDRegistry:      cache.NewCache[[]byte](nil, nil),
		LIDRegistry:     cache.NewCache[[]byte](nil, nil),
	}

	sealed := NewSealedPreloaded(
		active.BaseFileName,
		preloaded,
		storage.NewReadLimiter(1, nil),
		indexCache,
		cache.NewCache[[]byte](nil, nil),
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

func (s *ActiveFractionTestSuite) SetupSuite() {
	s.SetupSuiteCommon()
}

func (s *ActiveFractionTestSuite) SetupTest() {
	s.SetupTestCommon()

	s.insertDocuments = func(bulks ...[]string) {
		if s.fraction != nil {
			s.Require().Fail("can insert docs only once")
		}
		s.fraction = s.newActive(bulks...)
	}
}

func (s *ActiveFractionTestSuite) TearDownTest() {
	if active, ok := s.fraction.(*Active); ok {
		active.Release()
	} else {
		s.Require().Nil(s.fraction, "fraction is not of Active type")
	}

	s.TearDownTestCommon()
}

func (s *ActiveFractionTestSuite) TearDownSuite() {
	s.TearDownSuiteCommon()
}

/*
ActiveReplayedFractionTestSuite run tests for active fraction which was replayed from meta and docs file on disk
*/
type ActiveReplayedFractionTestSuite struct {
	FractionTestSuite
	originalFrac *Active
}

func (s *ActiveReplayedFractionTestSuite) SetupSuite() {
	s.SetupSuiteCommon()
}

func (s *ActiveReplayedFractionTestSuite) SetupTest() {
	s.SetupTestCommon()
	// Setting this flags allows to keep meta and docs files on disk after Active.Release() is called
	s.config.SkipSortDocs = true
	s.config.KeepMetaFile = true

	s.insertDocuments = func(bulks ...[]string) {
		if s.fraction != nil {
			s.Require().Fail("can insert docs only once")
		}
		s.fraction = s.Replay(s.newActive(bulks...))
	}
}

func (s *ActiveReplayedFractionTestSuite) Replay(frac *Active) Fraction {
	fracFileName := frac.BaseFileName
	s.originalFrac = frac
	replayedFrac := NewActive(
		fracFileName,
		s.activeIndexer,
		storage.NewReadLimiter(1, nil),
		cache.NewCache[[]byte](nil, nil),
		cache.NewCache[[]byte](nil, nil),
		&Config{})
	err := replayedFrac.Replay(context.Background())
	s.Require().NoError(err, "replay failed")
	return replayedFrac
}

func (s *ActiveReplayedFractionTestSuite) TearDownTest() {
	s.originalFrac.Release()
	if active, ok := s.fraction.(*Active); ok {
		active.Release()
	} else {
		s.Require().Nil(s.fraction, "fraction is not of Active type")
	}
	s.TearDownTestCommon()
}

func (s *ActiveReplayedFractionTestSuite) TearDownSuite() {
	s.TearDownSuiteCommon()
}

/*
SealedFractionTestSuite run tests for sealed fraction. Active fraction is created first and then sealed.
*/
type SealedFractionTestSuite struct {
	FractionTestSuite
}

func (s *SealedFractionTestSuite) SetupSuite() {
	s.SetupSuiteCommon()
}

func (s *SealedFractionTestSuite) SetupTest() {
	s.SetupTestCommon()

	s.insertDocuments = func(docs ...[]string) {
		if s.fraction != nil {
			s.Require().Fail("can insert docs only once")
		}
		s.fraction = s.newSealed(docs...)
	}
}

func (s *SealedFractionTestSuite) TearDownTest() {
	if sealed, ok := s.fraction.(*Sealed); ok {
		sealed.Release()
	} else {
		s.Require().Nil(s.fraction, "fraction is not of Sealed type")
	}
	s.TearDownTestCommon()
}

func (s *SealedFractionTestSuite) TearDownSuite() {
	s.TearDownSuiteCommon()
}

/*
SealedLoadedFractionTestSuite run tests for sealed fraction. Active fraction is created first and then sealed.
Sealed fraction is then loaded with sealed.NewSealed call
*/
type SealedLoadedFractionTestSuite struct {
	FractionTestSuite
}

func (s *SealedLoadedFractionTestSuite) SetupSuite() {
	s.SetupSuiteCommon()
}

func (s *SealedLoadedFractionTestSuite) SetupTest() {
	s.SetupTestCommon()

	s.insertDocuments = func(bulks ...[]string) {
		if s.fraction != nil {
			s.Require().Fail("can insert docs only once")
		}
		s.fraction = s.newSealedLoaded(bulks...)
	}
}

func (s *SealedLoadedFractionTestSuite) TearDownTest() {
	if sealed, ok := s.fraction.(*Sealed); ok {
		sealed.Release()
	} else {
		s.Require().Nil(s.fraction, "fraction is not of Sealed type")
	}
	s.TearDownTestCommon()
}

func (s *SealedLoadedFractionTestSuite) TearDownSuite() {
	s.TearDownSuiteCommon()
}

func (s *SealedLoadedFractionTestSuite) newSealedLoaded(bulks ...[]string) *Sealed {
	sealed := s.newSealed(bulks...)
	sealed.Release()

	indexCache := &IndexCache{
		MIDs:       cache.NewCache[[]byte](nil, nil),
		RIDs:       cache.NewCache[seqids.BlockRIDs](nil, nil),
		Params:     cache.NewCache[seqids.BlockParams](nil, nil),
		LIDs:       cache.NewCache[*lids.Block](nil, nil),
		Tokens:     cache.NewCache[*token.Block](nil, nil),
		TokenTable: cache.NewCache[token.Table](nil, nil),
		InfoRegistry:    cache.NewCache[[]byte](nil, nil),
		TokenRegistry:   cache.NewCache[[]byte](nil, nil),
		OffsetsRegistry: cache.NewCache[[]byte](nil, nil),
		IDRegistry:      cache.NewCache[[]byte](nil, nil),
		LIDRegistry:     cache.NewCache[[]byte](nil, nil),
	}

	sealed = NewSealed(
		sealed.BaseFileName,
		storage.NewReadLimiter(1, nil),
		indexCache,
		cache.NewCache[[]byte](nil, nil),
		nil,
		s.config)
	s.fraction = sealed
	return sealed
}

/*
RemoteFractionTestSuite runs tests for remote fraction. Fraction is first sealed, then uploaded
to fakes3 backend.
*/
type RemoteFractionTestSuite struct {
	FractionTestSuite

	s3Backend *s3mem.Backend
	s3server  *httptest.Server
}

func (s *RemoteFractionTestSuite) SetupSuite() {
	s.SetupSuiteCommon()

	s.s3Backend = s3mem.New()
	s.s3server = httptest.NewServer(gofakes3.New(s.s3Backend).Server())
}

func (s *RemoteFractionTestSuite) SetupTest() {
	s.SetupTestCommon()

	bucketName := fmt.Sprintf("bucket_%d_%d", time.Now().UnixMilli(), rand.Int())
	err := s.s3Backend.CreateBucket(bucketName)
	s.Require().NoError(err, "create bucket failed")

	s.insertDocuments = func(bulks ...[]string) {
		if s.fraction != nil {
			s.Require().Fail("can insert docs only once")
		}
		sealed := s.newSealed(bulks...)
		defer sealed.Suicide()

		s3cli, err := s3.NewClient(
			s.s3server.URL,
			"ACCESS_KEY",
			"SECRET_KEY",
			"eu-west-3",
			bucketName,
		)
		s.Require().NoError(err, "s3 client setup failed")

		offloaded, err := sealed.Offload(context.Background(), s3.NewUploader(s3cli))
		s.Require().NoError(err, "offload failed")
		s.Require().True(offloaded, "didn't offload frac")

		indexCache := &IndexCache{
			MIDs:       cache.NewCache[[]byte](nil, nil),
			RIDs:       cache.NewCache[seqids.BlockRIDs](nil, nil),
			Params:     cache.NewCache[seqids.BlockParams](nil, nil),
			LIDs:       cache.NewCache[*lids.Block](nil, nil),
			Tokens:     cache.NewCache[*token.Block](nil, nil),
			TokenTable: cache.NewCache[token.Table](nil, nil),
			InfoRegistry:    cache.NewCache[[]byte](nil, nil),
			TokenRegistry:   cache.NewCache[[]byte](nil, nil),
			OffsetsRegistry: cache.NewCache[[]byte](nil, nil),
			IDRegistry:      cache.NewCache[[]byte](nil, nil),
			LIDRegistry:     cache.NewCache[[]byte](nil, nil),
		}

		remoteFrac := NewRemote(
			context.Background(),
			sealed.BaseFileName,
			storage.NewReadLimiter(1, nil),
			indexCache,
			cache.NewCache[[]byte](nil, nil),
			sealed.info,
			s.config,
			s3cli)
		s.fraction = remoteFrac
	}
}

func (s *RemoteFractionTestSuite) TearDownTest() {
	if remote, ok := s.fraction.(*Remote); ok {
		remote.Suicide()
	} else {
		s.Require().Nil(s.fraction, "fraction is not of Remote type")
	}
	s.TearDownTestCommon()
}

func (s *RemoteFractionTestSuite) TearDownSuite() {
	s.TearDownSuiteCommon()

	s.s3server.Close()
}

func TestActiveFractionTestSuite(t *testing.T) {
	suite.Run(t, new(ActiveFractionTestSuite))
}

func TestActiveReplayedFractionTestSuite(t *testing.T) {
	suite.Run(t, new(ActiveReplayedFractionTestSuite))
}

func TestSealedFractionTestSuite(t *testing.T) {
	suite.Run(t, new(SealedFractionTestSuite))
}

func TestSealedLoadedFractionTestSuite(t *testing.T) {
	suite.Run(t, new(SealedLoadedFractionTestSuite))
}

func TestRemoteFractionTestSuite(t *testing.T) {
	suite.Run(t, new(RemoteFractionTestSuite))
}
