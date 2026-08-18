package integration_tests

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/asyncsearcher"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/skipmaskmanager"
	"github.com/ozontech/seq-db/tests/common"
	"github.com/ozontech/seq-db/tests/setup"
	"github.com/ozontech/seq-db/tests/suites"
)

func getTotal(regular int, withTotal bool) uint64 {
	if withTotal {
		return uint64(regular)
	}
	return 0
}

func getAutoTsGenerator(start time.Time, step time.Duration) func() string {
	return func() string {
		r := start.Format(time.RFC3339Nano)
		start = start.Add(step)
		return r
	}
}

func getAutoTimeGenerator(start time.Time, step time.Duration) func() time.Time {
	return func() time.Time {
		cur := start
		start = start.Add(step)
		return cur
	}
}

func (s *IntegrationTestSuite) TestSearchOne() {
	origDocs := []string{
		`{"service":"a", "xxxx":"yyyy"}`,
		`{"k8s_pod":"sq-toloka-loader-1788964-dryrun-58hmw", "yyyy":"xxxx"}`,
	}

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)
	env.WaitIdle()

	for _, withTotal := range []bool{true, false} {

		assertSearch := func(qpr *seq.QPR, err error) {
			assert.NoError(s.T(), err, "should be no errors")
			assert.Len(s.T(), qpr.IDs, 1, "wrong doc count")
			assert.Equal(s.T(), getTotal(1, withTotal), qpr.Total, "wrong doc count")
		}

		// search first
		qpr, docs, err := env.Search(`service:a`, 1000, setup.WithTotal(withTotal))
		assertSearch(qpr, err)
		if assert.Greater(s.T(), len(docs), 0, "no docs found") {
			assert.Equal(s.T(), origDocs[0], string(docs[0]), "wrong doc content")
		}

		// search first with _exists_
		qpr, docs, err = env.Search(`_exists_:service`, 1000, setup.WithTotal(withTotal))
		assertSearch(qpr, err)
		if assert.Greater(s.T(), len(docs), 0, "no docs found") {
			assert.Equal(s.T(), origDocs[0], string(docs[0]), "wrong doc content")
		}

		// search first with NOT _exists_
		qpr, docs, err = env.Search(`NOT _exists_:k8s_pod`, 1000, setup.WithTotal(withTotal))
		assertSearch(qpr, err)
		if assert.Greater(s.T(), len(docs), 0, "no docs found") {
			assert.Equal(s.T(), origDocs[0], string(docs[0]), "wrong doc content")
		}

		// search second
		qpr, docs, err = env.Search(`k8s_pod:sq-toloka-loader-1788964-dryrun-58hmw`, 1000, setup.WithTotal(withTotal))
		assertSearch(qpr, err)
		if assert.Greater(s.T(), len(docs), 0, "no docs found") {
			assert.Equal(s.T(), origDocs[1], string(docs[0]), "wrong doc content")
		}

		if withTotal {
			if assert.Greater(s.T(), int(qpr.Total), 0, "no docs found") {
				tmpDoc := env.Ingestor().SearchIngestor.Document(context.Background(), qpr.IDs[0].ID, search.FetchFieldsFilter{})
				assert.Equal(s.T(), origDocs[1], string(tmpDoc), "wrong doc content")
			}
		}
	}
}

func (s *IntegrationTestSuite) TestPipeFields() {
	config := *s.Config
	config.Mapping = map[string]seq.MappingTypes{
		"event":   seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"message": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
	}

	env := setup.NewTestingEnv(&config)
	defer env.StopAll()

	docs := []string{
		`{"level": "info","ts": "2000-01-13T17:36:10.593303253Z","logger": "fd.kubelet","message": "pipeline stats","stat": "interval=5s, active procs=0/4, events in use=0/256, out=0|0.0Mb, rate=0/s|0.0Mb/s, read ops=0/s, total=0|0.0Mb, avg size=0"}`,
		`{"level": "info","ts": "2000-01-13T17:36:12.790469375Z","logger": "fd.k8s.input.k8s","message": "file plugin stats for last 5 seconds: offsets saves=104111, jobs done=28, jobs total=28"}`,
		`{"level": "info","ts": "2000-01-13T17:36:14.715199225Z","logger": "fd.k8s.action.debug","message": "input event sample","offset": 40059539,"event": {"log": "{\"level\":\"info\",\"ts\":\"2025-01-13T17:36:08.729825704Z\",\"logger\":\"fd.dmesg\",\"message\":\"pipeline stats\",\"stat\":\"interval=5s, active procs=0/2, events in use=0/128, out=0|0.0Mb, rate=0/s|0.0Mb/s, read ops=0/s, total=100857|15.0Mb, avg size=1\"}\n","time": "2025-01-13T17:36:08.729920774Z","stream": "stderr","k8s_container_id": "52f2ab19fe0ba66f4f4e7910780da1e477be98015db58dc624a26c4a585e096b","app_label": "dmesg-reader-z504","pod_app": "dmesg-reader-z504-wjfh5@dmesg-reader-z504","k8s_pod": "dmesg-reader-z504-wjfh5","k8s_namespace": "logging","k8s_container": "dmesg-reader","k8s_node": "infrakuben87742z504","k8s_pod_label_app": "dmesg-reader-z504","k8s_node_label_topology.kubernetes.io/zone": "z504"}}`,
	}

	setup.Bulk(s.T(), env.IngestorBulkAddr(), docs)
	env.WaitIdle()

	r := require.New(s.T())
	test := func(query string, expectedDocsRaw []byte) {
		s.T().Helper()

		resp := setup.SearchHTTP(s.T(), env.IngestorSearchAddr(), &seqproxyapi.SearchRequest{
			Query: &seqproxyapi.SearchQuery{
				Query:   query,
				From:    timestamppb.New(time.Now().Add(-time.Hour * 720)),
				To:      timestamppb.New(time.Now().Add(time.Hour * 720)),
				Explain: false,
			},
			Size:      10,
			Offset:    0,
			WithTotal: false,
		})

		var actualDocs []map[string]any
		for _, doc := range resp.Docs {
			obj := make(map[string]any)
			r.NoError(json.Unmarshal(doc.Data, &obj))
			actualDocs = append(actualDocs, obj)
		}

		var expectedDocs []map[string]any
		r.NoError(json.Unmarshal(expectedDocsRaw, &expectedDocs))

		r.Equal(len(actualDocs), len(expectedDocs))
		for _, doc := range expectedDocs {
			r.Contains(actualDocs, doc)
		}
	}

	test(`* | fields message`, []byte(`[
		{"message":"pipeline stats"},
		{"message":"file plugin stats for last 5 seconds: offsets saves=104111, jobs done=28, jobs total=28"},
		{"message":"input event sample"}
	]`))
	test(`* | fields event`, []byte(`[
	  {"event": {"log": "{\"level\":\"info\",\"ts\":\"2025-01-13T17:36:08.729825704Z\",\"logger\":\"fd.dmesg\",\"message\":\"pipeline stats\",\"stat\":\"interval=5s, active procs=0/2, events in use=0/128, out=0|0.0Mb, rate=0/s|0.0Mb/s, read ops=0/s, total=100857|15.0Mb, avg size=1\"}\n","time": "2025-01-13T17:36:08.729920774Z","stream": "stderr","k8s_container_id": "52f2ab19fe0ba66f4f4e7910780da1e477be98015db58dc624a26c4a585e096b","app_label": "dmesg-reader-z504","pod_app": "dmesg-reader-z504-wjfh5@dmesg-reader-z504","k8s_pod": "dmesg-reader-z504-wjfh5","k8s_namespace": "logging","k8s_container": "dmesg-reader","k8s_node": "infrakuben87742z504","k8s_pod_label_app": "dmesg-reader-z504","k8s_node_label_topology.kubernetes.io/zone": "z504"}},
	  {},
	  {}
	]`))
	test(`* | fields except original_timestamp,ts, event`, []byte(`[
		{"level":"info","logger":"fd.k8s.action.debug","message":"input event sample","offset":40059539},
		{"level":"info","logger":"fd.kubelet","message":"pipeline stats","stat":"interval=5s, active procs=0/4, events in use=0/256, out=0|0.0Mb, rate=0/s|0.0Mb/s, read ops=0/s, total=0|0.0Mb, avg size=0"},
		{"level":"info","message":"file plugin stats for last 5 seconds: offsets saves=104111, jobs done=28, jobs total=28","logger":"fd.k8s.input.k8s"}
	]`))
	test(`* | fields _exists_`, []byte(`[
		{},
		{},
		{}
	]`))
	test(`_exists_:message | fields _exists_`, []byte(`[
		{},
		{},
		{}
	]`))
	test(`not _exists_: event | fields event`, []byte(`[
		{},
		{}
	]`))
}

func (s *IntegrationTestSuite) TestSearchOneHTTP() {
	origDocs := []string{
		`{"service":"a", "xxxx":"yyyy"}`,
		`{"service":"b", "k8s_pod":"sq-toloka-loader-1788964-dryrun-58hmw", "yyyy":"xxxx"}`,
	}

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)
	env.WaitIdle()

	searchDoc := func(query string, expectedService string) {
		resp := setup.SearchHTTP(s.T(), env.IngestorSearchAddr(), &seqproxyapi.SearchRequest{
			Query: &seqproxyapi.SearchQuery{
				Query:   query,
				From:    timestamppb.New(time.Now().Add(-time.Hour * 720)),
				To:      timestamppb.New(time.Now().Add(time.Hour * 720)),
				Explain: false,
			},
			Size:      10,
			Offset:    0,
			WithTotal: true,
		})

		r := require.New(s.T())
		r.Equal(int64(1), resp.Total)
		r.Equal(1, len(resp.Docs))

		type Doc struct {
			Service string `json:"service"`
		}
		doc := Doc{}
		r.NoError(json.Unmarshal(resp.Docs[0].Data, &doc))
		r.Equal(expectedService, doc.Service)
	}

	searchDoc("service:a", "a")
	searchDoc("k8s_pod:sq*", "b")
}

func (s *IntegrationTestSuite) TestSearchNothing() {
	origDocs := []string{
		`{"service":"a", "xxxx":"yyyy"}`,
		`{"k8s_pod":"sq-toloka-loader-1788964-dryrun-58hmw", "yyyy":"xxxx"}`,
	}
	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)

	qpr, _, err := env.Search(`k8s_pod:NO`, 1000, setup.NoFetch())
	assert.NoError(s.T(), err, "should be no errors")
	assert.Len(s.T(), qpr.IDs, 0, "wrong doc count")
	assert.Equal(s.T(), uint64(0), qpr.Total, "wrong doc count")
}

func (s *IntegrationTestSuite) TestSearchSequence() {
	docTemplate := `{"service":"a","time":"%s"}`
	bulks := 16
	bulkSize := 1024

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	// append some info
	for i := 0; i < bulks; i++ {
		origDocs := []string{}
		now := time.Now()
		for j := 0; j < bulkSize; j++ {
			ts := now.Add(time.Duration(rand.Uint64()%5) * time.Millisecond)
			origDocs = append(origDocs, fmt.Sprintf(docTemplate, ts.Format(consts.ESTimeFormat)))
		}

		setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)
	}
	env.WaitIdle()

	for _, o := range []seq.DocsOrder{seq.DocsOrderAsc, seq.DocsOrderDesc} {
		for _, withTotal := range []bool{true, false} {
			qpr, _, err := env.Search(`service:a`, math.MaxInt32, setup.NoFetch(), setup.WithTotal(withTotal), setup.WithOrder(o))
			assert.NoError(s.T(), err, "should be no errors")
			assert.Len(s.T(), qpr.IDs, bulks*bulkSize, "wrong doc count")
			assert.Equal(s.T(), getTotal(bulks*bulkSize, withTotal), qpr.Total, "wrong doc count")

			if o.IsReverse() {
				x := seq.ID{MID: 0, RID: 0}
				for _, idSource := range qpr.IDs {
					if idSource.ID.MID < x.MID {
						assert.FailNow(s.T(), "wrong sequence")
					}
					x = idSource.ID
				}
			} else {
				x := seq.ID{MID: math.MaxUint64, RID: math.MaxUint64}
				for _, idSource := range qpr.IDs {
					if idSource.ID.MID > x.MID {
						assert.FailNow(s.T(), "wrong sequence")
					}
					x = idSource.ID
				}
			}
		}
	}
}

func (s *IntegrationTestSuite) TestSearchMany() {
	const NetN = 256 * 1024
	n := int(math.Floor(NetN * 1.2))

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	// append some info
	origDocs := []string{}
	for i := 0; i < n; i++ {
		origDocs = append(origDocs, fmt.Sprintf(`{"service":"a", "xxxx":"%d"}`, i))
	}

	setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)
	env.WaitIdle()

	for _, withTotal := range []bool{true, false} {
		qpr, _, err := env.Search(`service:a`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(n, withTotal), qpr.Total, "wrong doc count")
	}
}

func largeString(ln int) string {
	str := []byte{'a'}
	for x := 0; x < ln; x++ {
		str = append(str, str...)
	}
	str = str[:len(str)-64]
	return string(str)
}

// getBulkIterationsNum gets min number of bulk iterations to cover (by round robin balancing) all store and ingector instances
func getBulkIterationsNum(e *setup.TestingEnv) int {
	r := len(e.ColdStores)
	if r < len(e.HotStores) {
		r = len(e.HotStores)
	}
	return r * len(e.Ingestors)
}

func (s *IntegrationTestSuite) envWithDummyDocs(n int) (*setup.TestingEnv, []string) {
	env := setup.NewTestingEnv(s.Config)

	str := largeString(20)
	bulksNum := getBulkIterationsNum(env)
	allDocsNum := 2 * n * bulksNum
	origDocs := make([]string, 0, allDocsNum)
	docsBulk := make([]string, 2*n)

	getNextTs := getAutoTsGenerator(time.Now(), -time.Nanosecond)

	for i := 0; i < bulksNum; i++ {

		for i := 0; i < n; i++ {
			docsBulk[2*i] = fmt.Sprintf(`{"service":"a", "xxxx":"%d", "ts":%q}`, i, getNextTs())
			docsBulk[2*i+1] = fmt.Sprintf(`{"service":"a", "xxxx":%q, "time":%q}`, str, getNextTs())
		}
		setup.Bulk(s.T(), env.IngestorBulkAddr(), docsBulk)
		origDocs = append(origDocs, docsBulk...)
	}
	return env, origDocs
}

func (s *IntegrationTestSuite) TestFetch() {
	env, origDocs := s.envWithDummyDocs(16)
	env.WaitIdle()
	for _, withTotal := range []bool{true, false} {
		qpr, _, err := env.Search(`service:a`, 10, setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(len(origDocs), withTotal), qpr.Total, "wrong doc count")
	}

	env.SealAll()
	env.StopAll()

	time.Sleep(time.Millisecond * 100)

	env = setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	size := 10

	for _, o := range []seq.DocsOrder{seq.DocsOrderAsc, seq.DocsOrderDesc} {
		copyDocs := copySlice(origDocs)
		if o.IsReverse() {
			slices.Reverse(copyDocs)
		}

		for _, withTotal := range []bool{true, false} {
			qpr, docs, err := env.Search(`service:a`, size, setup.WithTotal(withTotal), setup.WithOrder(o))

			assert.NoError(s.T(), err, "should be no errors")
			assert.Equal(s.T(), size, len(docs))
			assert.Equal(s.T(), getTotal(len(origDocs), withTotal), qpr.Total, "wrong doc count")

			for i, doc := range docs {
				assert.Equal(s.T(), copyDocs[i], string(doc), "wrong doc content")
			}
		}
	}
}

func (s *IntegrationTestSuite) TestFetchNotFound() {
	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	now := time.Now()
	nowNext := now.Add(time.Millisecond * 10)

	for i := 0; i < getBulkIterationsNum(env); i++ {
		// append some info
		origDocs := []string{
			fmt.Sprintf(`{"service":"a", "time":%q}`, now.Format(time.RFC3339Nano)),
			fmt.Sprintf(`{"service":"b", "time":%q}`, nowNext.Format(time.RFC3339Nano)),
		}
		setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)
	}

	env.WaitIdle()
	env.SealAll()
	doc := env.Ingestor().SearchIngestor.Document(context.Background(), seq.NewID(now, 0), search.FetchFieldsFilter{})
	assert.Empty(s.T(), doc)
}

func (s *IntegrationTestSuite) TestMulti() {
	// ingest
	getNextTs := getAutoTsGenerator(time.Now(), -time.Nanosecond)
	origDocs := []string{
		fmt.Sprintf(`{"service":"b1", "k8s_pod":"pod1", "yyyy":"xxxx1", "ts":%q}`, getNextTs()),
		fmt.Sprintf(`{"service":"b2", "k8s_pod":"pod2", "yyyy":"xxxx2", "ts":%q}`, getNextTs()),
		fmt.Sprintf(`{"service":"b3", "k8s_pod":"pod3", "yyyy":"xxxx3", "ts":%q}`, getNextTs()),
		fmt.Sprintf(`{"service":"b4", "k8s_pod":"pod4", "yyyy":"xxxx4", "ts":%q}`, getNextTs()),
	}

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()
	setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)
	env.WaitIdle()

	// search
	qpr, _, err := env.Search(`service:*`, 10)
	assert.NoError(s.T(), err, "should be no errors")
	assert.Equal(s.T(), uint64(len(origDocs)), qpr.Total, "wrong doc count")
	assert.Equal(s.T(), len(origDocs), len(qpr.IDs), "wrong doc count")

	idsToFetch := collectIDs(qpr)
	fetchedDocs := setup.FetchHTTP(s.T(), env.IngestorFetchAddr(), idsToFetch)

	for i, item := range fetchedDocs {
		assert.Equal(s.T(), item, fetchedDocs[i])
	}
}

func collectIDs(qpr *seq.QPR) []string {
	ids := make([]string, 0, len(qpr.IDs))
	for _, id := range qpr.IDs {
		ids = append(ids, id.ID.String())
	}
	return ids
}

func (s *IntegrationTestSuite) TestSearchNot() {
	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	n := 777
	bulksNum := getBulkIterationsNum(env)
	allDocsNum := 2 * n * bulksNum

	for j := 0; j < bulksNum; j++ {
		origDocs := []string{}
		for i := 0; i < n; i++ {
			origDocs = append(
				origDocs,
				fmt.Sprintf(`{"service":"a", "xxxx":"%d"}`, i),
				fmt.Sprintf(`{"service":"x", "xxxx":"%d"}`, i),
			)
		}
		setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)
	}

	env.WaitIdle()

	for _, withTotal := range []bool{true, false} {
		qpr, _, err := env.Search(`NOT service:b`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(2*n*bulksNum, withTotal), qpr.Total, "wrong doc count")

		qpr, _, err = env.Search(`NOT service:x`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(n*bulksNum, withTotal), qpr.Total, "wrong doc count")

		qpr, _, err = env.Search(`NOT service:a AND NOT service:x`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), 0, int(qpr.Total), "wrong doc count")

		qpr, _, err = env.Search(`NOT _exists_:service`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), 0, int(qpr.Total), "wrong doc count")

		qpr, _, err = env.Search(`NOT _exists_:k8s_pod`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(allDocsNum, withTotal), qpr.Total, "wrong doc count")

		qpr, _, err = env.Search(`NOT _exists_:k8s_pod`, -1, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.ErrorIs(s.T(), err, consts.ErrInvalidArgument)
		assert.Nil(s.T(), qpr)

		qpr, _, err = env.Search(`NOT _exists_:k8s_pod`, 1, setup.WithOffset(-1),
			setup.NoFetch(), setup.WithTotal(withTotal))
		assert.ErrorIs(s.T(), err, consts.ErrInvalidArgument)
		assert.Nil(s.T(), qpr)
	}

	env.SealAll()

	for _, withTotal := range []bool{true, false} {

		qpr, _, err := env.Search(`NOT service:x`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(n*bulksNum, withTotal), qpr.Total, "wrong doc count")

		qpr, _, err = env.Search(`NOT service:a AND NOT service:x`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), 0, int(qpr.Total), "wrong doc count")

		qpr, _, err = env.Search(`NOT _exists_:service`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), 0, int(qpr.Total), "wrong doc count")

		qpr, _, err = env.Search(`NOT _exists_:k8s_pod`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(allDocsNum, withTotal), qpr.Total, "wrong doc count")
	}
}

func (s *IntegrationTestSuite) TestSearchPattern() {
	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	n := 10000

	bulksNum := getBulkIterationsNum(env)
	allDocsNum := n * bulksNum

	for j := 0; j < bulksNum; j++ {
		// append some info
		origDocs := []string{}
		for i := 0; i < n; i++ {
			origDocs = append(origDocs, fmt.Sprintf(`{"service":"x%d"}`, i))
		}
		setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)
	}
	env.WaitIdle()

	for _, withTotal := range []bool{true, false} {
		qpr, _, err := env.Search(`service:x*`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(allDocsNum, withTotal), qpr.Total, "wrong doc count")
	}

	env.SealAll()

	for _, withTotal := range []bool{true, false} {
		qpr, _, err := env.Search(`service:x*`, 10, setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(allDocsNum, withTotal), qpr.Total, "wrong doc count")
	}
}

func (s *IntegrationTestSuite) TestSearchSimple() {
	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	n := 100
	bulksNum := getBulkIterationsNum(env)

	tokens := make([]string, n)
	tokens[0] = "1"
	for i := 1; i < n; i++ {
		tokens[i] = "1" + strconv.Itoa(i) // prefixed with "1"
	}

	for j := 0; j < bulksNum; j++ {
		// append some info
		origDocs := []string{}
		for i, token := range tokens {
			origDocs = append(origDocs, fmt.Sprintf(`{"service":"x%d", "message":%q}`, i, token))
		}
		setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)
	}
	env.WaitIdle()

	for _, token := range tokens {
		qpr, _, err := env.Search("message:"+token, 10, setup.NoFetch(), setup.WithTotal(true))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), bulksNum, int(qpr.Total), "wrong doc count for token "+token)
	}

	env.SealAll()

	for _, token := range tokens {
		qpr, _, err := env.Search("message:"+token, 10, setup.NoFetch(), setup.WithTotal(true))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), bulksNum, int(qpr.Total), "wrong doc count for token "+token)
	}
}

func (s *IntegrationTestSuite) TestManySearchRequests() {
	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	n := 66

	origDocs := []string{}
	for i := 0; i < n; i++ {
		origDocs = append(origDocs, fmt.Sprintf(`{"service":"x", "xxxx":"%d"}`, i))
	}
	setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)
	env.WaitIdle()

	for x := 0; x < 5000; x++ {
		qpr, _, err := env.Search(`service:x`, 10, setup.NoFetch())
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), uint64(n), qpr.Total, "wrong doc count")
	}
}

func (s *IntegrationTestSuite) TestAgg() {
	t := s.T()

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	n := 777
	bulksNum := getBulkIterationsNum(env)
	allDocsNum := n * bulksNum

	for j := 0; j < bulksNum; j++ {
		origDocs := make([]string, 0, n)
		for i := 0; i < n; i++ {
			origDocs = append(origDocs, fmt.Sprintf(`{"service":"x%d","k8s_pod":"y%d"}`, i%3, i%3))
		}
		setup.Bulk(t, env.IngestorBulkAddr(), origDocs)
	}

	env.WaitIdle()

	r := require.New(t)
	for _, withTotal := range []bool{true, false} {
		qpr, _, err := env.Search(`service:x1`, 10, setup.WithAggQuery("service"), setup.NoFetch(), setup.WithTotal(withTotal))
		r.NoError(err, "should be no errors")
		r.Equal(getTotal(allDocsNum/3, withTotal), qpr.Total, "wrong doc count")
		r.NotNil(qpr.Aggs[0].SamplesByBin[seq.AggBin{Token: "x1"}], qpr.Aggs[0].SamplesByBin)
		r.Equal(int64(allDocsNum/3), qpr.Aggs[0].SamplesByBin[seq.AggBin{Token: "x1"}].Total, "wrong doc count")

		qpr, _, err = env.Search(`service:x*`, 10, setup.WithAggQuery("service"), setup.NoFetch(), setup.WithTotal(withTotal))
		r.NoError(err, "should be no errors")
		r.Equal(getTotal(allDocsNum, withTotal), qpr.Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[0].SamplesByBin[seq.AggBin{Token: "x1"}].Total, "wrong doc count")

		aggQ := setup.WithAggQuery(
			"service",
			"k8s_pod",
		)
		qpr, _, err = env.Search(`service:x1`, 10, aggQ, setup.NoFetch(), setup.WithTotal(withTotal))
		r.NoError(err, "should be no errors")
		r.Equal(getTotal(allDocsNum/3, withTotal), qpr.Total, "wrong doc count")
		r.Equal(2, len(qpr.Aggs), "wrong agg count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[0].SamplesByBin[seq.AggBin{Token: "x1"}].Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[1].SamplesByBin[seq.AggBin{Token: "y1"}].Total, "wrong doc count")

		qpr, _, err = env.Search(`service:x*`, 10, aggQ, setup.NoFetch(), setup.WithTotal(withTotal))
		r.NoError(err, "should be no errors")
		r.Equal(2, len(qpr.Aggs), "wrong agg count")
		r.Equal(getTotal(allDocsNum, withTotal), qpr.Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[0].SamplesByBin[seq.AggBin{Token: "x1"}].Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[1].SamplesByBin[seq.AggBin{Token: "y1"}].Total, "wrong doc count")
	}

	env.SealAll()

	for _, withTotal := range []bool{true, false} {
		qpr, _, err := env.Search(`service:x1`, 10, setup.WithAggQuery("service"), setup.NoFetch(), setup.WithTotal(withTotal))
		r.NoError(err, "should be no errors")
		r.Equal(getTotal(allDocsNum/3, withTotal), qpr.Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[0].SamplesByBin[seq.AggBin{Token: "x1"}].Total, "wrong doc count")

		qpr, _, err = env.Search(`service:x*`, 10, setup.WithAggQuery("service"), setup.NoFetch(), setup.WithTotal(withTotal))
		r.NoError(err, "should be no errors")
		r.Equal(getTotal(allDocsNum, withTotal), qpr.Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[0].SamplesByBin[seq.AggBin{Token: "x1"}].Total, "wrong doc count")

		aggQ := setup.WithAggQuery(
			"service",
			"k8s_pod",
		)
		qpr, _, err = env.Search(`service:x1`, 10, aggQ, setup.NoFetch(), setup.WithTotal(withTotal))
		r.NoError(err, "should be no errors")
		r.Equal(getTotal(allDocsNum/3, withTotal), qpr.Total, "wrong doc count")
		r.Equal(2, len(qpr.Aggs), "wrong agg count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[0].SamplesByBin[seq.AggBin{Token: "x1"}].Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[1].SamplesByBin[seq.AggBin{Token: "y1"}].Total, "wrong doc count")

		qpr, _, err = env.Search(`service:x*`, 10, aggQ, setup.NoFetch(), setup.WithTotal(withTotal))
		r.NoError(err, "should be no errors")
		r.Equal(2, len(qpr.Aggs), "wrong agg count")
		r.Equal(getTotal(allDocsNum, withTotal), qpr.Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[0].SamplesByBin[seq.AggBin{Token: "x1"}].Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[1].SamplesByBin[seq.AggBin{Token: "y1"}].Total, "wrong doc count")
	}
}

func (s *IntegrationTestSuite) TestTimeseries() {
	t := s.T()

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	timeBinsCount := 10
	timeBins := []time.Time{time.Now().Truncate(time.Second)}

	// We have [timeBinsCount] intervals and each document will have timestamp
	// that equals to some value in [timeBins].
	//
	// Each [nextBin] documents we are going to advance selected time interval in one position
	// to the right.
	for i := 1; i < timeBinsCount; i++ {
		timeBins = append(timeBins, timeBins[i-1].Add(time.Second*30))
	}

	var (
		docs      []string
		nextBin   = 10
		batchSize = timeBinsCount * nextBin
	)

	bulkDataset := func(service string, level func(i int) int) {
		for i := range batchSize {
			first, err := json.Marshal(map[string]any{
				"ts":      timeBins[i/nextBin],
				"service": service,
				"level":   level(i),
			})
			require.NoError(t, err)

			second, err := json.Marshal(map[string]any{
				"ts":      timeBins[i/nextBin],
				"service": fmt.Sprintf("%s-noise", service),
				"level":   level(i),
			})
			require.NoError(t, err)

			docs = append(docs, string(first), string(second))
		}
		setup.Bulk(t, env.IngestorBulkAddr(), docs)
		env.WaitIdle()
	}

	t.Run("count", func(t *testing.T) {
		bulkDataset("nginx-count", func(int) int { return 1 })

		qpr, _, err := env.Search(`service:"nginx-count"`, 1024, setup.WithAggQuery(search.AggQuery{
			GroupBy:  "level",
			Func:     seq.AggFuncCount,
			Interval: seq.DurationToMID(30 * time.Second),
		}))
		require.NoError(t, err)

		hist := qpr.Aggs[0].SamplesByBin
		require.Len(t, hist, timeBinsCount)

		bins := sortedTimeBins(hist)
		for i := range timeBinsCount {
			require.Equal(t, int64(nextBin), hist[bins[i]].Total)
		}
	})

	t.Run("min", func(t *testing.T) {
		bulkDataset("nginx-min", func(i int) int { return i })

		qpr, _, err := env.Search(`service:"nginx-min"`, 1024, setup.WithAggQuery(search.AggQuery{
			Field:    "level",
			GroupBy:  "service",
			Func:     seq.AggFuncMin,
			Interval: seq.DurationToMID(30 * time.Second),
		}))
		require.NoError(t, err)

		hist := qpr.Aggs[0].SamplesByBin
		require.Len(t, hist, timeBinsCount)

		bins := sortedTimeBins(hist)
		for i := range timeBinsCount {
			require.Equal(t, float64(nextBin*i), hist[bins[i]].Min)
			require.Equal(t, "nginx-min", bins[i].Token)
		}
	})

	t.Run("max", func(t *testing.T) {
		bulkDataset("nginx-max", func(i int) int { return i })

		qpr, _, err := env.Search(`service:"nginx-max"`, 1024, setup.WithAggQuery(search.AggQuery{
			Field:    "level",
			Func:     seq.AggFuncMax,
			Interval: seq.DurationToMID(30 * time.Second),
		}))
		require.NoError(t, err)

		hist := qpr.Aggs[0].SamplesByBin
		require.Len(t, hist, timeBinsCount)

		bins := sortedTimeBins(hist)
		for i := range timeBinsCount {
			require.Equal(t, float64(nextBin*(i+1)-1), hist[bins[i]].Max)
		}
	})

	t.Run("avg", func(t *testing.T) {
		bulkDataset("nginx-avg", func(int) int { return 1 })

		qpr, _, err := env.Search(`service:"nginx-avg"`, 1024, setup.WithAggQuery(search.AggQuery{
			Field:    "level",
			Func:     seq.AggFuncAvg,
			Interval: seq.DurationToMID(30 * time.Second),
		}))
		require.NoError(t, err)

		hist := qpr.Aggs[0].SamplesByBin
		require.Len(t, hist, timeBinsCount)

		bins := sortedTimeBins(hist)
		for i := range timeBinsCount {
			require.Equal(t, float64(1), hist[bins[i]].Sum/float64(hist[bins[i]].Total))
		}
	})

	t.Run("sum", func(t *testing.T) {
		bulkDataset("nginx-sum", func(int) int { return 1 })

		qpr, _, err := env.Search(`service:"nginx-sum"`, 1024, setup.WithAggQuery(search.AggQuery{
			Field:    "level",
			Func:     seq.AggFuncSum,
			Interval: seq.DurationToMID(30 * time.Second),
		}))
		require.NoError(t, err)

		hist := qpr.Aggs[0].SamplesByBin
		require.Len(t, hist, timeBinsCount)

		bins := sortedTimeBins(hist)
		for i := range timeBinsCount {
			require.Equal(t, float64(nextBin), hist[bins[i]].Sum)
		}
	})

	t.Run("quantile", func(t *testing.T) {
		bulkDataset("nginx-quantile", func(i int) int { return i })

		qpr, _, err := env.Search(`service:"nginx-quantile"`, 1024, setup.WithAggQuery(search.AggQuery{
			Field:     "level",
			Func:      seq.AggFuncQuantile,
			Quantiles: []float64{0.5},
			Interval:  seq.DurationToMID(30 * time.Second),
		}))
		require.NoError(t, err)

		hist := qpr.Aggs[0].SamplesByBin
		require.Len(t, hist, timeBinsCount)

		bins := sortedTimeBins(hist)
		for i := range timeBinsCount {
			require.Equal(t, float64(nextBin*i+5), hist[bins[i]].Quantile(0.5))
		}
	})

	t.Run("unique_count", func(t *testing.T) {
		bulkDataset("nginx-unique-count", func(i int) int { return i % nextBin })

		qpr, _, err := env.Search(`service:"nginx-unique-count"`, 1024, setup.WithAggQuery(search.AggQuery{
			Field:    "level",
			GroupBy:  "service",
			Func:     seq.AggFuncUniqueCount,
			Interval: seq.DurationToMID(30 * time.Second),
		}))
		require.NoError(t, err)

		hist := qpr.Aggs[0].SamplesByBin
		require.Len(t, hist, timeBinsCount)

		bins := sortedTimeBins(hist)
		for i := range timeBinsCount {
			require.Equal(t, "nginx-unique-count", bins[i].Token)
			require.Equal(t, int64(nextBin), int64(len(hist[bins[i]].Values)))
		}

		require.NotEmpty(t, qpr.Aggs[0].ValuesPool)
		levelStrings := make(map[string]bool)
		for i := 0; i < nextBin; i++ {
			levelStrings[strconv.Itoa(i)] = true
		}
		for _, val := range qpr.Aggs[0].ValuesPool {
			delete(levelStrings, val)
		}
		require.Empty(t, levelStrings)
	})
}

func sortedTimeBins(hist map[seq.AggBin]*seq.SamplesContainer) []seq.AggBin {
	keys := slices.Collect(maps.Keys(hist))
	slices.SortFunc(keys, func(a, b seq.AggBin) int {
		return a.MID.Time().Compare(b.MID.Time())
	})
	return keys
}

func (s *IntegrationTestSuite) TestAggNoTotal() {
	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	size := 4
	parts := 5

	n := 100
	bulksNum := getBulkIterationsNum(env)
	allDocsNum := n * bulksNum
	aggCnt := uint64(allDocsNum / parts)
	tsStep := time.Second
	histInterval := time.Minute
	start := time.Now()
	getNextTs := getAutoTsGenerator(start, -tsStep)

	fromAligned := start.Add(-tsStep * time.Duration(allDocsNum-1)).Truncate(histInterval)
	toAligned := start.Truncate(histInterval)
	histCnt := int(toAligned.Sub(fromAligned)/histInterval) + 1

	for j := 0; j < bulksNum; j++ {
		origDocs := []string{}
		for i := 0; i < n; i++ {
			origDocs = append(origDocs, fmt.Sprintf(`{"service":"x%d", "ts":%q}`, i%parts, getNextTs()))
		}
		setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)
	}

	env.WaitIdle()

	searchNoTotal := func(agg string, interval time.Duration) (*seq.QPR, [][]byte, error) {
		options := []setup.SearchOption{setup.WithInterval(interval), setup.NoFetch(), setup.WithTotal(false)}
		if agg != "" {
			options = append(options, setup.WithAggQuery(agg))
		}
		return env.Search(`service:x*`, size, options...)
	}

	searchWithTotal := func(agg string, interval time.Duration) (*seq.QPR, [][]byte, error) {
		options := []setup.SearchOption{setup.WithInterval(interval), setup.NoFetch()}
		if agg != "" {
			options = append(options, setup.WithAggQuery(agg))
		}
		return env.Search(`service:x*`, size, options...)
	}

	test := func(t *testing.T) {
		// search
		qpr, _, err := searchWithTotal("", 0)
		require.NoError(t, err, "should be no errors")
		assert.Equal(t, uint64(allDocsNum), qpr.Total, "we must scann all docs in withTotal=true mode")
		assert.Equal(t, size, len(qpr.IDs), "we must get only size ids")

		qpr, _, err = searchNoTotal("", 0)
		require.NoError(t, err, "should be no errors")
		assert.Equal(t, uint64(0), qpr.Total, "we must get Total = 0 in withTotal=false mode")
		assert.Equal(t, size, len(qpr.IDs), "we must get only size ids")

		// aggregation
		qpr, _, err = searchWithTotal("service", 0)
		require.NoError(t, err, "should be no errors")
		assert.Equal(t, uint64(allDocsNum), qpr.Total, "we must scan all docs in withTotal=true mode")
		assert.Equal(t, size, len(qpr.IDs), "we must get only size ids")
		for i := 0; i < parts; i++ {
			k := "x" + strconv.Itoa(i)
			assert.Equal(t, int(aggCnt), int(qpr.Aggs[0].SamplesByBin[seq.AggBin{Token: k}].Total), "we expect 1/%d of all documents", parts)
		}

		qpr, _, err = searchNoTotal("service", 0)
		require.NoError(t, err, "should be no errors")
		assert.Equal(t, uint64(0), qpr.Total, "we must get Total = 0 in withTotal=false mode")
		assert.Equal(t, size, len(qpr.IDs), "we must get only size ids")
		for i := 0; i < parts; i++ {
			k := "x" + strconv.Itoa(i)
			assert.Equal(t, int(aggCnt), int(qpr.Aggs[0].SamplesByBin[seq.AggBin{Token: k}].Total), "we expect 1/%d of all documents", parts)
		}

		// histogram
		qpr, _, err = searchWithTotal("", histInterval)
		require.NoError(t, err, "should be no errors")
		assert.Equal(t, uint64(allDocsNum), qpr.Total, "we must scann all docs in withTotal=true mode")
		assert.Equal(t, size, len(qpr.IDs), "we must get only size ids")
		assert.Equal(t, histCnt, len(qpr.Histogram))
		histSum := uint64(0)
		for _, v := range qpr.Histogram {
			histSum += v
		}
		assert.Equal(t, uint64(allDocsNum), histSum, "the sum of the histogram should be equal to the number of all documents")

		qpr, _, err = searchNoTotal("", histInterval)
		require.NoError(t, err, "should be no errors")
		assert.Equal(t, uint64(0), qpr.Total, "we must get Total = 0 in withTotal=false mode")
		assert.Equal(t, size, len(qpr.IDs), "we must get only size ids")
		assert.Equal(t, histCnt, len(qpr.Histogram))

		histSum = uint64(0)
		for _, v := range qpr.Histogram {
			histSum += v
		}
		assert.Equal(t, uint64(allDocsNum), histSum, "the sum of the histogram should be equal to the number of all documents")
	}

	s.T().Run("ActiveFraction", test)
	env.SealAll()
	s.T().Run("SealedFraction", test)
}

func (s *IntegrationTestSuite) TestSeal() {
	env := setup.NewTestingEnv(s.Config)

	bulksNum := getBulkIterationsNum(env)
	iterations := bulksNum
	result := 174746 * iterations
	for i := 0; i < iterations; i++ {
		file, err := os.Open(common.TestDataDir + "/k8s.logs")
		require.NoError(s.T(), err)
		reader := bufio.NewScanner(file)

		var payload []byte
		lines := 0
		for reader.Scan() {
			line := reader.Bytes()
			lines++
			payload = append(payload, `{"index":true}`...)
			payload = append(payload, '\n')
			payload = append(payload, line...)
			payload = append(payload, '\n')
		}
		require.NoError(s.T(), file.Close())
		require.True(s.T(), lines > 1024)

		resp, err := http.Post(env.IngestorBulkAddr(), "", bytes.NewReader(payload))
		assert.NoError(s.T(), err, "should be no errors")
		if resp.StatusCode != http.StatusOK {
			body, err := io.ReadAll(resp.Body)
			require.NoError(s.T(), err)
			s.T().Fatalf("wrong http status: %d: %s", resp.StatusCode, body)
		}
		esResp := struct {
			Items []json.RawMessage `json:"items"`
		}{}
		require.NoError(s.T(), json.NewDecoder(resp.Body).Decode(&esResp))
		require.Equal(s.T(), lines, len(esResp.Items))
		require.NoError(s.T(), resp.Body.Close())
	}

	env.WaitIdle()
	for _, withTotal := range []bool{true, false} {
		qpr, _, err := env.Search(`status:200`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(result, withTotal), qpr.Total, "wrong doc count")
	}

	env.WaitIdle()
	env.SealAll()

	for _, withTotal := range []bool{true, false} {
		qpr, _, err := env.Search(`status:200`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(result, withTotal), qpr.Total, "wrong doc count")
	}

	env.StopAll()

	time.Sleep(time.Millisecond * 100)

	env = setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	for _, withTotal := range []bool{true, false} {
		qpr, _, err := env.Search(`status:200`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(result, withTotal), qpr.Total, "wrong doc count")
	}
}

func (s *IntegrationTestSuite) TestQueryErr() {
	origDocs := []string{
		`{"service":"a", "xxxx":"yyyy"}`,
		`{"service":"a", "yyyy":"xxxx"}`,
	}

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)

	for _, withTotal := range []bool{true, false} {
		_, _, err := env.Search(`service:a:`, 1000, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.True(s.T(), err != nil, "should be an error")
	}
}

func (s *IntegrationTestSuite) TestConnectionRefused() {
	s.T().Skip() // temporary skip this test until we fix it in CORELOG-299

	env := setup.NewTestingEnv(s.Config)
	env.StopStore()
	defer env.StopAll()

	go func() {
		bulkQueue := [][]byte{
			[]byte(`{"service":"a", "xxxx":"yyyy"}`),
			[]byte(`{"service":"a", "yyyy":"xxxx"}`),
		}
		_, _ = env.Ingestor().BulkIngestor.ProcessDocuments(context.Background(), time.Now(), func() ([]byte, error) {
			if len(bulkQueue) == 0 {
				return nil, nil
			}
			next := bulkQueue[0]
			bulkQueue = bulkQueue[1:]
			return next, nil
		})
	}()
	_, _, err := env.Search(`service:a`, 1000, setup.NoFetch())

	if assert.True(s.T(), err != nil, "should be an error") {
		assert.True(s.T(), strings.Contains(err.Error(), "connection refused"), "error should be connection refused")
	}
}

func (s *IntegrationTestSuite) TestSearchProxyTimeout() {
	if s.Config.Name != configBasic {
		s.T().Skip("no need to run in", s.Config.Name, "env")
	}

	origDocs := []string{
		`{"service":"a", "xxxx":"yyyy"}`,
		`{"service":"a", "yyyy":"xxxx"}`,
	}

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	cancel()

	sr := search.SearchRequest{
		Explain:     false,
		Q:           []byte(`service:a`),
		Offset:      0,
		Size:        1000,
		Interval:    0,
		From:        0,
		To:          math.MaxUint64,
		WithTotal:   true,
		ShouldFetch: true,
	}

	_, _, _, err := env.Ingestor().SearchIngestor.Search(ctx, &sr, nil)
	assert.Error(s.T(), err, "should be error")

	sr.WithTotal = false
	_, _, _, err = env.Ingestor().SearchIngestor.Search(ctx, &sr, nil)
	assert.Error(s.T(), err, "should be error")
}

func (s *IntegrationTestSuite) TestSearchStoreTimeout() {
	if s.Config.Name != configBasic {
		s.T().Skip("no need to run in", s.Config.Name, "env")
	}

	origDocs := []string{
		`{"service":"a", "xxxx":"yyyy"}`,
		`{"service":"a", "yyyy":"xxxx"}`,
	}

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	cancel()

	data, err := env.HotStores[0][0].GrpcV1().Search(ctx, &storeapi.SearchRequest{
		Query:       "service:a",
		From:        0,
		To:          math.MaxInt64,
		Size:        100,
		Offset:      0,
		Interval:    0,
		Aggregation: "",
		Explain:     false,
	})
	assert.Error(s.T(), err, "should be a (timeout) error")
	assert.Nil(s.T(), data)
}

func (s *IntegrationTestSuite) TestBulkBadTimestamp() {
	type Doc struct {
		Service string `json:"service"`
		Level   string `json:"level"`
		Time    string `json:"time"`
	}

	doc1 := `{"service": "a", "level": "INFO", "time": "2021-01-01T00:00:00Z"}`       // this time is too old
	doc2 := fmt.Sprintf(`{"service":"a","time":%q}`, time.Now().Format(time.RFC3339)) // this doc will go as is

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	setup.Bulk(s.T(), env.IngestorBulkAddr(), []string{doc1, doc2})
	env.WaitIdle()

	for _, o := range []seq.DocsOrder{seq.DocsOrderAsc, seq.DocsOrderDesc} {
		for _, withTotal := range []bool{true, false} {
			qpr, docs, err := env.Search(`service:a`, 1000, setup.WithTotal(withTotal), setup.WithOrder(o))
			assert.NoError(s.T(), err, "should be no errors")

			if o.IsReverse() {
				slices.Reverse(docs)
			}

			assert.Len(s.T(), qpr.IDs, 2, "wrong doc count")
			if assert.Equal(s.T(), getTotal(2, withTotal), qpr.Total, "wrong doc count") {
				assert.Equal(s.T(), doc2, string(docs[1]), "wrong doc content")

				// check correct time was applied to doc
				origDoc := &Doc{}
				_ = json.Unmarshal([]byte(doc1), origDoc)

				doc := &Doc{}
				err = json.Unmarshal(docs[0], doc)
				assert.NoError(s.T(), err, "json from search should be valid")

				assert.Equal(s.T(), origDoc.Service, doc.Service, "service field should be equal")
				assert.Equal(s.T(), origDoc.Level, doc.Level, "level field should be equal")
			}
		}
	}
}

const configBasic = "Basic"

func TestBasicIntegration(t *testing.T) {
	cfg := setup.TestingEnvConfig{
		Name:          configBasic,
		IngestorCount: 1,
		HotShards:     1,
		HotFactor:     1,
	}
	t.Parallel()
	dd := &IntegrationTestSuite{Base: *suites.NewBase(&cfg)}
	suite.Run(t, dd)
}

func TestColdStoreIntegration(t *testing.T) {
	cfg := setup.TestingEnvConfig{
		Name:           "WithColdStore",
		IngestorCount:  1,
		ColdShards:     1,
		ColdFactor:     1,
		HotShards:      1,
		HotFactor:      1,
		HotModeEnabled: false,
	}
	t.Parallel()
	dd := &IntegrationTestSuite{Base: *suites.NewBase(&cfg)}
	suite.Run(t, dd)
}

func TestColdHotStoreIntegration(t *testing.T) {
	cfg := setup.TestingEnvConfig{
		Name:           "WithColdAndHotStoreEnabled",
		IngestorCount:  2,
		ColdShards:     1,
		ColdFactor:     1,
		HotShards:      1,
		HotFactor:      1,
		HotModeEnabled: true,
	}
	t.Parallel()
	dd := &IntegrationTestSuite{Base: *suites.NewBase(&cfg)}
	suite.Run(t, dd)
}

func TestBigWithReplicasIntegration(t *testing.T) {
	cfg := setup.TestingEnvConfig{
		Name:           "BigWithReplicas",
		IngestorCount:  2,
		ColdShards:     4,
		ColdFactor:     1,
		HotShards:      4,
		HotFactor:      1,
		HotModeEnabled: true,
	}
	t.Parallel()
	dd := &IntegrationTestSuite{Base: *suites.NewBase(&cfg)}
	suite.Run(t, dd)
}

func (s *IntegrationTestSuite) TestDownsamplePropagation() {
	t := s.T()
	r := require.New(t)

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	const (
		docsPerBulk      = 200
		tolerancePercent = 0.5
	)

	// Setup data with status field for aggregations.
	bulksNum := getBulkIterationsNum(env)
	totalDocs := docsPerBulk * bulksNum

	origDocs := make([]string, docsPerBulk)
	for j := 0; j < bulksNum; j++ {
		baseIdx := j * docsPerBulk
		for i := range origDocs {
			origDocs[i] = fmt.Sprintf(`{"service":"a","id":%d,"status":%d}`, baseIdx+i, i%3)
		}
		setup.Bulk(t, env.IngestorBulkAddr(), origDocs)
	}

	type testCase struct {
		name       string
		downsample *uint32 // nil = option not passed
		wantAll    bool    // expect all documents returned
	}

	cases := []testCase{{
		name:       "no downsample option",
		downsample: nil,
		wantAll:    true,
	}, {
		name:       "downsample=0",
		downsample: ptr[uint32](0),
		wantAll:    true,
	}, {
		name:       "downsample=1",
		downsample: ptr[uint32](1),
		wantAll:    true,
	}, {
		name:       "downsample=10",
		downsample: ptr[uint32](10),
		wantAll:    false,
	}}

	env.WaitIdle()

	for _, tc := range cases {
		opts := []setup.SearchOption{setup.NoFetch(), setup.WithTotal(true)}
		if tc.downsample != nil {
			opts = append(opts, setup.WithDownsample(*tc.downsample))
		}

		resp := env.HTTPSearch(t, `service:a`, math.MaxInt32, opts...)
		r.Equal(seqproxyapi.ErrorCode_ERROR_CODE_NO, resp.Error.Code, "store search with %s should succeed", tc.name)

		if tc.wantAll {
			r.Equal(totalDocs, len(resp.Docs), "store search %s: should return all %d docs", tc.name, totalDocs)
		} else {
			r.Greater(len(resp.Docs), 0, "store search %s: should return at least some results", tc.name)
			// downsample=N: expect approximately total/N docs with ±3% tolerance.
			ds := int(*tc.downsample)
			delta := float64(totalDocs/ds) * tolerancePercent
			r.InDelta(totalDocs/ds, len(resp.Docs), delta,
				"store search %s: should return ~%d docs", tc.name, totalDocs/ds)
		}

		r.Equal(int64(totalDocs), resp.Total,
			"store search %s: Total should reflect full count (%d)", tc.name, totalDocs)
	}
}

// ptr returns a pointer to the given value.
func ptr[T any](v T) *T {
	return &v
}

func (s *IntegrationTestSuite) TestDocuments() {
	n := 32
	env, origDocs := s.envWithDummyDocs(n)
	defer env.StopAll()

	env.WaitIdle()

	for _, o := range []seq.DocsOrder{seq.DocsOrderAsc, seq.DocsOrderDesc} {
		qpr, _, err := env.Search(`service:a`, n, setup.WithTotal(true), setup.NoFetch(), setup.WithOrder(o))
		s.Assert().NoError(err)
		s.Assert().Equal(getTotal(len(origDocs), true), qpr.Total, "wrong doc count")

		ctx, cancel := context.WithCancel(context.Background())

		docsStream, err := env.Ingestor().SearchIngestor.Documents(ctx, search.FetchRequest{IDs: qpr.IDs.IDs()})
		s.Assert().NoError(err)

		actualDocs := []string{}
		actualIDs := []seq.ID{}
		for doc := range search.DocsIteratorSeq(docsStream) {
			actualIDs = append(actualIDs, doc.ID)
			actualDocs = append(actualDocs, string(doc.Data))
		}
		s.Assert().Equal(qpr.IDs.IDs(), actualIDs)

		copyDocs := copySlice(origDocs)
		if o.IsReverse() {
			slices.Reverse(copyDocs)
		}

		s.Assert().Equal(copyDocs[:n], actualDocs)
		cancel()
	}
}

func copySlice[V any](src []V) []V {
	dst := make([]V, len(src))
	copy(dst, src)
	return dst
}

func (s *IntegrationTestSuite) TestSearchFieldsWithMultipleTypes() {
	t := s.T()

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	docs := []string{
		`{"service":"a", "message":"doc one"}`,
		`{"service":"b", "message":"doc one"}`,
		`{"service":"a", "message":"doc two"}`,
		`{"service":"b", "message":"doc two"}`,
		`{"service":"a", "message":"doc three"}`,
		`{"service":"b", "message":"doc three"}`,
	}

	setup.Bulk(s.T(), env.IngestorBulkAddr(), docs)
	env.WaitIdle()

	type testCase struct {
		title, request string
		cnt            int
	}

	tests := []testCase{
		{title: "text field", request: "message:doc", cnt: 6},
		{title: "keyword field no matches", request: "message.keyword:\"doc\"", cnt: 0},
		{title: "keyword field wildcard", request: "message.keyword:\"doc*\"", cnt: 6},
		{title: "keyword field exact match 1", request: "message.keyword:\"doc one\"", cnt: 2},
		{title: "keyword field exact match 2", request: "message.keyword:\"doc two\"", cnt: 2},
	}

	test := func(tc testCase) func(t *testing.T) {
		return func(t *testing.T) {
			qpr, _, err := env.Search(tc.request, 100, setup.WithTotal(true))
			require.NoError(t, err)
			assert.Len(t, qpr.IDs, tc.cnt)
			assert.Equal(t, tc.cnt, int(qpr.Total))
		}
	}

	for _, tc := range tests {
		t.Run(tc.title, test(tc))
	}

	env.WaitIdle()
	env.SealAll()

	for _, tc := range tests {
		t.Run(tc.title, test(tc))
	}
}

func (s *IntegrationTestSuite) TestAggregateFieldsWithMultipleTypes() {
	t := s.T()

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	docs := []string{
		`{"service":"a", "message":"doc one", "level":"error"}`,
		`{"service":"a", "message":"doc two", "level":"error"}`,
		`{"service":"b", "message":"doc two", "level":"error"}`,
		`{"service":"a", "message":"doc three", "level":"error"}`,
		`{"service":"b", "message":"doc three", "level":"error"}`,
		`{"service":"c", "message":"doc three", "level":"error"}`,
		`{"service":"c", "message":"doc info", "level":"info"}`,
		`{"service":"c", "message":true, "level":"error"}`,
		`{"service":"c", "message":true, "level":"error"}`,
		`{"service":"c", "message":false, "level":"error"}`,
		`{"service":"c", "message":false, "level":"info"}`,
	}

	setup.Bulk(s.T(), env.IngestorBulkAddr(), docs)
	env.WaitIdle()

	qpr, _, err := env.Search(
		"level:error",
		100,
		setup.WithAggQuery(search.AggQuery{Field: "message.keyword", Func: seq.AggFuncCount}),
	)
	require.NoError(t, err)

	gotBuckets := qpr.Aggregate([]seq.AggregateArgs{{Func: seq.AggFuncCount}})

	assert.Equal(t, 1, len(gotBuckets))
	assert.ElementsMatch(
		t,
		[]seq.AggregationBucket{
			{Name: "doc three", Value: 3},
			{Name: "doc two", Value: 2},
			{Name: "doc one", Value: 1},
			{Name: "true", Value: 2},
			{Name: "false", Value: 1},
		},
		gotBuckets[0].Buckets,
	)
}

// TestTimeField checks that if time in document exceeds PastDrift or FutureDrift
// time field is replaced with time.Now()
func (s *IntegrationTestSuite) TestTimeField() {
	config := *s.Config
	config.Mapping = map[string]seq.MappingTypes{
		"event":   seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"message": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
	}

	env := setup.NewTestingEnv(&config)
	defer env.StopAll()

	docs := []string{
		`{"level": "info","ts": "2000-01-13T17:36:10.593303253Z","logger": "fd.kubelet","message": "pipeline stats","stat": "interval=5s, active procs=0/4, events in use=0/256, out=0|0.0Mb, rate=0/s|0.0Mb/s, read ops=0/s, total=0|0.0Mb, avg size=0"}`,
		`{"level": "info","ts": "2000-01-13T17:36:12.790469375Z","logger": "fd.k8s.input.k8s","message": "file plugin stats for last 5 seconds: offsets saves=104111, jobs done=28, jobs total=28"}`,
		`{"level": "info","ts": "3000-01-13T17:36:14.715199225Z","logger": "fd.k8s.action.debug","message": "input event sample","offset": 40059539,"event": {"log": "{\"level\":\"info\",\"ts\":\"2025-01-13T17:36:08.729825704Z\",\"logger\":\"fd.dmesg\",\"message\":\"pipeline stats\",\"stat\":\"interval=5s, active procs=0/2, events in use=0/128, out=0|0.0Mb, rate=0/s|0.0Mb/s, read ops=0/s, total=100857|15.0Mb, avg size=1\"}\n","time": "2025-01-13T17:36:08.729920774Z","stream": "stderr","k8s_container_id": "52f2ab19fe0ba66f4f4e7910780da1e477be98015db58dc624a26c4a585e096b","app_label": "dmesg-reader-z504","pod_app": "dmesg-reader-z504-wjfh5@dmesg-reader-z504","k8s_pod": "dmesg-reader-z504-wjfh5","k8s_namespace": "logging","k8s_container": "dmesg-reader","k8s_node": "infrakuben87742z504","k8s_pod_label_app": "dmesg-reader-z504","k8s_node_label_topology.kubernetes.io/zone": "z504"}}`,
	}

	setup.Bulk(s.T(), env.IngestorBulkAddr(), docs)
	env.WaitIdle()

	r := require.New(s.T())

	now := time.Now()
	resp := setup.SearchHTTP(s.T(), env.IngestorSearchAddr(), &seqproxyapi.SearchRequest{
		Query: &seqproxyapi.SearchQuery{
			Query:   "",
			From:    timestamppb.New(now.Add(-time.Hour)),
			To:      timestamppb.New(now.Add(time.Hour)),
			Explain: false,
		},
		Size:      10,
		Offset:    0,
		WithTotal: false,
	})

	r.Equal(len(docs), len(resp.Docs))
}

func (s *IntegrationTestSuite) TestAsyncSearch() {
	t := s.T()
	r := require.New(t)
	now := time.Now()

	cfg := *s.Config
	cfg.Mapping = map[string]seq.MappingTypes{
		"ip":     seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"method": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"uri":    seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"status": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"size":   seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
	}
	env := setup.NewTestingEnv(&cfg)
	defer env.StopAll()

	docs := []string{
		`{"timestamp":"2009-11-10T22:58:44Z","ip":"226.166.207.153","method":"PUT","uri":"/api/data","status":201,"size":5116}`,
		`{"timestamp":"2009-11-10T22:54:26Z","ip":"211.170.224.81","method":"GET","uri":"/api/data","status":500,"size":2375}`,
		`{"timestamp":"2009-11-10T22:57:28Z","ip":"13.30.65.187","method":"POST","uri":"/","status":201,"size":3892}`,
		`{"timestamp":"2009-11-10T22:44:01Z","ip":"181.10.24.51","method":"GET","uri":"/api/data","status":201,"size":4002}`,
		`{"timestamp":"2009-11-10T22:53:51Z","ip":"107.2.249.68","method":"PUT","uri":"/dashboard","status":400,"size":4334}`,
		`{"timestamp":"2009-11-10T22:52:50Z","ip":"70.83.163.58","method":"DELETE","uri":"/","status":400,"size":2525}`,
		`{"timestamp":"2009-11-10T22:55:31Z","ip":"106.51.48.84","method":"DELETE","uri":"/api/data","status":400,"size":3015}`,
		`{"timestamp":"2009-11-10T22:58:54Z","ip":"117.81.168.0","method":"GET","uri":"/","status":404,"size":4734}`,
		`{"timestamp":"2009-11-10T22:58:04Z","ip":"132.240.243.74","method":"PUT","uri":"/login","status":400,"size":1598}`,
		`{"timestamp":"2009-11-10T22:46:58Z","ip":"222.36.179.145","method":"GET","uri":"/dashboard","status":404,"size":2683}`,
	}

	// Create active and sealed fractions.
	setup.Bulk(s.T(), env.IngestorBulkAddr(), docs)
	env.WaitIdle()

	searcher := env.Ingestor().Ingestor.SearchIngestor

	ctx := t.Context()

	searchIDs := make([]string, 0)

	// StartAsyncSearch

	startReq := search.AsyncRequest{
		Query:     "* | fields ip, method, uri",
		From:      now.UTC().Truncate(time.Millisecond),
		To:        now.UTC().Add(time.Minute).Truncate(time.Millisecond),
		Retention: time.Minute * 5,
		Aggregations: []search.AggQuery{
			{
				Field:   "size",
				GroupBy: "ip",
				Func:    seq.AggFuncSum,
			},
			{
				Field:     "size",
				GroupBy:   "method",
				Func:      seq.AggFuncQuantile,
				Quantiles: []float64{0.99, 0.95, 0.50},
			},
		},
		HistogramInterval: seq.MID(time.Second.Nanoseconds()),
		WithDocs:          true,
		Size:              100,
	}
	resp, err := searcher.StartAsyncSearch(ctx, startReq)
	r.NoError(err)
	r.NotEmpty(resp.ID)
	searchIDs = append(searchIDs, resp.ID)

	// FetchAsyncSearchResult

	freq := search.FetchAsyncSearchResultRequest{
		ID:     resp.ID,
		Size:   100,
		Offset: 0,
	}

	r.Eventually(func() bool {
		resp, _, err := searcher.FetchAsyncSearchResult(ctx, freq)
		r.NoError(err)
		return resp.Status == asyncsearcher.AsyncSearchStatusDone
	}, 10*time.Second, 50*time.Millisecond)

	fresp, _, err := searcher.FetchAsyncSearchResult(ctx, freq)
	r.NoError(err)

	r.Equalf(asyncsearcher.AsyncSearchStatusDone, fresp.Status, "unexpected status code=%d with error=%q", fresp.Status, fresp.QPR.Errors)
	r.Equal([]seq.ErrorSource(nil), fresp.QPR.Errors)
	r.True(fresp.ExpiresAt.After(time.Now().UTC()))
	r.Equal([]seq.AggregationResult{
		{
			Buckets: []seq.AggregationBucket{
				{Name: "226.166.207.153", Value: 5116},
				{Name: "117.81.168.0", Value: 4734},
				{Name: "107.2.249.68", Value: 4334},
				{Name: "181.10.24.51", Value: 4002},
				{Name: "13.30.65.187", Value: 3892},
				{Name: "106.51.48.84", Value: 3015},
				{Name: "222.36.179.145", Value: 2683},
				{Name: "70.83.163.58", Value: 2525},
				{Name: "211.170.224.81", Value: 2375},
				{Name: "132.240.243.74", Value: 1598},
			},
		},
		{
			Buckets: []seq.AggregationBucket{
				{Name: "delete", Value: 3015, Quantiles: []float64{3015, 3015, 3015}},
				{Name: "get", Value: 4734, Quantiles: []float64{4734, 4734, 4002}},
				{Name: "post", Value: 3892, Quantiles: []float64{3892, 3892, 3892}},
				{Name: "put", Value: 5116, Quantiles: []float64{5116, 5116, 4334}},
			},
		},
	}, fresp.AggResult)
	r.Equal(startReq, fresp.Request)

	r.True(len(fresp.QPR.Histogram) != 0)
	r.Equal(len(docs), fresp.QPR.IDs.Len())
	r.Equal(float64(1), fresp.Progress)

	// GetAsyncSearchesList

	startResp, err := searcher.StartAsyncSearch(ctx, startReq)
	r.NoError(err)
	r.NotEmpty(startResp.ID)
	searchIDs = append(searchIDs, startResp.ID)
	freq.ID = startResp.ID

	r.Eventually(func() bool {
		resp, _, err := searcher.FetchAsyncSearchResult(ctx, freq)
		r.NoError(err)
		return resp.Status == asyncsearcher.AsyncSearchStatusDone
	}, 10*time.Second, 50*time.Millisecond)

	listResp, err := searcher.GetAsyncSearchesList(ctx, search.GetAsyncSearchesListRequest{})
	r.NoError(err)
	r.Len(listResp, 2)

	for i, s := range listResp {
		r.True(s.ID == searchIDs[len(searchIDs)-i-1]) // list is sorted by startedAt desc
		r.Equal(asyncsearcher.AsyncSearchStatusDone, s.Status)
		r.Equal(startReq, s.Request)
		r.True(s.ExpiresAt.After(time.Now().UTC()))
		r.Equal(float64(1), s.Progress)
	}

	// DeleteAsyncSearch

	err = searcher.DeleteAsyncSearch(ctx, startResp.ID)
	r.NoError(err)

	r.Eventually(func() bool {
		listResp, err := searcher.GetAsyncSearchesList(ctx, search.GetAsyncSearchesListRequest{})
		r.NoError(err)
		return len(listResp) == 1
	}, 10*time.Second, 50*time.Millisecond)
}

func (s *IntegrationTestSuite) TestPaginationWithOffsetAndSize() {
	t := s.T()
	r := require.New(t)

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	docsPerBulk := 100
	bulksNum := getBulkIterationsNum(env)
	totalDocs := docsPerBulk * bulksNum

	for j := 0; j < bulksNum; j++ {
		var bulk []string
		for i := 0; i < docsPerBulk; i++ {
			bulk = append(bulk, fmt.Sprintf(`{"service":"api-gateway", "doc":"%d"}`, j*docsPerBulk+i))
		}
		setup.Bulk(s.T(), env.IngestorBulkAddr(), bulk)
	}
	env.WaitIdle()

	fetchedIDs := make(map[string]bool)
	fetchedDocs := make(map[string]bool)
	offset := 0
	pageSize := 53

	for _, order := range []seq.DocsOrder{seq.DocsOrderDesc, seq.DocsOrderAsc} {
		for {
			qpr, docs, err := env.Search(`service:*`, pageSize, setup.WithOffset(offset), setup.WithOrder(order))
			r.NoError(err, "search failed")

			if len(qpr.IDs) == 0 {
				break
			}

			for i, doc := range docs {
				docID := qpr.IDs[i].ID.String()
				r.False(fetchedIDs[docID], "seen some doc ID twice")
				fetchedIDs[docID] = true
				docStr := string(doc)
				r.False(fetchedDocs[docStr], "seen some doc twice")
				fetchedDocs[string(doc)] = true
			}

			offset += len(qpr.IDs)
		}

		r.Equal(totalDocs, len(fetchedIDs), "total doc IDs count does not match")
		r.Equal(totalDocs, len(fetchedDocs), "count of unique docs does not match")
	}
}

func (s *IntegrationTestSuite) TestPaginationWithOffsetId() {
	t := s.T()
	r := require.New(t)

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	docsPerBulk := 100
	bulksNum := getBulkIterationsNum(env)
	totalDocs := docsPerBulk * bulksNum

	for j := 0; j < bulksNum; j++ {
		var bulk []string
		for i := 0; i < docsPerBulk; i++ {
			bulk = append(bulk, fmt.Sprintf(`{"service":"api-gateway", "doc":"%d"}`, j*docsPerBulk+i))
		}
		setup.Bulk(s.T(), env.IngestorBulkAddr(), bulk)
	}
	env.WaitIdle()

	pageSize := 53

	for _, order := range []seq.DocsOrder{seq.DocsOrderDesc, seq.DocsOrderAsc} {

		fetchedIDs := make(map[string]bool)
		fetchedDocs := make(map[string]bool)
		var offsetId string
		for {
			qpr, docs, err := env.Search(`service:*`, pageSize, setup.WithOffsetId(offsetId), setup.WithOrder(order))
			r.NoError(err, "search failed")

			if len(qpr.IDs) == 0 {
				break
			}

			for i, doc := range docs {
				docID := qpr.IDs[i].ID.String()
				r.False(fetchedIDs[docID], "doc ID has appeared more than once")
				fetchedIDs[docID] = true
				docStr := string(doc)
				r.False(fetchedDocs[docStr], "doc has appeared more than once")
				fetchedDocs[string(doc)] = true
			}

			offsetId = qpr.IDs.IDs()[qpr.IDs.Len()-1].String()
		}

		r.Equal(totalDocs, len(fetchedIDs), "total doc IDs count does not match")
		r.Equal(totalDocs, len(fetchedDocs), "count of unique docs does not match")
	}
}

func (s *IntegrationTestSuite) TestSkipMaskManager() {
	t := s.T()
	r := require.New(t)

	cfg := *s.Config
	env := setup.NewTestingEnv(&cfg)

	docs := []string{
		`{"service":"visible", "message":"doc1"}`,
		`{"service":"hidden", "message":"doc2"}`,
		`{"service":"visible", "message":"doc3"}`,
		`{"service":"hidden", "message":"doc4"}`,
	}
	setup.Bulk(t, env.IngestorBulkAddr(), docs)
	env.WaitIdle()
	env.SealAll()

	// bulk docs one more time to have sealed and active fracs
	setup.Bulk(t, env.IngestorBulkAddr(), docs)

	// save hidden doc ids to test fetch later
	qpr, _, err := env.Search(`service:hidden`, 10, setup.WithTotal(true))
	r.NoError(err)
	hiddenDocIDs := qpr.IDs.IDs()

	env.WaitIdle()
	env.StopAll()

	cfg.SkipMaskParams = []skipmaskmanager.SkipMaskParams{
		{
			Query: "service:hidden",
			From:  0,
			To:    seq.TimeToMID(time.Now()),
		},
	}
	env = setup.NewTestingEnv(&cfg)
	defer env.StopAll()

	var checkSkipMasksStatus = func(stores setup.Stores) bool {
		for _, ss := range stores {
			for _, s := range ss {
				if !s.SkipMaskManager.IsDone() {
					return false
				}
			}
		}
		return true
	}

	// wait for skip masks processing
	r.Eventually(func() bool {
		return checkSkipMasksStatus(env.HotStores) && checkSkipMasksStatus(env.ColdStores)
	}, 5*time.Second, 100*time.Millisecond)

	// test search

	qpr, _, err = env.Search(`service:hidden`, 10, setup.WithTotal(true))
	r.NoError(err)
	r.Equal(uint64(0), qpr.Total)

	qpr, _, err = env.Search(`service:*`, 10, setup.WithTotal(true))
	r.NoError(err)
	r.Equal(uint64(4), qpr.Total)

	// test fetch

	fetchedDocs, err := env.Fetch(hiddenDocIDs)
	r.NoError(err)
	r.Len(fetchedDocs, len(hiddenDocIDs))
	for _, doc := range fetchedDocs {
		r.Len(doc, 0) // fetch hiddenID returns nothing
	}

	// refresh frac

	env.WaitIdle()
	env.SealAll()

	// wait for skip masks processing
	r.Eventually(func() bool {
		return checkSkipMasksStatus(env.HotStores) && checkSkipMasksStatus(env.ColdStores)
	}, 5*time.Second, 100*time.Millisecond)

	qpr, _, err = env.Search(`service:hidden`, 10, setup.WithTotal(true))
	r.NoError(err)
	r.Equal(uint64(0), qpr.Total)
}

// newStreamSearchClient connects to a random ingestor's gRPC endpoint and opens
// a StreamSearch stream.
func newStreamSearchClient(t *testing.T, env *setup.TestingEnv) (
	seqproxyapi.SeqProxyApi_StreamSearchClient,
	*grpc.ClientConn,
	context.Context,
	context.CancelFunc,
) {
	t.Helper()
	addr := env.Ingestor().Config.API.GatewayAddr
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := seqproxyapi.NewSeqProxyApiClient(conn).StreamSearch(ctx)
	require.NoError(t, err)
	return stream, conn, ctx, cancel
}

// sendStreamSearchQuery sends the initial search query on the stream.
func sendStreamSearchQuery(t *testing.T, stream seqproxyapi.SeqProxyApi_StreamSearchClient, query string) {
	t.Helper()
	now := time.Now()
	require.NoError(t, stream.Send(&seqproxyapi.StreamSearchRequest{
		RequestType: &seqproxyapi.StreamSearchRequest_Query{
			Query: &seqproxyapi.StreamSearchQuery{
				Query:     query,
				From:      timestamppb.New(now.Add(-time.Hour)),
				To:        timestamppb.New(now.Add(time.Hour)),
				WithTotal: true,
			},
		},
	}))
}

// collectStreamData reads responses until the summary is received or the stream
// ends. It returns the collected document payloads (the "data" column of each
// record), the record count and the final summary.
func collectStreamData(
	t *testing.T,
	stream seqproxyapi.SeqProxyApi_StreamSearchClient,
) ([][]byte, *seqproxyapi.ResponseSummary) {
	t.Helper()
	var docs [][]byte
	for {
		resp, err := stream.Recv()
		if err != nil {
			return docs, nil
		}
		switch v := resp.ResponseType.(type) {
		case *seqproxyapi.StreamSearchResponse_Header:
			// expected: typing metadata
		case *seqproxyapi.StreamSearchResponse_Data:
			for _, rec := range v.Data.GetBatch().GetRecords() {
				raw := rec.GetRawData()
				if len(raw) > 0 {
					docs = append(docs, raw[len(raw)-1])
				}
			}
		case *seqproxyapi.StreamSearchResponse_Summary:
			return docs, v.Summary
		}
	}
}

func (s *IntegrationTestSuite) TestStreamSearch() {
	t := s.T()
	r := require.New(t)

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	// A large dataset is required so that the response exceeds the gRPC
	// flow-control window: this forces the server to block between batches and
	// actually observe the control actions the client sends mid-stream. With a
	// tiny dataset the server buffers the whole response and finishes before any
	// control message arrives.
	const totalDocs = 20020
	getNextTs := getAutoTsGenerator(time.Now(), -time.Nanosecond)
	origDocs := make([]string, totalDocs)
	for i := range totalDocs {
		origDocs[i] = fmt.Sprintf(`{"service":"a", "trace_id":"%d", "ts":%q}`, i, getNextTs())
	}
	setup.Bulk(t, env.IngestorBulkAddr(), origDocs)
	env.WaitIdle()

	streamQuery := func(limit int) string {
		return fmt.Sprintf(`service:a | limit %d`, limit)
	}

	t.Run("finalize after full stream", func(t *testing.T) {
		stream, conn, _, cancel := newStreamSearchClient(t, env)
		defer cancel()
		defer conn.Close()

		sendStreamSearchQuery(t, stream, streamQuery(totalDocs))
		// No explicit control action: the server must send the summary once the
		// data is exhausted.
		docs, summary := collectStreamData(t, stream)
		r.Len(docs, totalDocs)

		gotDocs := make([]string, 0, len(docs))
		for _, d := range docs {
			gotDocs = append(gotDocs, string(d))
		}
		wantDocs := make([]string, 0, len(origDocs))
		for _, d := range origDocs {
			wantDocs = append(wantDocs, d)
		}
		r.Equal(wantDocs, gotDocs, "streamed documents must match the ingested ones")

		r.NotNil(summary)
		r.Equal(uint64(totalDocs), summary.GetTotal(), "summary total must match the document count")
		r.Equal(seqproxyapi.ErrorCode_ERROR_CODE_NO, summary.GetError().GetCode())
	})

	t.Run("explicit finalize", func(t *testing.T) {
		stream, conn, _, cancel := newStreamSearchClient(t, env)
		defer cancel()
		defer conn.Close()

		sendStreamSearchQuery(t, stream, streamQuery(totalDocs))

		// Read until the first data batch arrives, then ask the server to
		// finalize before all data is consumed.
		var gotRecordsCount int
		finalizeSent := false
		for {
			resp, err := stream.Recv()
			r.NoError(err)

			switch v := resp.ResponseType.(type) {
			case *seqproxyapi.StreamSearchResponse_Data:
				gotRecordsCount += len(v.Data.GetBatch().GetRecords())
				if !finalizeSent {
					require.NoError(t, stream.Send(&seqproxyapi.StreamSearchRequest{
						RequestType: &seqproxyapi.StreamSearchRequest_Control{
							Control: &seqproxyapi.StreamControl{Action: seqproxyapi.ControlAction_FINALIZE},
						},
					}))
					finalizeSent = true
				}
			case *seqproxyapi.StreamSearchResponse_Summary:
				r.True(finalizeSent, "got summary without finalize")
				r.Greater(gotRecordsCount, 0)
				r.Less(gotRecordsCount, totalDocs, "finalize should stop the stream before all data is sent")
				return
			}
		}
	})

	t.Run("cancel mid-stream", func(t *testing.T) {
		stream, conn, _, cancel := newStreamSearchClient(t, env)
		defer cancel()
		defer conn.Close()

		sendStreamSearchQuery(t, stream, streamQuery(totalDocs))

		// Send CANCEL as soon as the first data batch arrives, then keep reading
		// until the stream ends. The server must terminate without a summary.
		canceled := false
		for {
			resp, err := stream.Recv()
			if err != nil {
				r.ErrorIs(err, io.EOF, "cancel must terminate the stream with EOF")
				return
			}
			switch resp.ResponseType.(type) {
			case *seqproxyapi.StreamSearchResponse_Data:
				if !canceled {
					require.NoError(t, stream.Send(&seqproxyapi.StreamSearchRequest{
						RequestType: &seqproxyapi.StreamSearchRequest_Control{
							Control: &seqproxyapi.StreamControl{Action: seqproxyapi.ControlAction_CANCEL},
						},
					}))
					canceled = true
				}
			case *seqproxyapi.StreamSearchResponse_Summary:
				r.Fail("cancel must not produce a summary")
			}
		}
	})

	t.Run("aggregation stream", func(t *testing.T) {
		stream, conn, _, cancel := newStreamSearchClient(t, env)
		defer cancel()
		defer conn.Close()

		sendStreamSearchQuery(t, stream, `service:a | stats count by (trace_id)`)

		var gotBucketsCount int
		for {
			resp, err := stream.Recv()
			if err != nil {
				break
			}
			switch v := resp.ResponseType.(type) {
			case *seqproxyapi.StreamSearchResponse_Data:
				gotBucketsCount += len(v.Data.GetBatch().GetRecords())
			case *seqproxyapi.StreamSearchResponse_Summary:
				return
			}
		}
		r.Equal(totalDocs, gotBucketsCount, "each distinct `trace_id` value should produce one bucket")
	})

	t.Run("missing query is rejected", func(t *testing.T) {
		stream, conn, _, cancel := newStreamSearchClient(t, env)
		defer cancel()
		defer conn.Close()

		require.NoError(t, stream.Send(&seqproxyapi.StreamSearchRequest{
			RequestType: &seqproxyapi.StreamSearchRequest_Control{
				Control: &seqproxyapi.StreamControl{Action: seqproxyapi.ControlAction_FINALIZE},
			},
		}))
		_, err := stream.Recv()
		r.ErrorIs(err, status.Error(codes.InvalidArgument, "first message must be a search query"))
	})
}
