package integration_tests

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
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
	"github.com/ozontech/seq-db/fracmanager"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/seq"
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
		qpr, docs, _, err := env.Search(`service:a`, 1000, setup.WithTotal(withTotal))
		assertSearch(qpr, err)
		if assert.Greater(s.T(), len(docs), 0, "no docs found") {
			assert.Equal(s.T(), origDocs[0], string(docs[0]), "wrong doc content")
		}

		// search first with _exists_
		qpr, docs, _, err = env.Search(`_exists_:service`, 1000, setup.WithTotal(withTotal))
		assertSearch(qpr, err)
		if assert.Greater(s.T(), len(docs), 0, "no docs found") {
			assert.Equal(s.T(), origDocs[0], string(docs[0]), "wrong doc content")
		}

		// search first with NOT _exists_
		qpr, docs, _, err = env.Search(`NOT _exists_:k8s_pod`, 1000, setup.WithTotal(withTotal))
		assertSearch(qpr, err)
		if assert.Greater(s.T(), len(docs), 0, "no docs found") {
			assert.Equal(s.T(), origDocs[0], string(docs[0]), "wrong doc content")
		}

		// search second
		qpr, docs, _, err = env.Search(`k8s_pod:sq-toloka-loader-1788964-dryrun-58hmw`, 1000, setup.WithTotal(withTotal))
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

	qpr, _, _, err := env.Search(`k8s_pod:NO`, 1000, setup.NoFetch())
	assert.NoError(s.T(), err, "should be no errors")
	assert.Len(s.T(), qpr.IDs, 0, "wrong doc count")
	assert.Equal(s.T(), uint64(0), qpr.Total, "wrong doc count")
}

func (s *IntegrationTestSuite) TestSearchBackwards() {
	now := time.Now()
	before := now.Add(-5 * time.Hour)
	origDocs := []string{
		fmt.Sprintf(`{"service":"a","xxxx":"yyyy","time":%q}`, now.Format(time.RFC3339)),
		fmt.Sprintf(`{"service":"a","yyyy":"xxxx","time":%q}`, before.Format(time.RFC3339)),
	}

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)
	env.WaitIdle()

	for _, o := range []seq.DocsOrder{seq.DocsOrderAsc, seq.DocsOrderDesc} {
		for _, withTotal := range []bool{true, false} {
			qpr, docs, _, err := env.Search(`service:a`, 1000, setup.WithTotal(withTotal), setup.WithOrder(o))

			if o.IsReverse() {
				slices.Reverse(docs)
			}

			assert.NoError(s.T(), err, "should be no errors")
			assert.Len(s.T(), qpr.IDs, 2, "wrong doc count")
			assert.Equal(s.T(), origDocs[0], string(docs[0]), "wrong doc content")
			assert.Equal(s.T(), origDocs[1], string(docs[1]), "wrong doc content")
			assert.Equal(s.T(), getTotal(2, withTotal), qpr.Total, "wrong doc count")
		}
	}
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
			qpr, _, _, err := env.Search(`service:a`, math.MaxInt32, setup.NoFetch(), setup.WithTotal(withTotal), setup.WithOrder(o))
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
		qpr, _, _, err := env.Search(`service:a`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
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

	getNextTs := getAutoTsGenerator(time.Now(), -time.Second)

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
		qpr, _, _, err := env.Search(`service:a`, 10, setup.WithTotal(withTotal))
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
			qpr, docs, _, err := env.Search(`service:a`, size, setup.WithTotal(withTotal), setup.WithOrder(o))

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
	getNextTs := getAutoTsGenerator(time.Now(), -time.Second)
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
	qpr, _, _, err := env.Search(`service:*`, 10)
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
		qpr, _, _, err := env.Search(`NOT service:b`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(2*n*bulksNum, withTotal), qpr.Total, "wrong doc count")

		qpr, _, _, err = env.Search(`NOT service:x`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(n*bulksNum, withTotal), qpr.Total, "wrong doc count")

		qpr, _, _, err = env.Search(`NOT service:a AND NOT service:x`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), 0, int(qpr.Total), "wrong doc count")

		qpr, _, _, err = env.Search(`NOT _exists_:service`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), 0, int(qpr.Total), "wrong doc count")

		qpr, _, _, err = env.Search(`NOT _exists_:k8s_pod`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(allDocsNum, withTotal), qpr.Total, "wrong doc count")

		qpr, _, _, err = env.Search(`NOT _exists_:k8s_pod`, -1, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.ErrorIs(s.T(), err, consts.ErrInvalidArgument)
		assert.Nil(s.T(), qpr)

		qpr, _, _, err = env.Search(`NOT _exists_:k8s_pod`, 1, setup.WithOffset(-1),
			setup.NoFetch(), setup.WithTotal(withTotal))
		assert.ErrorIs(s.T(), err, consts.ErrInvalidArgument)
		assert.Nil(s.T(), qpr)
	}

	env.SealAll()

	for _, withTotal := range []bool{true, false} {

		qpr, _, _, err := env.Search(`NOT service:x`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(n*bulksNum, withTotal), qpr.Total, "wrong doc count")

		qpr, _, _, err = env.Search(`NOT service:a AND NOT service:x`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), 0, int(qpr.Total), "wrong doc count")

		qpr, _, _, err = env.Search(`NOT _exists_:service`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), 0, int(qpr.Total), "wrong doc count")

		qpr, _, _, err = env.Search(`NOT _exists_:k8s_pod`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
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
		qpr, _, _, err := env.Search(`service:x*`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(allDocsNum, withTotal), qpr.Total, "wrong doc count")
	}

	env.SealAll()

	for _, withTotal := range []bool{true, false} {
		qpr, _, _, err := env.Search(`service:x*`, 10, setup.WithTotal(withTotal))
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
		qpr, _, _, err := env.Search("message:"+token, 10, setup.NoFetch(), setup.WithTotal(true))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), bulksNum, int(qpr.Total), "wrong doc count for token "+token)
	}

	env.SealAll()

	for _, token := range tokens {
		qpr, _, _, err := env.Search("message:"+token, 10, setup.NoFetch(), setup.WithTotal(true))
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
		qpr, _, _, err := env.Search(`service:x`, 10, setup.NoFetch())
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
		qpr, _, _, err := env.Search(`service:x1`, 10, setup.WithAggQuery("service"), setup.NoFetch(), setup.WithTotal(withTotal))
		r.NoError(err, "should be no errors")
		r.Equal(getTotal(allDocsNum/3, withTotal), qpr.Total, "wrong doc count")
		r.NotNil(qpr.Aggs[0].HistogramByToken["x1"], qpr.Aggs[0].HistogramByToken)
		r.Equal(int64(allDocsNum/3), qpr.Aggs[0].HistogramByToken["x1"].Total, "wrong doc count")

		qpr, _, _, err = env.Search(`service:x*`, 10, setup.WithAggQuery("service"), setup.NoFetch(), setup.WithTotal(withTotal))
		r.NoError(err, "should be no errors")
		r.Equal(getTotal(allDocsNum, withTotal), qpr.Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[0].HistogramByToken["x1"].Total, "wrong doc count")

		aggQ := setup.WithAggQuery(
			"service",
			"k8s_pod",
		)
		qpr, _, _, err = env.Search(`service:x1`, 10, aggQ, setup.NoFetch(), setup.WithTotal(withTotal))
		r.NoError(err, "should be no errors")
		r.Equal(getTotal(allDocsNum/3, withTotal), qpr.Total, "wrong doc count")
		r.Equal(2, len(qpr.Aggs), "wrong agg count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[0].HistogramByToken["x1"].Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[1].HistogramByToken["y1"].Total, "wrong doc count")

		qpr, _, _, err = env.Search(`service:x*`, 10, aggQ, setup.NoFetch(), setup.WithTotal(withTotal))
		r.NoError(err, "should be no errors")
		r.Equal(2, len(qpr.Aggs), "wrong agg count")
		r.Equal(getTotal(allDocsNum, withTotal), qpr.Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[0].HistogramByToken["x1"].Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[1].HistogramByToken["y1"].Total, "wrong doc count")
	}

	env.SealAll()

	for _, withTotal := range []bool{true, false} {
		qpr, _, _, err := env.Search(`service:x1`, 10, setup.WithAggQuery("service"), setup.NoFetch(), setup.WithTotal(withTotal))
		r.NoError(err, "should be no errors")
		r.Equal(getTotal(allDocsNum/3, withTotal), qpr.Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[0].HistogramByToken["x1"].Total, "wrong doc count")

		qpr, _, _, err = env.Search(`service:x*`, 10, setup.WithAggQuery("service"), setup.NoFetch(), setup.WithTotal(withTotal))
		r.NoError(err, "should be no errors")
		r.Equal(getTotal(allDocsNum, withTotal), qpr.Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[0].HistogramByToken["x1"].Total, "wrong doc count")

		aggQ := setup.WithAggQuery(
			"service",
			"k8s_pod",
		)
		qpr, _, _, err = env.Search(`service:x1`, 10, aggQ, setup.NoFetch(), setup.WithTotal(withTotal))
		r.NoError(err, "should be no errors")
		r.Equal(getTotal(allDocsNum/3, withTotal), qpr.Total, "wrong doc count")
		r.Equal(2, len(qpr.Aggs), "wrong agg count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[0].HistogramByToken["x1"].Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[1].HistogramByToken["y1"].Total, "wrong doc count")

		qpr, _, _, err = env.Search(`service:x*`, 10, aggQ, setup.NoFetch(), setup.WithTotal(withTotal))
		r.NoError(err, "should be no errors")
		r.Equal(2, len(qpr.Aggs), "wrong agg count")
		r.Equal(getTotal(allDocsNum, withTotal), qpr.Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[0].HistogramByToken["x1"].Total, "wrong doc count")
		r.Equal(int64(allDocsNum/3), qpr.Aggs[1].HistogramByToken["y1"].Total, "wrong doc count")
	}
}

func (s *IntegrationTestSuite) TestAggStat() {
	t := s.T()

	cfg := *s.Config
	cfg.Mapping = map[string]seq.MappingTypes{
		"service": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"v":       seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"level":   seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
	}

	type Expected struct {
		NotExists int64
		Buckets   []seq.AggregationBucket
	}
	type TestCase struct {
		Name        string
		ToBulk      []string
		SearchQuery string
		AggQuery    search.AggQuery
		Expected    Expected
	}

	tcs := []TestCase{
		{
			Name: "sum",
			ToBulk: []string{
				`{"service": "sum1", "v":1}`,
				`{"service": "some_log", "v":2}`,
				`{"service": "sum1", "v":1}`,
				`{"service": "sum1", "v":-1}`,
				`{"service": "sum1", "v":-0}`,
				`{"service": "sum1", "v":+0}`,
				`{"service": "sum1", "v":0}`,
				`{"service": "sum1"}`,
				// test negative values
				`{"service": "sum2", "v":-1}`,
				`{"service": "sum2", "v":-3}`,
				`{"service": "sum2", "v":-4}`,
				// test same token ("1") repetitions
				`{"service": "sum3", "v":1}`,
				`{"service": "sum4", "v":99}`,
				`{"service": "sum4", "v":1}`,
				`{"service": "sum4", "v":1}`,
				`{"service": "sum4", "v":1}`,
				`{"service": "sum4", "v":1}`,
				`{"service": "sum4", "v":1}`,
				// test sort
				`{"service": "sum5", "v":1}`,
				// test not exists
				`{"service": "sum5"}`,
			},
			SearchQuery: "service:sum*",
			AggQuery: search.AggQuery{
				Field:   "v",
				GroupBy: "service",
				Func:    seq.AggFuncSum,
			},
			Expected: Expected{
				NotExists: 0,
				Buckets: []seq.AggregationBucket{
					{Name: "sum4", Value: 104, NotExists: 0},
					{Name: "sum1", Value: 1, NotExists: 1},
					{Name: "sum3", Value: 1, NotExists: 0},
					{Name: "sum5", Value: 1, NotExists: 1},
					{Name: "sum2", Value: -8, NotExists: 0},
				},
			},
		},
		{
			Name: "min",
			ToBulk: []string{
				`{"service": "min1", "v":1}`,
				`{"service": "min1", "v":2}`,
				`{"service": "min2", "v":3}`,
				`{"service": "min2", "v":"-10"}`,
				`{"service": "min4"}`,
				`{"service": "min4"}`,
				`{"service": "min4"}`,
				`{"service": "min4"}`,
				`{"service": "min4"}`,
				`{"service": "min4"}`,
				`{"service": "min4"}`,
				`{"service": null, "v":null}`,
				`{"v":null}`,
			},
			SearchQuery: "service:min*",
			AggQuery: search.AggQuery{
				Field:   "v",
				GroupBy: "service",
				Func:    seq.AggFuncMin,
			},
			Expected: Expected{
				NotExists: 0,
				Buckets: []seq.AggregationBucket{
					{Name: "min4", Value: math.NaN(), NotExists: 7},
					{Name: "min2", Value: -10, NotExists: 0},
					{Name: "min1", Value: 1, NotExists: 0},
				},
			},
		},
		{
			Name: "max",
			ToBulk: []string{
				`{"service": "max1", "v":1}`,
				`{"service": "max1", "v":2}`,
				`{"service": "max2", "v":3}`,
				`{"service": "max2", "v":"-10"}`,
				`{"service": "max4"}`,
				`{"service": "max4"}`,
				`{"service": null, "v":null}`,
				`{"v":null}`,
			},
			SearchQuery: "service:max*",
			AggQuery: search.AggQuery{
				Field:   "v",
				GroupBy: "service",
				Func:    seq.AggFuncMax,
			},
			Expected: Expected{
				NotExists: 0,
				Buckets: []seq.AggregationBucket{
					{Name: "max2", Value: 3, NotExists: 0},
					{Name: "max1", Value: 2, NotExists: 0},
					{Name: "max4", Value: math.NaN(), NotExists: 2},
				},
			},
		},
		{
			Name: "quantile",
			ToBulk: []string{
				`{"service": "quantile1", "v":1}`,
				`{"service": "quantile1", "v":2}`,
				`{"service": "quantile1", "v":3}`,
				`{"service": "quantile1", "v":4}`,
				`{"service": "quantile1", "v":5}`,
				`{"service": "quantile1", "v":6}`,
				`{"service": "quantile1", "v":7}`,
				`{"service": "quantile1", "v":8}`,
				`{"service": "quantile1", "v":9}`,
				`{"service": "quantile1", "v":10}`,
			},
			SearchQuery: "service:quantile*",
			AggQuery: search.AggQuery{
				Field:     "v",
				GroupBy:   "service",
				Func:      seq.AggFuncQuantile,
				Quantiles: []float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 0.99, 0.999, 0.99999999},
			},
			Expected: Expected{
				NotExists: 0,
				Buckets: []seq.AggregationBucket{
					{
						Name:      "quantile1",
						Value:     1,
						Quantiles: []float64{1, 2, 3, 4, 5, 6, 6, 7, 8, 8, 9, 10, 10, 10},
						NotExists: 0,
					},
				},
			},
		},
		{
			Name: "unique",
			ToBulk: []string{
				`{"service": "some_log", "level": 2}`,
				`{"service": "unique1", "level": 3}`,
				`{"service": "unique2", "level": 3}`,
				`{"service": "unique2", "level": 3}`,
				`{"service": "unique3", "level": 3}`,
				`{"service": "unique3", "level": 2}`,
				`{"service": "unique4", "level": 3}`,
				`{"service": "unique4", "level": 2}`,
				`{"service": "unique4", "level": 3}`,
				`{"service": "unique5", "level": 3}`,
				`{"level": 3}`,
			},
			SearchQuery: "level:3",
			AggQuery: search.AggQuery{
				GroupBy: "service",
				Func:    seq.AggFuncUnique,
			},
			Expected: Expected{
				NotExists: 1,
				Buckets: []seq.AggregationBucket{
					{Name: "unique1", Value: 0, NotExists: 0},
					{Name: "unique2", Value: 0, NotExists: 0},
					{Name: "unique3", Value: 0, NotExists: 0},
					{Name: "unique4", Value: 0, NotExists: 0},
					{Name: "unique5", Value: 0, NotExists: 0},
				},
			},
		},
		{
			Name: "sum without group_by",
			ToBulk: []string{
				`{"v":1, "service":"sum_without_group_by"}`,
				`{"v":1, "service":"sum_without_group_by"}`,
				`{"v":2, "service":"sum_without_group_by"}`,
				`{"v":1, "service":"sum_without_group_by"}`,
				`{"v":1, "service":"sum_without_group_by"}`,
				`{"v":1, "service":"sum_without_group_by"}`,
				`{"v":1, "service":"sum_without_group_by"}`,
				`{"v":2, "service":"sum_without_group_by"}`,
				`{"v":-0, "service":"sum_without_group_by"}`,
				`{"v":+0, "service":"sum_without_group_by"}`,
				`{"v":0, "service":"sum_without_group_by"}`,
			},
			SearchQuery: `service:"sum_without_group_by"`,
			AggQuery:    search.AggQuery{Field: "v", Func: seq.AggFuncSum},
			Expected:    Expected{NotExists: 0, Buckets: []seq.AggregationBucket{{Name: "", Value: 10, NotExists: 0}}},
		},
		{
			Name: "max without group_by",
			ToBulk: []string{
				`{"v":100, "service":"max_without_group_by"}`,
				`{"v":-200, "service":"max_without_group_by"}`,
				`{"v":300, "service":"max_without_group_by"}`,
				`{"v":-300, "service":"max_without_group_by"}`,
			},
			SearchQuery: `service:"max_without_group_by"`,
			AggQuery:    search.AggQuery{Field: "v", Func: seq.AggFuncMax},
			Expected:    Expected{NotExists: 0, Buckets: []seq.AggregationBucket{{Name: "", Value: 300, NotExists: 0}}},
		},
		{
			Name:        "check not_exists without group_by",
			ToBulk:      []string{`{"service":"not_exists_without_group_by"}`},
			SearchQuery: `service:"not_exists_without_group_by"`,
			AggQuery:    search.AggQuery{Field: "v", Func: seq.AggFuncAvg},
			Expected:    Expected{NotExists: 0, Buckets: []seq.AggregationBucket{{Name: "", Value: math.NaN(), NotExists: 1}}},
		},
		{
			Name: "avg without group_by",
			ToBulk: []string{
				`{"v":200, "service":"avg_without_group_by"}`,
				`{"v":500, "service":"avg_without_group_by"}`,
			},
			SearchQuery: `service:"avg_without_group_by"`,
			AggQuery:    search.AggQuery{Field: "v", Func: seq.AggFuncAvg},
			Expected:    Expected{NotExists: 0, Buckets: []seq.AggregationBucket{{Name: "", Value: 350, NotExists: 0}}},
		},
	}

	for i := range tcs {
		tc := &tcs[i]
		t.Run(tc.Name, func(t *testing.T) {
			r := require.New(t)
			env := setup.NewTestingEnv(&cfg)
			defer env.StopAll()

			setup.Bulk(t, env.IngestorBulkAddr(), tc.ToBulk)
			env.WaitIdle()

			qpr, _, _, err := env.Search(tc.SearchQuery, math.MaxInt32, setup.WithAggQuery(tc.AggQuery))
			r.NoError(err)

			gotBuckets := qpr.Aggregate([]seq.AggregateArgs{{Func: tc.AggQuery.Func, Quantiles: tc.AggQuery.Quantiles}})

			r.Equal(1, len(gotBuckets))
			r.Equal(1, len(qpr.Aggs))
			r.Equal(tc.Expected.NotExists, qpr.Aggs[0].NotExists)

			// Handwritten bucket comparison to ignore NaN values
			r.Len(tc.Expected.Buckets, len(gotBuckets[0].Buckets), "wrong bucket count, expected=%v, got=%v", tc.Expected.Buckets, gotBuckets[0])
			for i, expBucket := range tc.Expected.Buckets {
				gotBucket := gotBuckets[0].Buckets[i]
				if math.IsNaN(expBucket.Value) || math.IsNaN(gotBucket.Value) {
					r.Truef(math.IsNaN(expBucket.Value) && math.IsNaN(gotBucket.Value), "wrong bucket value, expected=%v, got=%v", expBucket.Value, gotBucket.Value)
					expBucket.Value = 0
					gotBucket.Value = 0
				}
				r.EqualValues(expBucket, gotBucket)
			}
		})
	}
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

	searchNoTotal := func(agg string, interval time.Duration) (*seq.QPR, [][]byte, time.Duration, error) {
		options := []setup.SearchOption{setup.WithInterval(interval), setup.NoFetch(), setup.WithTotal(false)}
		if agg != "" {
			options = append(options, setup.WithAggQuery(agg))
		}
		return env.Search(`service:x*`, size, options...)
	}

	searchWithTotal := func(agg string, interval time.Duration) (*seq.QPR, [][]byte, time.Duration, error) {
		options := []setup.SearchOption{setup.WithInterval(interval), setup.NoFetch()}
		if agg != "" {
			options = append(options, setup.WithAggQuery(agg))
		}
		return env.Search(`service:x*`, size, options...)
	}

	test := func(t *testing.T) {
		// search
		qpr, _, _, err := searchWithTotal("", 0)
		require.NoError(t, err, "should be no errors")
		assert.Equal(t, uint64(allDocsNum), qpr.Total, "we must scann all docs in withTotal=true mode")
		assert.Equal(t, size, len(qpr.IDs), "we must get only size ids")

		qpr, _, _, err = searchNoTotal("", 0)
		require.NoError(t, err, "should be no errors")
		assert.Equal(t, uint64(0), qpr.Total, "we must get Total = 0 in withTotal=false mode")
		assert.Equal(t, size, len(qpr.IDs), "we must get only size ids")

		// aggregation
		qpr, _, _, err = searchWithTotal("service", 0)
		require.NoError(t, err, "should be no errors")
		assert.Equal(t, uint64(allDocsNum), qpr.Total, "we must scan all docs in withTotal=true mode")
		assert.Equal(t, size, len(qpr.IDs), "we must get only size ids")
		for i := 0; i < parts; i++ {
			k := "x" + strconv.Itoa(i)
			assert.Equal(t, int(aggCnt), int(qpr.Aggs[0].HistogramByToken[k].Total), "we expect 1/%d of all documents", parts)
		}

		qpr, _, _, err = searchNoTotal("service", 0)
		require.NoError(t, err, "should be no errors")
		assert.Equal(t, uint64(0), qpr.Total, "we must get Total = 0 in withTotal=false mode")
		assert.Equal(t, size, len(qpr.IDs), "we must get only size ids")
		for i := 0; i < parts; i++ {
			k := "x" + strconv.Itoa(i)
			assert.Equal(t, int(aggCnt), int(qpr.Aggs[0].HistogramByToken[k].Total), "we expect 1/%d of all documents", parts)
		}

		// histogram
		qpr, _, _, err = searchWithTotal("", histInterval)
		require.NoError(t, err, "should be no errors")
		assert.Equal(t, uint64(allDocsNum), qpr.Total, "we must scann all docs in withTotal=true mode")
		assert.Equal(t, size, len(qpr.IDs), "we must get only size ids")
		assert.Equal(t, histCnt, len(qpr.Histogram))
		histSum := uint64(0)
		for _, v := range qpr.Histogram {
			histSum += v
		}
		assert.Equal(t, uint64(allDocsNum), histSum, "the sum of the histogram should be equal to the number of all documents")

		qpr, _, _, err = searchNoTotal("", histInterval)
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
	result := 51639 * iterations
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
		qpr, _, _, err := env.Search(`level:ERROR`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(result, withTotal), qpr.Total, "wrong doc count")
	}

	env.WaitIdle()
	env.SealAll()

	for _, withTotal := range []bool{true, false} {
		qpr, _, _, err := env.Search(`level:ERROR`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(result, withTotal), qpr.Total, "wrong doc count")
	}

	env.StopAll()

	time.Sleep(time.Millisecond * 100)

	env = setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	for _, withTotal := range []bool{true, false} {
		qpr, _, _, err := env.Search(`level:ERROR`, 10, setup.NoFetch(), setup.WithTotal(withTotal))
		assert.NoError(s.T(), err, "should be no errors")
		assert.Equal(s.T(), getTotal(result, withTotal), qpr.Total, "wrong doc count")
	}
}

func (s *IntegrationTestSuite) TestSearchRange() {
	doc := `{"service": "test-service", "level": "%d"}`

	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	origDocs := []string{}
	for i := 0; i < 100; i = 2*i + 1 {
		origDocs = append(origDocs, fmt.Sprintf(doc, i))
	}
	setup.Bulk(s.T(), env.IngestorBulkAddr(), origDocs)
	env.WaitIdle()

	tests := []struct {
		request string
		cnt     int
	}{
		{
			request: "[1 TO 3]",
			cnt:     2,
		},
		{
			request: "[0 TO 3]",
			cnt:     3,
		},
		{
			request: "{0 TO 3}",
			cnt:     1,
		},
		{
			request: "{0 TO 3]",
			cnt:     2,
		},
		{
			request: "[0 TO 3}",
			cnt:     2,
		},
		{
			request: "[0 TO 63]",
			cnt:     7,
		},
		{
			request: "[-100 TO 100]",
			cnt:     7,
		},
		{
			request: "{-100 TO 100}",
			cnt:     7,
		},
		{
			request: "[0 TO *]",
			cnt:     7,
		},
		{
			request: "[0 TO *}",
			cnt:     7,
		},
	}

	for _, test := range tests {
		for _, withTotal := range []bool{true, false} {
			req := fmt.Sprintf(`level:%v`, test.request)
			qpr, _, _, err := env.Search(req, 1000, setup.WithTotal(withTotal))
			require.NoError(s.T(), err, "should be no errors")
			assert.Len(s.T(), qpr.IDs, test.cnt, "wrong doc count")
			assert.Equal(s.T(), getTotal(test.cnt, withTotal), qpr.Total, "wrong doc count")
		}
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
		_, _, _, err := env.Search(`service:a:`, 1000, setup.NoFetch(), setup.WithTotal(withTotal))
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
	_, _, _, err := env.Search(`service:a`, 1000, setup.NoFetch())

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
			qpr, docs, _, err := env.Search(`service:a`, 1000, setup.WithTotal(withTotal), setup.WithOrder(o))
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

func (s *IntegrationTestSuite) TestDocuments() {
	n := 32
	env, origDocs := s.envWithDummyDocs(n)
	defer env.StopAll()

	env.WaitIdle()

	for _, o := range []seq.DocsOrder{seq.DocsOrderAsc, seq.DocsOrderDesc} {
		qpr, _, _, err := env.Search(`service:a`, n, setup.WithTotal(true), setup.NoFetch(), setup.WithOrder(o))
		s.Assert().NoError(err)
		s.Assert().Equal(getTotal(len(origDocs), true), qpr.Total, "wrong doc count")

		ctx, cancel := context.WithCancel(context.Background())

		docsStream, err := env.Ingestor().SearchIngestor.Documents(ctx, search.FetchRequest{IDs: qpr.IDs.IDs()})
		s.Assert().NoError(err)

		actualDocs := []string{}
		actualIDs := []seq.ID{}
		for doc, err := docsStream.Next(); err == nil; doc, err = docsStream.Next() {
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

func (s *IntegrationTestSuite) TestPathSearch() {
	env := setup.NewTestingEnv(s.Config)
	defer env.StopAll()

	docs := []string{
		`{"service":"a", "request_uri":"/one"}`,
		`{"service":"a", "request_uri":"/one/two"}`,
		`{"service":"a", "request_uri":"/one/two/three"}`,
		`{"service":"a", "request_uri":"/one/two.three/four"}`,
		`{"service":"a", "request_uri":"/one/two.three/five"}`,
		`{"service":"a", "request_uri":"/one/two/three/"}`,
		`{"service":"a", "request_uri":"/one/two/three/1"}`,
		`{"service":"a", "request_uri":"/one/two/three/2"}`,
		`{"service":"a", "request_uri":"/one/two/three/3/four/"}`,
		`{"service":"a", "request_uri":"/one/four/three/3/"}`,
		`{"service":"a", "request_uri":"/two/one/three/2"}`,
	}

	setup.Bulk(s.T(), env.IngestorBulkAddr(), docs)
	env.WaitIdle()

	tests := []struct {
		request string
		cnt     int
	}{
		{request: "/one", cnt: 10},
		{request: "/two", cnt: 1},
		{request: "/one/two", cnt: 6},
		{request: "/one/two/three", cnt: 5},
		{request: "/one/two/three/1", cnt: 1},
		{request: "/one/two.three", cnt: 2},
		{request: "/one/two.three/four", cnt: 1},
		{request: "/one/*/three", cnt: 6},
		{request: "/two/*/three", cnt: 1},
		{request: "*/three/", cnt: 1},
		{request: "*/three", cnt: 7},
	}

	for _, test := range tests {
		req := fmt.Sprintf(`request_uri:%v`, test.request)
		qpr, _, _, err := env.Search(req, 1000, setup.WithTotal(true))
		require.NoError(s.T(), err, "should be no errors")
		assert.Len(s.T(), qpr.IDs, test.cnt, "wrong doc count")
		assert.Equal(s.T(), test.cnt, int(qpr.Total), "wrong doc count")
	}

	env.WaitIdle()
	env.SealAll()

	for _, test := range tests {
		req := fmt.Sprintf(`request_uri:%v`, test.request)
		qpr, _, _, err := env.Search(req, 1000, setup.WithTotal(true))
		require.NoError(s.T(), err, "should be no errors")
		assert.Len(s.T(), qpr.IDs, test.cnt, "wrong doc count")
		assert.Equal(s.T(), test.cnt, int(qpr.Total), "wrong doc count")
	}
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
			qpr, _, _, err := env.Search(tc.request, 100, setup.WithTotal(true))
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

	qpr, _, _, err := env.Search(
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
		`{"level": "info","ts": "2000-01-13T17:36:14.715199225Z","logger": "fd.k8s.action.debug","message": "input event sample","offset": 40059539,"event": {"log": "{\"level\":\"info\",\"ts\":\"2025-01-13T17:36:08.729825704Z\",\"logger\":\"fd.dmesg\",\"message\":\"pipeline stats\",\"stat\":\"interval=5s, active procs=0/2, events in use=0/128, out=0|0.0Mb, rate=0/s|0.0Mb/s, read ops=0/s, total=100857|15.0Mb, avg size=1\"}\n","time": "2025-01-13T17:36:08.729920774Z","stream": "stderr","k8s_container_id": "52f2ab19fe0ba66f4f4e7910780da1e477be98015db58dc624a26c4a585e096b","app_label": "dmesg-reader-z504","pod_app": "dmesg-reader-z504-wjfh5@dmesg-reader-z504","k8s_pod": "dmesg-reader-z504-wjfh5","k8s_namespace": "logging","k8s_container": "dmesg-reader","k8s_node": "infrakuben87742z504","k8s_pod_label_app": "dmesg-reader-z504","k8s_node_label_topology.kubernetes.io/zone": "z504"}}`,
	}

	setup.Bulk(s.T(), env.IngestorBulkAddr(), docs)
	env.WaitIdle()

	r := require.New(s.T())

	now := time.Now()
	resp := setup.SearchHTTP(s.T(), env.IngestorSearchAddr(), &seqproxyapi.SearchRequest{
		Query: &seqproxyapi.SearchQuery{
			Query:   "",
			From:    timestamppb.New(time.Now().Add(-time.Hour * 720)),
			To:      timestamppb.New(time.Now().Add(time.Hour * 720)),
			Explain: false,
		},
		Size:      10,
		Offset:    0,
		WithTotal: false,
	})

	for _, d := range resp.Docs {
		diff := d.Time.AsTime().Sub(now).Abs()
		r.True(diff < 100*time.Millisecond)
	}
}

func (s *IntegrationTestSuite) TestAsyncSearch() {
	t := s.T()
	r := require.New(t)

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
	resp, err := searcher.StartAsyncSearch(ctx, search.AsyncRequest{
		Query:     "* | fields ip, method, uri",
		From:      time.UnixMilli(0),
		To:        time.Now().Add(time.Hour),
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
		HistogramInterval: seq.MID(time.Second.Milliseconds()),
		WithDocs:          true,
	})
	r.NoError(err)
	r.NotEmpty(resp.ID)

	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	freq := search.FetchAsyncSearchResultRequest{
		ID:     resp.ID,
		Size:   100,
		Offset: 0,
	}
	for ctx.Err() == nil {
		fresp, _, err := searcher.FetchAsyncSearchResult(ctx, freq)
		r.NoError(err)
		if fresp.Status == fracmanager.AsyncSearchStatusInProgress {
			time.Sleep(time.Millisecond * 50)
			continue
		}
		break
	}
	r.NoError(ctx.Err())

	fresp, _, err := searcher.FetchAsyncSearchResult(ctx, freq)
	r.NoError(err)

	r.Equalf(fracmanager.AsyncSearchStatusDone, fresp.Status, "unexpected status code=%d with error=%q", fresp.Status, fresp.QPR.Errors)
	r.Equal([]seq.ErrorSource(nil), fresp.QPR.Errors)
	r.True(fresp.ExpiresAt.After(time.Now()))
	r.Equal([]seq.AggregationResult{
		{Buckets: []seq.AggregationBucket{
			{Name: "226.166.207.153", Value: 5116},
			{Name: "117.81.168.0", Value: 4734},
			{Name: "107.2.249.68", Value: 4334},
			{Name: "181.10.24.51", Value: 4002},
			{Name: "13.30.65.187", Value: 3892},
			{Name: "106.51.48.84", Value: 3015},
			{Name: "222.36.179.145", Value: 2683},
			{Name: "70.83.163.58", Value: 2525},
			{Name: "211.170.224.81", Value: 2375},
			{Name: "132.240.243.74", Value: 1598}}},
		{Buckets: []seq.AggregationBucket{
			{Name: "delete", Value: 3015, Quantiles: []float64{3015, 3015, 3015}},
			{Name: "get", Value: 4734, Quantiles: []float64{4734, 4734, 4002}},
			{Name: "post", Value: 3892, Quantiles: []float64{3892, 3892, 3892}},
			{Name: "put", Value: 5116, Quantiles: []float64{5116, 5116, 4334}}}},
	}, fresp.AggResult)

	r.True(len(fresp.QPR.Histogram) != 0)
	r.Equal(len(docs), fresp.QPR.IDs.Len())
	r.Equal(float64(1), fresp.Progress)
}
