package proxyapi

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/proxyapi/mock"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tracing"
	"github.com/ozontech/seq-db/util"
)

type testSearchQuery struct {
	query   string
	from    time.Time
	to      time.Time
	explain bool
}

type testHistQuery struct {
	interval string
}

type testAggQuery struct {
	aggField   string
	bucketsCnt int
}

type siSearchRet struct {
	qpr  *seq.QPR
	docs search.DocsIterator
	took time.Duration
	err  error
}

type siSearchMockData struct {
	sr  *search.SearchRequest
	ret siSearchRet
}

type siDocumentsRet struct {
	docs search.DocsIterator
	err  error
}

type siDocumentsMockData struct {
	ids []seq.ID
	ret siDocumentsRet
}

type siMockData struct {
	search    *siSearchMockData
	documents *siDocumentsMockData
}

type rlMockData struct {
	query   string
	limited bool
}

type acMockData struct {
	mapping []byte
}

type mocksData struct {
	si *siMockData
	rl *rlMockData
	ac *acMockData
}

type mocks struct {
	siMock *mock.MockSearchIngestor
	rlMock *mock.MockRateLimiter
	acMock *mock.MockMapping
}

type testGrpcV1Data struct {
	ctx context.Context
	s   *grpcV1
	m   *mocks
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func runGRPCServerWithClient(apiServer seqproxyapi.SeqProxyApiServer) (seqproxyapi.SeqProxyApiClient, func()) {
	ctx := context.Background()
	lis := bufconn.Listen(10 * 1024 * 1024)
	server := grpc.NewServer(
		grpc.StatsHandler(&tracing.ServerHandler{}),
	)
	seqproxyapi.RegisterSeqProxyApiServer(server, apiServer)
	go func() {
		if err := server.Serve(lis); err != nil {
			logger.Fatal("failed to run grpc server", zap.Error(err))
		}
	}()
	closer := func() {
		err := lis.Close()
		if err != nil {
			logger.Fatal("failed to close listener", zap.Error(err))
		}
		server.Stop()
	}
	conn, err := grpc.DialContext(
		ctx,
		"",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(&tracing.ClientHandler{}),
	)
	if err != nil {
		closer()
		logger.Fatal("failed to init dial context", zap.Error(err))
	}
	client := seqproxyapi.NewSeqProxyApiClient(conn)
	return client, closer
}

func initTestGrpcV1(ctrl *gomock.Controller) testGrpcV1Data {
	ctx := context.Background()
	ctx, _ = trace.StartSpan(
		ctx, "test", trace.WithSampler(trace.AlwaysSample()),
	)
	m := &mocks{}
	m.siMock = mock.NewMockSearchIngestor(ctrl)
	m.acMock = mock.NewMockMapping(ctrl)
	m.rlMock = mock.NewMockRateLimiter(ctrl)

	cfg := APIConfig{
		SearchTimeout: consts.DefaultSearchTimeout,
		ExportTimeout: consts.DefaultExportTimeout,
	}
	s := newGrpcV1(cfg, m.siMock, m.acMock, m.rlMock, nil)

	return testGrpcV1Data{ctx: ctx, s: s, m: m}
}

func prepareMock(m *mocks, mData *mocksData) {
	if m.siMock != nil && mData.si != nil {
		if mData.si.search != nil {
			ret := mData.si.search.ret
			m.siMock.EXPECT().Search(
				gomock.Any(), mData.si.search.sr,
			).Return(ret.qpr, ret.docs, ret.took, ret.err)
		}
		if mData.si.documents != nil {
			ret := mData.si.documents.ret
			m.siMock.EXPECT().Documents(
				gomock.Any(), mData.si.documents.ids,
			).Return(ret.docs, ret.err)
		}
	}
	if m.rlMock != nil && mData.rl != nil {
		m.rlMock.EXPECT().Account(
			mData.rl.query,
		).Return(!mData.rl.limited)
	}
	if m.acMock != nil && mData.ac != nil {
		m.acMock.EXPECT().GetRawMappingBytes().Return(mData.ac.mapping)
	}
}

func prepareTestGrpcV1(ctrl *gomock.Controller, mData *mocksData) testGrpcV1Data {
	a := initTestGrpcV1(ctrl)
	prepareMock(a.m, mData)
	return a
}

type testSearchResp struct {
	ids      seq.IDSources
	docs     [][]byte
	respDocs []*seqproxyapi.Document
}

func makeSearchRespData(size int) *testSearchResp {
	ids := make(seq.IDSources, 0)
	docs := make([][]byte, 0)
	respDocs := make([]*seqproxyapi.Document, 0)
	for i := 0; i < size; i++ {
		id := seq.SimpleID(i)
		ids = append(ids, seq.IDSource{ID: id, Source: 0})
		data := []byte("doc" + strconv.Itoa(i))
		docs = append(docs, data)
		respDocs = append(respDocs,
			&seqproxyapi.Document{Id: id.String(), Data: data},
		)
	}
	return &testSearchResp{
		ids:      ids,
		docs:     docs,
		respDocs: respDocs,
	}
}

type testGetHistResp struct {
	qprHist map[seq.MID]uint64
	apiHist *seqproxyapi.Histogram
}

func makeGetHistRespData(interval string, totalSize, fromTs, toTs int64) (*testGetHistResp, error) {
	intervalDur, err := util.ParseDuration(interval)
	if err != nil {
		return nil, fmt.Errorf("failed to parse histogram interval duration: %q", interval)
	}
	buckets := make([]*seqproxyapi.Histogram_Bucket, 0)
	qprHist := make(map[seq.MID]uint64)
	intervalMS := intervalDur.Milliseconds()
	bucketsCnt := int((toTs - fromTs) / intervalMS)
	docCnt := totalSize / int64(bucketsCnt)
	remainCnt := totalSize - docCnt*(int64(bucketsCnt)-1)
	bucketKey := fromTs
	qprHist[seq.MID(bucketKey)] = uint64(remainCnt)
	ts := time.UnixMilli(bucketKey)
	bucket := &seqproxyapi.Histogram_Bucket{
		DocCount: uint64(remainCnt),
		Ts:       timestamppb.New(ts),
	}
	buckets = append(buckets, bucket)
	for i := 1; i < bucketsCnt; i++ {
		bucketKey := fromTs + int64(i)*intervalMS
		ts := time.UnixMilli(bucketKey)
		qprHist[seq.MID(bucketKey)] = uint64(docCnt)
		bucket := &seqproxyapi.Histogram_Bucket{
			DocCount: uint64(docCnt),
			Ts:       timestamppb.New(ts),
		}
		buckets = append(buckets, bucket)
	}
	return &testGetHistResp{
		qprHist: qprHist,
		apiHist: &seqproxyapi.Histogram{
			Buckets: buckets,
		},
	}, nil
}

type testGetAggResp struct {
	qprAgg map[string]*seq.AggregationHistogram
	apiAgg *seqproxyapi.Aggregation
}

func makeGetAggRespData(totalSize int64, bucketsCnt int) *testGetAggResp {
	buckets := make([]*seqproxyapi.Aggregation_Bucket, 0)
	qprAgg := make(map[string]*seq.AggregationHistogram)
	docCnt := totalSize / int64(bucketsCnt)
	remainCnt := totalSize - docCnt*(int64(bucketsCnt)-1)
	bucketKey := "bucket0"
	qprAgg[bucketKey] = seq.NewAggregationHistogram()
	qprAgg[bucketKey].Total = remainCnt
	bucket := &seqproxyapi.Aggregation_Bucket{
		Value: float64(remainCnt), Key: bucketKey,
	}
	buckets = append(buckets, bucket)
	for i := 1; i < bucketsCnt; i++ {
		bucketKey = "bucket" + strconv.Itoa(i)
		qprAgg[bucketKey] = &seq.AggregationHistogram{Total: docCnt}
		bucket = &seqproxyapi.Aggregation_Bucket{
			Value: float64(docCnt), Key: bucketKey,
		}
		buckets = append(buckets, bucket)
	}
	return &testGetAggResp{
		qprAgg: qprAgg,
		apiAgg: &seqproxyapi.Aggregation{
			Buckets: buckets,
		},
	}
}

type testExportResp struct {
	ids  seq.IDSources
	docs [][]byte
	resp []*seqproxyapi.ExportResponse
}

func makeExportRespData(size int) *testExportResp {
	ids := make(seq.IDSources, size)
	docs := make([][]byte, size)
	resp := make([]*seqproxyapi.ExportResponse, size)
	for i := 0; i < size; i++ {
		id := seq.SimpleID(i)
		ids[i] = seq.IDSource{ID: id, Source: 0}

		data := []byte("doc" + strconv.Itoa(i))
		docs[i] = data

		resp[i] = &seqproxyapi.ExportResponse{
			Doc: &seqproxyapi.Document{
				Id:   id.String(),
				Data: data,
			},
		}
	}

	return &testExportResp{
		ids:  ids,
		docs: docs,
		resp: resp,
	}
}

type sliceDocsIterator struct {
	ids  []seq.IDSource
	docs [][]byte
}

func newSliceDocsStream(ids []seq.IDSource, docs [][]byte) *sliceDocsIterator {
	return &sliceDocsIterator{
		ids:  ids,
		docs: docs,
	}
}

func (d *sliceDocsIterator) Next() (search.StreamingDoc, error) {
	if len(d.ids) == 0 {
		return search.StreamingDoc{}, io.EOF
	}

	var doc []byte
	var id seq.IDSource

	id, d.ids = d.ids[0], d.ids[1:]
	if len(d.docs) > 0 {
		doc, d.docs = d.docs[0], d.docs[1:]
	}
	return search.NewStreamingDoc(id, doc), nil
}
