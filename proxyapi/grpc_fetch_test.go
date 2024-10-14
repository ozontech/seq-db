package proxyapi

import (
	"context"
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/trace"

	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxyapi/mock"
	"github.com/ozontech/seq-db/seq"
)

type fetchTestCaseData struct {
	startID       int
	size          int
	noResp        bool
	addID         string
	siErr         error
	noSiMock      bool
	rateLimited   bool
	streamSendErr bool
}

type fetchTestData struct {
	req   *seqproxyapi.FetchRequest
	want  []*seqproxyapi.Document
	mData *mocksData
}

func prepareFetchTestData(cData fetchTestCaseData) fetchTestData {
	ids := seq.IDSources(make([]seq.IDSource, 0))
	idsStr := make([]string, 0)
	docs := [][]byte{}
	apiDocs := make([]*seqproxyapi.Document, 0)
	for i := 0; i < cData.size; i++ {
		id := seq.SimpleID(cData.startID + i)
		ids = append(ids, seq.IDSource{ID: id})
		idsStr = append(idsStr, id.String())
		if !cData.noResp {
			doc := []byte("doc" + strconv.Itoa(cData.startID+i))
			docs = append(docs, doc)
			apiDocs = append(apiDocs,
				&seqproxyapi.Document{Id: id.String(), Data: doc},
			)
		}
	}
	if cData.addID != "" {
		idsStr = append(idsStr, cData.addID)
	}
	sort.Strings(idsStr)
	req := &seqproxyapi.FetchRequest{Ids: idsStr}
	var resp []*seqproxyapi.Document
	if !cData.noResp {
		resp = apiDocs
	}
	var siDocumentsMock *siDocumentsMockData
	if !cData.noSiMock {
		siDocumentsMock = &siDocumentsMockData{
			ids: ids.IDs(),
			ret: siDocumentsRet{
				docs: newSliceDocsStream(ids, docs),
				err:  cData.siErr,
			},
		}
	}
	idsStrJoined := strings.Join(idsStr, ",")
	rlMock := &rlMockData{
		query:   idsStrJoined,
		limited: cData.rateLimited,
	}
	return fetchTestData{
		req:  req,
		want: resp,
		mData: &mocksData{
			si: &siMockData{
				documents: siDocumentsMock,
			},
			rl: rlMock,
		},
	}
}

func TestGrpcV1_Fetch(t *testing.T) {
	tests := []struct {
		name    string
		data    fetchTestCaseData
		wantErr bool
	}{
		{
			name: "ok",
			data: fetchTestCaseData{
				startID: 1,
				size:    10,
			},
			wantErr: false,
		},
		{
			name: "ok_with_err_id",
			data: fetchTestCaseData{
				startID: -1,
				size:    1,
				addID:   "g-error",
			},
			wantErr: false,
		},
		{
			name: "rate_limited",
			data: fetchTestCaseData{
				startID:     -1,
				size:        1,
				noSiMock:    true,
				noResp:      true,
				rateLimited: true,
			},
			wantErr: true,
		},
		{
			name: "too_many_ids",
			data: fetchTestCaseData{
				startID:  1,
				size:     conf.MaxRequestedDocuments + 1,
				noSiMock: true,
				noResp:   true,
			},
			wantErr: true,
		},
		{
			name: "search_ingestor_err",
			data: fetchTestCaseData{
				startID: -2,
				size:    1,
				siErr:   errors.New("test"),
				noResp:  true,
			},
			wantErr: true,
		},
		{
			name: "stream_send_err",
			data: fetchTestCaseData{
				startID:       -2,
				size:          1,
				noResp:        true,
				streamSendErr: true,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			testData := prepareFetchTestData(tt.data)
			a := prepareTestGrpcV1(ctrl, testData.mData)

			ctx := context.Background()
			ctx, span := trace.StartSpan(
				ctx, "fetch-test", trace.WithSampler(trace.AlwaysSample()),
			)
			defer span.End()
			streamMock := mock.NewMockFetchServer(ctrl)
			streamMock.EXPECT().Send(gomock.Any()).DoAndReturn(
				func(_ *seqproxyapi.Document) error {
					if tt.data.streamSendErr {
						return errors.New("test-send-error")
					}
					return nil
				},
			).AnyTimes()
			streamMock.EXPECT().Context().Return(ctx).AnyTimes()

			err := a.s.Fetch(testData.req, streamMock)
			r.Equal(tt.wantErr, err != nil)
		})
	}
}

func TestGrpcV1_FetchLive(t *testing.T) {
	tests := []struct {
		name    string
		data    fetchTestCaseData
		wantErr bool
	}{
		{
			name: "ok",
			data: fetchTestCaseData{
				startID: 1,
				size:    10,
			},
		},
		{
			name: "ok_with_err_id",
			data: fetchTestCaseData{
				startID: -1,
				size:    1,
				addID:   "g-error",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			testData := prepareFetchTestData(tt.data)
			a := prepareTestGrpcV1(ctrl, testData.mData)

			client, closer := runGRPCServerWithClient(a.s)
			defer closer()

			var recvErr error
			out, err := client.Fetch(a.ctx, testData.req)
			r.NoError(err)
			got := make([]*seqproxyapi.Document, 0)
			for {
				o, err := out.Recv()
				if errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					recvErr = err
					break
				}
				got = append(got, o)
			}
			r.Equal(tt.wantErr, recvErr != nil)
			if tt.wantErr {
				return
			}
			r.Equal(len(testData.want), len(got))
			for i := 0; i < len(got); i++ {
				r.Equal(testData.want[i].Id, got[i].Id)
				r.Equal(string(testData.want[i].Data), string(got[i].Data))
			}
		})
	}
}
