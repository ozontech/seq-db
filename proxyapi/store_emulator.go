package proxyapi

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/storage"
	store "github.com/ozontech/seq-db/storeapi"
	"github.com/ozontech/seq-db/util"
)

// storeEmulator implements store API (Search, Fetch, Status) so seq-proxy can be
// queried as a store for aggregation-friendly cross-region queries (e.g. quantiles).
type storeEmulator struct {
	storeapi.UnimplementedStoreApiServer
	searchIngestor SearchIngestor
}

func newStoreEmulator(si SearchIngestor) *storeEmulator {
	return &storeEmulator{searchIngestor: si}
}

func (s *storeEmulator) Search(ctx context.Context, req *storeapi.SearchRequest) (*storeapi.SearchResponse, error) {
	sr := search.SearchRequestFromStoreAPI(req)
	qpr, _, _, err := s.searchIngestor.Search(ctx, sr, querytracer.New(req.Explain, "store-emulator/Search"))
	if err != nil {
		if code, ok := parseStoreErrorToCode(err); ok {
			return &storeapi.SearchResponse{Code: code}, nil
		}
		if errors.Is(err, consts.ErrIngestorQueryWantsOldData) {
			return &storeapi.SearchResponse{Code: storeapi.SearchErrorCode_INGESTOR_QUERY_WANTS_OLD_DATA}, nil
		}
		if st, ok := status.FromError(err); ok && st.Code() == codes.InvalidArgument {
			return &storeapi.SearchResponse{Code: storeapi.SearchErrorCode_INGESTOR_QUERY_WANTS_OLD_DATA}, nil
		}
		return nil, err
	}
	return store.BuildSearchResponse(qpr), nil
}

func (s *storeEmulator) Fetch(req *storeapi.FetchRequest, stream storeapi.StoreApi_FetchServer) error {
	ctx := stream.Context()
	fetchReq, err := search.FetchRequestFromStoreAPI(req)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "fetch request: %v", err)
	}
	docsStream, err := s.searchIngestor.Documents(ctx, fetchReq)
	if err != nil {
		return fmt.Errorf("documents: %w", err)
	}
	var buf []byte
	for {
		doc, err := docsStream.Next()
		if err != nil {
			break
		}
		buf = util.EnsureSliceSize(buf, storage.DocBlockHeaderLen+len(doc.Data))
		block := storage.PackDocBlock(doc.Data, buf)
		block.SetExt1(uint64(doc.ID.MID))
		block.SetExt2(uint64(doc.ID.RID))
		if err := stream.Send(&storeapi.BinaryData{Data: block}); err != nil {
			return err
		}
	}
	return nil
}

// TODO(moflotas) in case of regions work improperly
func (s *storeEmulator) Status(_ context.Context, _ *storeapi.StatusRequest) (*storeapi.StatusResponse, error) {
	st := s.searchIngestor.Status(context.Background())
	if st == nil || st.OldestStorageTime == nil {
		return &storeapi.StatusResponse{}, nil
	}
	return &storeapi.StatusResponse{
		OldestTime: timestamppb.New(*st.OldestStorageTime),
	}, nil
}

func parseStoreErrorToCode(e error) (storeapi.SearchErrorCode, bool) {
	if errors.Is(e, consts.ErrTooManyFieldTokens) {
		return storeapi.SearchErrorCode_TOO_MANY_FIELD_TOKENS, true
	}
	if errors.Is(e, consts.ErrTooManyFieldValues) {
		return storeapi.SearchErrorCode_TOO_MANY_FIELD_VALUES, true
	}
	if errors.Is(e, consts.ErrTooManyGroupTokens) {
		return storeapi.SearchErrorCode_TOO_MANY_GROUP_TOKENS, true
	}
	if errors.Is(e, consts.ErrTooManyFractionTokens) {
		return storeapi.SearchErrorCode_TOO_MANY_FRACTION_TOKENS, true
	}
	if errors.Is(e, consts.ErrTooManyFractionsHit) {
		return storeapi.SearchErrorCode_TOO_MANY_FRACTIONS_HIT, true
	}
	return 0, false
}
