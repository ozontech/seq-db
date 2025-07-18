package proxyapi

import (
	"context"
	"fmt"
	"strings"

	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/seq"
)

func (g *grpcV1) Fetch(req *seqproxyapi.FetchRequest, stream seqproxyapi.SeqProxyApi_FetchServer) error {
	ctx, cancel := context.WithTimeout(stream.Context(), g.config.SearchTimeout)
	defer cancel()

	span := trace.FromContext(ctx)

	idsStr := strings.Join(req.Ids, ",")
	if span.IsRecordingEvents() {
		span.AddAttributes(trace.StringAttribute("ids", idsStr))
	}

	if !g.rateLimiter.Account(idsStr) {
		return status.Error(codes.ResourceExhausted, consts.ErrRequestWasRateLimited.Error())
	}

	ids := make([]seq.ID, 0, len(req.Ids))
	for _, id := range req.Ids {
		seqID, err := seq.FromString(id)
		if err != nil {
			logger.Warn("wrong format of id",
				zap.String("id", id),
				zap.Error(err),
			)
		} else {
			ids = append(ids, seqID)
		}
	}
	if conf.MaxRequestedDocuments > 0 && len(ids) > conf.MaxRequestedDocuments {
		errMsg := fmt.Sprintf("too many documents are requested: count=%d", len(ids))
		return status.Error(codes.InvalidArgument, errMsg)
	}
	docsStream, err := g.searchIngestor.Documents(ctx, search.FetchRequest{
		IDs: ids,
		FieldsFilter: search.FetchFieldsFilter{
			Fields:    req.GetFieldsFilter().GetFields(),
			AllowList: req.GetFieldsFilter().GetAllowList(),
		},
	})
	if err != nil {
		return status.Errorf(codes.Internal, "can't fetch: %v", err)
	}
	for doc, err := docsStream.Next(); err == nil; doc, err = docsStream.Next() {
		err := stream.Send(&seqproxyapi.Document{
			Id:   doc.ID.String(),
			Data: doc.Data,
			Time: timestamppb.New(doc.ID.MID.Time()),
		})
		if err != nil {
			return status.Errorf(codes.Internal, "failed to send fetched document: %v", err)
		}
	}
	return nil
}
