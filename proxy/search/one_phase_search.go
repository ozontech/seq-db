package search

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/alecthomas/units"
	"google.golang.org/grpc"

	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
)

func (si *Ingestor) OnePhaseSearch(
	ctx context.Context,
	sr *SearchRequest,
	tr *querytracer.Tracer,
) (*seq.QPR, DocsIterator, error) {
	searchStores := si.config.HotStores
	if si.config.HotReadStores != nil && len(si.config.HotReadStores.Shards) > 0 {
		searchStores = si.config.HotReadStores
	}

	host := searchStores.Shards[0][0] // TODO: handle multiple stores and shards

	client, has := si.clients[host]
	if !has {
		return nil, nil, fmt.Errorf("can't fetch: no client for host %s", host)
	}

	fieldsFilter := tryParseFieldsFilter(string(sr.Q))
	req := &storeapi.OnePhaseSearchRequest{
		Query:     string(sr.Q),
		From:      int64(sr.From),
		To:        int64(sr.To),
		Size:      int64(sr.Size),
		Offset:    int64(sr.Offset),
		Explain:   sr.Explain,
		WithTotal: sr.WithTotal,
		Order:     storeapi.Order(sr.Order),
		OffsetId:  sr.OffsetId,
		FieldsFilter: &storeapi.FieldsFilter{
			Fields:    fieldsFilter.Fields,
			AllowList: fieldsFilter.AllowList,
		},
	}

	stream, err := client.OnePhaseSearch(ctx, req,
		grpc.MaxCallRecvMsgSize(256*int(units.MiB)),
		grpc.MaxCallSendMsgSize(256*int(units.MiB)),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("can't fetch docs: %s", err.Error())
	}

	msg, err := stream.Recv()
	if err != nil {
		return nil, nil, nil
	}

	header := msg.GetHeader()

	errs := make([]seq.ErrorSource, 0, len(header.Errors))
	for _, err := range header.Errors {
		errs = append(errs, seq.ErrorSource{ErrStr: err})
	}

	qpr := &seq.QPR{
		Total:  header.Total,
		Errors: errs,
	}

	return qpr, &OnePhaseSearchIterator{stream: stream, limit: sr.Size}, nil
}

type OnePhaseSearchIterator struct {
	stream storeapi.StoreApi_OnePhaseSearchClient

	fetched int
	limit   int
}

func (it *OnePhaseSearchIterator) Next() (StreamingDoc, error) {
	if it.fetched >= it.limit {
		return StreamingDoc{}, io.EOF
	}

	data, err := it.stream.Recv()
	if errors.Is(err, io.EOF) {
		return StreamingDoc{}, io.EOF
	}
	if err != nil {
		return StreamingDoc{}, err
	}

	doc := data.GetDocument()
	block := storage.DocBlock(doc.Data.Data)
	mid := block.GetExt1()

	it.fetched++

	return StreamingDoc{
		ID: seq.ID{
			MID: seq.MID(mid),
			RID: seq.RID(block.GetExt2()),
		},
		Data: block.Payload(),
	}, nil
}
