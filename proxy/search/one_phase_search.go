package search

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/alecthomas/units"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
)

func (si *Ingestor) OnePhaseSearch(
	ctx context.Context,
	sr *SearchRequest,
	tr *querytracer.Tracer,
) (*seq.QPR, DocsIterator, AggsIterator, error) {
	// TODO: handle consts.ErrIngestorQueryWantsOldData
	searchStores := si.config.HotStores
	if si.config.HotReadStores != nil && len(si.config.HotReadStores.Shards) > 0 {
		searchStores = si.config.HotReadStores
	}

	host := searchStores.Shards[0][0] // TODO: handle multiple stores and shards !!!

	client, has := si.clients[host]
	if !has {
		return nil, nil, nil, fmt.Errorf("can't fetch: no client for host %s", host)
	}

	fieldsFilter := tryParseFieldsFilter(string(sr.Q))
	req := &storeapi.OnePhaseSearchRequest{
		Query:     string(sr.Q),
		From:      timestamppb.New(sr.From.Time()),
		To:        timestamppb.New(sr.To.Time()),
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
		return nil, nil, nil, fmt.Errorf("can't fetch docs: %s", err.Error())
	}

	msg, err := stream.Recv()
	if err != nil {
		return nil, nil, nil, nil
	}

	header := msg.GetHeader()

	errs := make([]seq.ErrorSource, 0, len(header.Metadata.Errors))
	for _, err := range header.Metadata.Errors {
		errs = append(errs, seq.ErrorSource{ErrStr: err})
	}

	qpr := &seq.QPR{
		Total:  header.Metadata.Total,
		Errors: errs,
	}

	return qpr, &OnePhaseSearchDocsIterator{stream: stream, limit: sr.Size}, &OnePhaseSearchAggsIterator{stream: stream, limit: sr.Size}, nil
}

func (si *Ingestor) searchShardOnePhase() {
	// TODO:
}

func (si *Ingestor) searchHostOnePhase() {
	// TODO:
}

type OnePhaseSearchDocsIterator struct {
	stream storeapi.StoreApi_OnePhaseSearchClient

	curBatch []*storeapi.Record

	fetched int
	limit   int
}

func (it *OnePhaseSearchDocsIterator) Next() (StreamingDoc, error) {
	if it.fetched >= it.limit {
		return StreamingDoc{}, io.EOF
	}

	if len(it.curBatch) == 0 {
		data, err := it.stream.Recv()
		if errors.Is(err, io.EOF) {
			return StreamingDoc{}, io.EOF
		}
		if err != nil {
			return StreamingDoc{}, err
		}
		it.curBatch = data.GetBatch().Records
	}

	// TODO: get fields values from columns info

	record := it.curBatch[0]
	it.curBatch = it.curBatch[1:]

	it.fetched++

	return StreamingDoc{
		ID: seq.ID{
			MID: seq.MID(binary.LittleEndian.Uint64(record.RawData[0])),
			RID: seq.RID(binary.LittleEndian.Uint64(record.RawData[1])),
		},
		Data: record.RawData[2],
	}, nil
}

type StreamingAgg struct {
	Label     string
	Min       float64
	Max       float64
	Sum       float64
	Total     uint64
	NotExists uint64
}

type AggsIterator interface {
	Next() (StreamingAgg, error)
}

type OnePhaseSearchAggsIterator struct {
	stream storeapi.StoreApi_OnePhaseSearchClient

	curBatch []*storeapi.Record

	fetched int
	limit   int
}

func (it *OnePhaseSearchAggsIterator) Next() (StreamingAgg, error) {
	if it.fetched >= it.limit {
		return StreamingAgg{}, io.EOF
	}

	if len(it.curBatch) == 0 {
		data, err := it.stream.Recv()
		if errors.Is(err, io.EOF) {
			return StreamingAgg{}, io.EOF
		}
		if err != nil {
			return StreamingAgg{}, err
		}
		it.curBatch = data.GetBatch().Records
	}

	// TODO: get fields values from columns info

	record := it.curBatch[0]
	it.curBatch = it.curBatch[1:]

	it.fetched++

	return StreamingAgg{
		Label:     string(record.RawData[0]),
		Min:       Float64FromBytes(record.RawData[1]),
		Max:       Float64FromBytes(record.RawData[2]),
		Sum:       Float64FromBytes(record.RawData[3]),
		Total:     binary.LittleEndian.Uint64(record.RawData[4]),
		NotExists: binary.LittleEndian.Uint64(record.RawData[5]),
	}, nil
}

func Float64FromBytes(in []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(in))
}
