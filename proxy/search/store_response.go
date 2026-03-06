package search

import (
	"fmt"

	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

// FetchRequestFromStoreAPI converts store API FetchRequest to proxy search FetchRequest.
func FetchRequestFromStoreAPI(req *storeapi.FetchRequest) (FetchRequest, error) {
	ids, err := extractIDsFromStoreAPI(req)
	if err != nil {
		return FetchRequest{}, err
	}
	ff := FetchFieldsFilter{}
	if req.FieldsFilter != nil {
		ff.Fields = req.FieldsFilter.Fields
		ff.AllowList = req.FieldsFilter.AllowList
	}
	return FetchRequest{IDs: ids, FieldsFilter: ff}, nil
}

func extractIDsFromStoreAPI(req *storeapi.FetchRequest) ([]seq.ID, error) {
	if len(req.IdsWithHints) != 0 {
		ids := make([]seq.ID, 0, len(req.IdsWithHints))
		for _, id := range req.IdsWithHints {
			parsed, err := seq.FromString(id.Id)
			if err != nil {
				return nil, fmt.Errorf("wrong doc id %s format: %w", id.Id, err)
			}
			ids = append(ids, parsed)
		}
		return ids, nil
	}
	ids := make([]seq.ID, 0, len(req.Ids))
	for _, s := range req.Ids {
		parsed, err := seq.FromString(s)
		if err != nil {
			return nil, fmt.Errorf("wrong doc id %s format: %w", s, err)
		}
		ids = append(ids, parsed)
	}
	return ids, nil
}

// SearchRequestFromStoreAPI converts store API SearchRequest to proxy search SearchRequest.
func SearchRequestFromStoreAPI(req *storeapi.SearchRequest) *SearchRequest {
	aggQ := make([]AggQuery, 0, len(req.Aggs))
	for _, a := range req.Aggs {
		aggQ = append(aggQ, AggQuery{
			Field:     a.Field,
			GroupBy:   a.GroupBy,
			Func:      a.Func.MustAggFunc(),
			Quantiles: a.Quantiles,
			Interval:  seq.MillisToMID(uint64(a.Interval)),
		})
	}
	order := req.Order.MustDocsOrder()
	return &SearchRequest{
		Q:           util.StringToByteUnsafe(req.Query),
		From:        seq.MillisToMID(uint64(req.From)),
		To:          seq.MillisToMID(uint64(req.To)),
		Size:        int(req.Size),
		Offset:      int(req.Offset),
		OffsetId:    req.OffsetId,
		Interval:    seq.MillisToMID(uint64(req.Interval)),
		AggQ:        aggQ,
		Explain:     req.Explain,
		WithTotal:   req.WithTotal,
		ShouldFetch: true,
		Order:       order,
	}
}
