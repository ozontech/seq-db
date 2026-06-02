package exec

import (
	"context"
	"encoding/binary"
	"errors"
	"math"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
)

type FractionDataSource struct {
	ctx context.Context

	frac         frac.Fraction
	searchParams processor.SearchParams

	qpr       *seq.QPR
	docs      [][]byte
	curDocIdx int
}

func NewFractionDatasource(
	ctx context.Context,
	fraction frac.Fraction,
	searchParams processor.SearchParams,
) *FractionDataSource {
	return &FractionDataSource{
		ctx:          ctx,
		frac:         fraction,
		searchParams: searchParams,
	}
}

func (s *FractionDataSource) Next() (*query.Record, *query.Metadata) {
	if len(s.docs) == 0 {
		if err := s.scan(); err != nil {
			return nil, &query.Metadata{Err: err}
		}
	}

	if s.curDocIdx >= len(s.docs) {
		return nil, nil
	}

	docRecord := makeDocumentRecord(s.qpr.IDs[s.curDocIdx].ID, s.docs[s.curDocIdx])
	s.curDocIdx++

	return docRecord, nil
}

func (s *FractionDataSource) Ctx() context.Context {
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

func (s *FractionDataSource) scan() error {
	qpr, err := s.frac.Search(s.Ctx(), s.searchParams)
	if err != nil {
		return err
	}

	if len(qpr.Errors) > 0 {
		var resErr error
		for _, e := range qpr.Errors {
			resErr = errors.Join(errors.New(e.ErrStr)) // TODO: ???
		}
		return resErr
	}

	if len(qpr.IDs) == 0 {
		return nil
	}

	docs, err := s.frac.Fetch(s.Ctx(), qpr.IDs.IDs(), false)
	if err != nil {
		return err
	}

	s.qpr = qpr
	s.docs = docs

	return nil
}

type SearcherDataSource struct {
	ctx context.Context
	tr  *querytracer.Tracer

	searchParams processor.SearchParams // TODO: ???
	isAgg        bool

	fracManager *fracmanager.FracManager
	searcher    *fracmanager.Searcher
	fetcher     *fracmanager.Fetcher

	qpr  *seq.QPR
	docs [][]byte
	aggs []*storeapi.SearchResponse_Agg // TODO: internal struct

	curIdx int
}

func NewSearcherDataSource(
	ctx context.Context,
	tr *querytracer.Tracer,
	searchParams processor.SearchParams,
	fracManager *fracmanager.FracManager,
	searcher *fracmanager.Searcher,
	fetcher *fracmanager.Fetcher,
) *SearcherDataSource {
	return &SearcherDataSource{
		ctx:          ctx,
		tr:           tr,
		searchParams: searchParams,
		fracManager:  fracManager,
		searcher:     searcher,
		fetcher:      fetcher,
		isAgg:        len(searchParams.AggQ) > 0,
	}
}

func (s *SearcherDataSource) Next() (*query.Record, *query.Metadata) {
	// TODO: get rid of hardcode (???)
	if s.isAgg {
		return s.nextAgg()
	} else {
		return s.nextDoc()
	}
}

func (s *SearcherDataSource) nextDoc() (*query.Record, *query.Metadata) {
	if len(s.docs) == 0 {
		if err := s.scan(); err != nil {
			return nil, &query.Metadata{Err: err}
		}
	}

	if s.curIdx >= len(s.docs) {
		return nil, nil
	}

	record := makeDocumentRecord(s.qpr.IDs[s.curIdx].ID, s.docs[s.curIdx])

	s.curIdx++

	return record, nil
}

func (s *SearcherDataSource) nextAgg() (*query.Record, *query.Metadata) {
	if len(s.aggs) == 0 {
		if err := s.scan(); err != nil {
			return nil, &query.Metadata{Err: err}
		}
	}

	if s.aggs[0] == nil {
		return nil, nil
	}

	if s.curIdx >= len(s.aggs[0].Timeseries) {
		return nil, nil
	}

	record := makeAggRecord(s.aggs[0].Timeseries[s.curIdx])

	s.curIdx++

	return record, nil
}

func (s *SearcherDataSource) Ctx() context.Context {
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

func (s *SearcherDataSource) scan() error {
	qpr, err := s.searcher.SearchDocs(s.Ctx(), s.fracManager.Fractions(), s.searchParams, s.tr)
	if err != nil {
		return err
	}

	if len(qpr.Errors) > 0 {
		var resErr error
		for _, e := range qpr.Errors {
			resErr = errors.Join(errors.New(e.ErrStr)) // TODO: ???
		}
		return resErr
	}

	s.qpr = qpr
	s.aggs = buildAggs(qpr)

	if len(qpr.IDs) == 0 {
		return nil
	}

	docs, err := s.fetcher.FetchDocs(s.Ctx(), s.fracManager.Fractions(), qpr.IDs, false)
	if err != nil {
		return err
	}
	s.docs = docs

	return nil
}

func buildAggs(qpr *seq.QPR) []*storeapi.SearchResponse_Agg {
	aggsBuf := make([]storeapi.SearchResponse_Agg, len(qpr.Aggs))
	aggs := make([]*storeapi.SearchResponse_Agg, len(qpr.Aggs))

	for i, fromAgg := range qpr.Aggs {
		curAgg := &aggsBuf[i]

		from := fromAgg.SamplesByBin
		to := make(map[string]*storeapi.SearchResponse_Histogram, len(from))

		for bin, hist := range from {
			pbhist := &storeapi.SearchResponse_Histogram{
				Min:       hist.Min,
				Max:       hist.Max,
				Sum:       hist.Sum,
				Total:     hist.Total,
				Samples:   hist.Samples,
				NotExists: hist.NotExists,
			}

			curAgg.Timeseries = append(curAgg.Timeseries,
				&storeapi.SearchResponse_Bin{
					Label: bin.Token,
					Ts:    timestamppb.New(bin.MID.Time()),
					Hist:  pbhist,
				},
			)

			to[bin.Token] = pbhist
		}

		curAgg.NotExists = fromAgg.NotExists
		curAgg.AggHistogram = to

		aggs[i] = curAgg
	}

	return aggs
}

func makeDocumentRecord(id seq.ID, payload []byte) *query.Record {
	return &query.Record{
		Vals: []*query.RecordVals{
			query.NewRecordVals(query.DataTypeUint64, Uint64ToBytes(uint64(id.MID))),
			query.NewRecordVals(query.DataTypeUint64, Uint64ToBytes(uint64(id.RID))),
			query.NewRecordVals(query.DataTypeDocument, payload),
		},
	}
}

func makeAggRecord(bin *storeapi.SearchResponse_Bin) *query.Record {
	return &query.Record{
		Vals: []*query.RecordVals{
			query.NewRecordVals(query.DataTypeBytes, []byte(bin.Label)),
			query.NewRecordVals(query.DataTypeFloat64, Float64ToBytes(bin.Hist.Min)),
			query.NewRecordVals(query.DataTypeFloat64, Float64ToBytes(bin.Hist.Max)),
			query.NewRecordVals(query.DataTypeFloat64, Float64ToBytes(bin.Hist.Sum)),
			query.NewRecordVals(query.DataTypeUint64, Uint64ToBytes(uint64(bin.Hist.Total))),
			query.NewRecordVals(query.DataTypeUint64, Uint64ToBytes(uint64(bin.Hist.NotExists))),
		},
	}
}

func Uint64ToBytes(val uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)
	return b
}

func Float64ToBytes(val float64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(val))
	return b
}
