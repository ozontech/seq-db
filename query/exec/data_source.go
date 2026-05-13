package exec

import (
	"context"
	"encoding/binary"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/fracmanager"
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
	if len(qpr.IDs) == 0 {
		return nil
	}
	docs, err := s.frac.Fetch(s.Ctx(), qpr.IDs.IDs())
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

	fracManager *fracmanager.FracManager
	searcher    *fracmanager.Searcher
	fetcher     *fracmanager.Fetcher

	qpr       *seq.QPR
	docs      [][]byte
	curDocIdx int
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
	}
}

func (s *SearcherDataSource) Next() (*query.Record, *query.Metadata) {
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

	if len(qpr.IDs) == 0 {
		return nil
	}

	docs, err := s.fetcher.FetchDocs(s.Ctx(), s.fracManager.Fractions(), qpr.IDs)
	if err != nil {
		return err
	}

	s.qpr = qpr
	s.docs = docs

	return nil
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

func Uint64ToBytes(val uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)
	return b
}
