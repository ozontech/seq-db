package exec

import (
	"context"
	"encoding/binary"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/seq"
)

type FractionDataSource struct {
	ctx context.Context

	frac         frac.Fraction
	searchParams processor.SearchParams

	qpr      *seq.QPR
	curIdIdx int
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

func (s *FractionDataSource) Next() (*query.Record, bool) {
	if s.qpr == nil {
		s.scan()
	}

	if s.curIdIdx >= len(s.qpr.IDs) {
		return nil, false
	}

	curId := s.qpr.IDs[s.curIdIdx]
	fetched, err := s.frac.Fetch(s.Ctx(), []seq.ID{curId.ID}) // TODO: fetch all ids in single request
	if err != nil {
		panic(err) // TODO: error handling
	}

	s.curIdIdx++
	return makeDocumentRecord(curId.ID, fetched[0]), true
}

func (s *FractionDataSource) Ctx() context.Context {
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

func (s *FractionDataSource) scan() {
	qpr, err := s.frac.Search(s.Ctx(), s.searchParams)
	if err != nil {
		panic(err) // TODO: error handling
	}
	s.qpr = qpr
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
