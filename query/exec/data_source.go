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
	docs, err := s.frac.Fetch(s.Ctx(), qpr.IDs.IDs())
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
