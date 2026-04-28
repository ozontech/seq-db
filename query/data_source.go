package query

import "github.com/ozontech/seq-db/frac"

type DataSource interface {
	Scan() (*Record, bool)
}

type FractionDataSource struct {
	frac frac.Fraction
	// TODO:
}

func NewFractionDatasource() *FractionDataSource {
	// TODO:
	return &FractionDataSource{}
}

func (s *FractionDataSource) Next() (*Record, bool) {
	// TODO:
	return nil, false
}
