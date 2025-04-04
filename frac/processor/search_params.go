package processor

import (
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

type AggQuery struct {
	Field     *parser.Literal
	GroupBy   *parser.Literal
	Func      seq.AggFunc
	Quantiles []float64
}

type SearchParams struct {
	AST *parser.ASTNode

	AggQ         []AggQuery
	HistInterval uint64

	From  seq.MID
	To    seq.MID
	Limit int

	WithTotal bool
	Order     seq.DocsOrder
}

func (p *SearchParams) HasHist() bool {
	return p.HistInterval > 0
}

func (p *SearchParams) HasAgg() bool {
	return len(p.AggQ) > 0
}

func (p *SearchParams) IsScanAllRequest() bool {
	return p.WithTotal || p.HasAgg() || p.HasHist()
}
