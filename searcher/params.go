package searcher

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

type Params struct {
	AST *parser.ASTNode

	AggQ         []AggQuery
	HistInterval uint64

	From  seq.MID
	To    seq.MID
	Limit int

	WithTotal bool
	Order     seq.DocsOrder
}

func (p *Params) HasHist() bool {
	return p.HistInterval > 0
}

func (p *Params) HasAgg() bool {
	return len(p.AggQ) > 0
}

func (p *Params) IsScanAllRequest() bool {
	return p.WithTotal || p.HasAgg() || p.HasHist()
}
