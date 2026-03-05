package processor

import (
	"go.uber.org/zap/zapcore"

	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

type AggQuery struct {
	Field     *parser.Literal
	GroupBy   *parser.Literal
	Func      seq.AggFunc
	Quantiles []float64
	Interval  int64
}

func (q AggQuery) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if q.Field != nil {
		enc.AddString("field", q.Field.Field)
	}
	if q.GroupBy != nil {
		enc.AddString("groupBy", q.GroupBy.Field)
	}
	enc.AddString("func", q.Func.String())
	if len(q.Quantiles) != 0 {
		enc.AddInt("quantiles_count", len(q.Quantiles))
	}
	if q.Interval != 0 {
		enc.AddInt64("interval", q.Interval)
	}
	return nil
}

type SearchParams struct {
	AST *parser.ASTNode `json:"-"`

	AggQ         []AggQuery
	HistInterval uint64

	From     seq.MID
	To       seq.MID
	OffsetId seq.ID
	Limit    int

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

func (p *SearchParams) Type() string {
	if p.HasAgg() {
		return "agg"
	} else if p.HasHist() {
		return "hist"
	}

	return "reg"
}
