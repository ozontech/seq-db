package processor

import (
	"fmt"

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

func (p SearchParams) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if p.AST != nil {
		enc.AddString("request", p.AST.SeqQLString())
	}
	enc.AddString("type", p.Type())
	if p.HistInterval != 0 {
		enc.AddUint64("hist_interval", p.HistInterval)
	}
	enc.AddString("from", p.From.String())
	enc.AddString("to", p.To.String())
	enc.AddUint64("range_seconds", seq.MIDToSeconds(p.To)-seq.MIDToSeconds(p.From))
	if uint64(p.OffsetId.MID) != 0 {
		enc.AddString("offset_id", p.OffsetId.String())
	}
	if p.Limit != 0 {
		enc.AddInt("limit", p.Limit)
	}
	enc.AddBool("with_total", p.WithTotal)
	enc.AddString("order", p.Order.String())
	for i, agg := range p.AggQ {
		err := enc.AddObject(fmt.Sprintf("agg_%d", i), agg)
		if err != nil {
			return err
		}
	}
	return nil
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
