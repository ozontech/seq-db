package plan

import (
	"fmt"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type Plan struct {
	Scan   Scan
	Ops    []Op
	Schema *query.Schema
}

func (p *Plan) IsAgg() bool {
	return len(p.Scan.AggQ) > 0
}

// Scan describes the datasource.
type Scan struct {
	AST *parser.ASTNode

	From, To  seq.MID
	Order     seq.DocsOrder
	WithTotal bool

	AggQ []processor.AggQuery

	OffsetID seq.ID
}

type BuildParams struct {
	SeqQL *parser.SeqQLQuery

	// Input is the record schema the pipeline starts from
	Input *query.Schema
	// DocField is the title of document field in Input.
	// We still keep some hardcoded params. Will ger rid of it later.
	DocField string

	From, To  seq.MID
	OffsetID  string
	WithTotal bool
}

// Build converts a parsed SeqQL query into a validated logical plan.
func Build(params BuildParams) (*Plan, error) {
	if params.Input == nil {
		return nil, fmt.Errorf("input schema is not set")
	}
	if params.DocField == "" {
		return nil, fmt.Errorf("doc column is not set")
	}
	if _, err := params.Input.Index(params.DocField); err != nil {
		return nil, fmt.Errorf("doc column: %w", err)
	}

	p := &Plan{
		Scan: Scan{
			AST:       params.SeqQL.Root,
			From:      params.From,
			To:        params.To,
			WithTotal: params.WithTotal,
		},
		Schema: params.Input,
	}

	var limit, offset int
	for _, pipe := range params.SeqQL.Pipes {
		switch pipe := pipe.(type) {
		case *parser.PipeStats:
			aggQ, err := convertStatsAggToAggQuery(pipe.Agg)
			if err != nil {
				return nil, fmt.Errorf("failed to convert stats aggs: %w", err)
			}
			p.Scan.AggQ = []processor.AggQuery{aggQ}
			p.Schema = query.AggsSchema
		case *parser.PipeFilter:
			// we need to extract the field so filter can compare based on it.
			if _, err := params.Input.Index(pipe.Condition.Field); err == nil {
				return nil, fmt.Errorf("cannot filter on reserved column %q", pipe.Condition.Field)
			}
			p.Ops = append(p.Ops,
				&ExtractOp{Fields: []string{pipe.Condition.Field}, OpField: params.DocField},
				&FilterOp{Cond: pipe.Condition, WithTotal: params.WithTotal},
			)
		case *parser.PipeFields:
			p.Ops = append(p.Ops, &ProjectOp{Fields: pipe.Fields, AllowList: !pipe.Except, OpField: params.DocField})
		case *parser.PipeSort:
			if pipe.Order == "desc" {
				p.Scan.Order = seq.DocsOrderDesc
			} else {
				p.Scan.Order = seq.DocsOrderAsc
			}
		case *parser.PipeLimit:
			limit = pipe.Limit
		case *parser.PipeOffset:
			offset = pipe.Offset
		}
	}

	if limit > 0 {
		p.Ops = append(p.Ops, &LimitOp{Limit: limit, Offset: offset})
	}

	if err := p.applyOffsetID(params.OffsetID, offset); err != nil {
		return nil, err
	}
	return p, nil
}

var searchAllTerms = []parser.Term{{
	Kind: parser.TermSymbol, Data: "*",
}}

func convertStatsAggToAggQuery(statsAgg parser.StatsAgg) (processor.AggQuery, error) {
	aggFunc, err := convertStringToAggFunc(statsAgg.Func)
	if err != nil {
		return processor.AggQuery{}, err
	}

	// 'groupBy' is required for Count and Unique.
	if statsAgg.GroupBy == "" && (aggFunc == seq.AggFuncCount || aggFunc == seq.AggFuncUnique) {
		return processor.AggQuery{}, fmt.Errorf("%w: groupBy is required for %s func", consts.ErrInvalidAggQuery, aggFunc)
	}

	// 'field' is required for stat functions like sum, avg, max and min.
	if statsAgg.Field == "" && aggFunc != seq.AggFuncCount && aggFunc != seq.AggFuncUnique {
		return processor.AggQuery{}, fmt.Errorf("%w: field is required for %s func", consts.ErrInvalidAggQuery, aggFunc)
	}

	// Check 'quantiles' is not empty for Quantile func.
	if len(statsAgg.Quantiles) == 0 && aggFunc == seq.AggFuncQuantile {
		return processor.AggQuery{}, fmt.Errorf("%w: expect an argument for Quantile func", consts.ErrInvalidAggQuery)
	}

	var field *parser.Literal
	if statsAgg.Field != "" {
		field = &parser.Literal{
			Field: statsAgg.Field,
			Terms: searchAllTerms,
		}
	}

	var groupBy *parser.Literal
	if statsAgg.GroupBy != "" {
		groupBy = &parser.Literal{
			Field: statsAgg.GroupBy,
			Terms: searchAllTerms,
		}
	}

	procAgg := processor.AggQuery{
		Field:     field,
		GroupBy:   groupBy,
		Func:      aggFunc,
		Quantiles: statsAgg.Quantiles,
	}

	if statsAgg.Interval != "" {
		interval, err := util.ParseDuration(statsAgg.Interval)
		if err != nil {
			return processor.AggQuery{}, fmt.Errorf("failed to parse interval: %w", err)
		}
		procAgg.Interval = int64(seq.MIDToMillis(seq.MID(interval.Nanoseconds())))
	}

	return procAgg, nil
}

func convertStringToAggFunc(funcName string) (seq.AggFunc, error) {
	switch funcName {
	case "count":
		return seq.AggFuncCount, nil
	case "sum":
		return seq.AggFuncSum, nil
	case "min":
		return seq.AggFuncMin, nil
	case "max":
		return seq.AggFuncMax, nil
	case "avg":
		return seq.AggFuncAvg, nil
	case "quantile":
		return seq.AggFuncQuantile, nil
	case "unique":
		return seq.AggFuncUnique, nil
	case "unique_count":
		return seq.AggFuncUniqueCount, nil
	default:
		return 0, fmt.Errorf("unknown aggregation function: %s", funcName)
	}
}

func (p *Plan) applyOffsetID(offsetID string, offset int) error {
	if offsetID == "" {
		return nil
	}
	if offset != 0 {
		return fmt.Errorf(`only one of "offset" and "offset_id" must be provided`)
	}
	id, err := seq.FromString(offsetID)
	if err != nil {
		return fmt.Errorf("could not parse offset_id: %s", offsetID)
	}
	if p.IsAgg() {
		return fmt.Errorf("offset_id is not supported for aggregation requests")
	}
	p.Scan.OffsetID = id
	if p.Scan.Order == seq.DocsOrderDesc {
		p.Scan.To = id.MID
	} else {
		p.Scan.From = id.MID
	}
	return nil
}
