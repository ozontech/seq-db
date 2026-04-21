package parser

import (
	"fmt"
	"strconv"
	"strings"
)

type Pipe interface {
	Name() string
	DumpSeqQL(*strings.Builder)
}

func parsePipes(lex *lexer) ([]Pipe, error) {
	// Counter of 'fields' pipes.
	fieldFilters := 0
	var pipes []Pipe
	for !lex.IsEnd() {
		if !lex.IsKeyword("|") {
			return nil, fmt.Errorf("expect pipe separator '|', got %s", lex.Token)
		}
		lex.Next()

		switch {
		case lex.IsKeyword("fields"):
			p, err := parsePipeFields(lex)
			if err != nil {
				return nil, fmt.Errorf("parsing 'fields' pipe: %s", err)
			}
			pipes = append(pipes, p)
			fieldFilters++
		case lex.IsKeyword("stats"):
			p, err := parsePipeStats(lex)
			if err != nil {
				return nil, fmt.Errorf("parsing 'stats' pipe: %s", err)
			}
			pipes = append(pipes, p)
		default:
			return nil, fmt.Errorf("unknown pipe: %s", lex.Token)
		}

		if fieldFilters > 1 {
			return nil, fmt.Errorf("multiple field filters is not allowed")
		}
	}
	return pipes, nil
}

type PipeFields struct {
	Fields []string
	Except bool
}

func (f *PipeFields) Name() string {
	return "fields"
}

func (f *PipeFields) DumpSeqQL(o *strings.Builder) {
	o.WriteString("fields ")
	if f.Except {
		o.WriteString("except ")
	}
	for i, field := range f.Fields {
		if i > 0 {
			o.WriteString(", ")
		}
		o.WriteString(quoteTokenIfNeeded(field))
	}
}

type StatsAgg struct {
	Func      string
	Field     string
	GroupBy   string
	Interval  string
	Quantiles []float64
}

type PipeStats struct {
	Aggs []StatsAgg
}

func (p *PipeStats) Name() string {
	return "stats"
}

func (p *PipeStats) DumpSeqQL(o *strings.Builder) {
	o.WriteString("stats ")
	for i, agg := range p.Aggs {
		if i > 0 {
			o.WriteString(", ")
		}
		o.WriteString(agg.Func)
		if agg.Field != "" {
			o.WriteString("(")
			o.WriteString(quoteTokenIfNeeded(agg.Field))
			for _, q := range agg.Quantiles {
				fmt.Fprintf(o, ", %v", q)
			}
			o.WriteString(")")
		}
		if agg.GroupBy != "" {
			o.WriteString(" by (")
			o.WriteString(quoteTokenIfNeeded(agg.GroupBy))
			o.WriteString(")")
		}
		if agg.Interval != "" {
			o.WriteString(" interval(")
			o.WriteString(agg.Interval)
			o.WriteString(")")
		}
	}
}

func parsePipeFields(lex *lexer) (*PipeFields, error) {
	if !lex.IsKeyword("fields") {
		return nil, fmt.Errorf("missing 'fields' keyword")
	}

	lex.Next()
	except := false
	if lex.IsKeyword("except") {
		except = true
		lex.Next()
	}

	fields, err := parseFieldList(lex)
	if err != nil {
		return nil, err
	}

	return &PipeFields{
		Fields: fields,
		Except: except,
	}, nil
}

func parsePipeStats(lex *lexer) (*PipeStats, error) {
	if !lex.IsKeyword("stats") {
		return nil, fmt.Errorf("missing 'stats' keyword")
	}
	lex.Next()

	var aggs []StatsAgg
	for {
		agg, err := parseStatsAgg(lex)
		if err != nil {
			return nil, err
		}
		aggs = append(aggs, agg)

		if !lex.IsKeyword(",") {
			break
		}
		lex.Next()
	}

	if len(aggs) == 0 {
		return nil, fmt.Errorf("at least one aggregation is required")
	}

	return &PipeStats{Aggs: aggs}, nil
}

func parseStatsAgg(lex *lexer) (StatsAgg, error) {
	var agg StatsAgg

	if !lex.IsKeywords("count", "sum", "min", "max", "avg", "quantile", "unique", "unique_count") {
		return agg, fmt.Errorf("expected aggregation function (count, sum, min, max, avg, quantile, unique, unique_count), got %s", lex.Token)
	}
	agg.Func = strings.ToLower(lex.Token)
	lex.Next()

	if lex.IsKeyword("(") {
		lex.Next()
		field, err := parseCompositeTokenReplaceWildcards(lex)
		if err != nil {
			return agg, err
		}
		agg.Field = field

		for lex.IsKeyword(",") {
			lex.Next()
			q, err := parseNumber(lex)
			if err != nil {
				return agg, fmt.Errorf("failed to parse quantile: %w", err)
			}
			agg.Quantiles = append(agg.Quantiles, q)
		}

		if !lex.IsKeyword(")") {
			return agg, fmt.Errorf("expected ')' after field, got %s", lex.Token)
		}
		lex.Next()
	}

	if lex.IsKeyword("by") {
		lex.Next()
		if !lex.IsKeyword("(") {
			return agg, fmt.Errorf("expected '(' after 'by', got %s", lex.Token)
		}
		lex.Next()
		groupBy, err := parseCompositeTokenReplaceWildcards(lex)
		if err != nil {
			return agg, err
		}
		agg.GroupBy = groupBy
		if !lex.IsKeyword(")") {
			return agg, fmt.Errorf("expected ')' after groupBy, got %s", lex.Token)
		}
		lex.Next()
	}

	if lex.IsKeyword("interval") {
		lex.Next()
		if !lex.IsKeyword("(") {
			return agg, fmt.Errorf("expected '(' after 'interval', got %s", lex.Token)
		}
		lex.Next()
		interval := lex.Token
		if interval == "" {
			return agg, fmt.Errorf("expected interval value, got %s", lex.Token)
		}
		agg.Interval = interval
		lex.Next()
		if !lex.IsKeyword(")") {
			return agg, fmt.Errorf("expected ')' after interval, got %s", lex.Token)
		}
		lex.Next()
	}

	return agg, nil
}

func parseNumber(lex *lexer) (float64, error) {
	if lex.Token == "" {
		return 0, fmt.Errorf("expected number, got empty token")
	}
	q, err := strconv.ParseFloat(lex.Token, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse number %s: %w", lex.Token, err)
	}
	lex.Next()
	return q, nil
}

func parseFieldList(lex *lexer) ([]string, error) {
	var fields []string
	trailingComma := false
	for !lex.IsKeywords("|", "") {
		trailingComma = false
		field, err := parseCompositeTokenReplaceWildcards(lex)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
		if lex.IsKeyword(",") {
			lex.Next()
			trailingComma = true
		}
	}
	if trailingComma {
		return nil, fmt.Errorf("trailing comma not allowed")
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("empty list")
	}
	return fields, nil
}

func quoteTokenIfNeeded(token string) string {
	if !needQuoteToken(token) {
		return token
	}
	return quote(token)
}

// quote returns string with escaped special characters.
func quote(s string) string {
	s = strconv.Quote(s)
	s = strings.ReplaceAll(s, "*", `\*`)
	return s
}

var reservedKeywords = uniqueTokens([]string{
	// End of query.
	"",
	// Range filter and parentheses for grouping filters.
	"(", ")",
	// Range filter.
	"[", "]",
	// Range border separators.
	",",

	// Logical operators.
	"or",
	"and",
	"not",

	// Wildcard.
	"*",

	// Field delimiter.
	":",

	// Pipe separator.
	"|",

	// Pipe specific keywords.
	"fields", "except", "stats", "by", "interval", "unique_count",
})

func needQuoteToken(s string) bool {
	if _, ok := reservedKeywords[strings.ToLower(s)]; ok {
		return true
	}
	for _, r := range s {
		if !isTokenRune(r) && r != '-' {
			return true
		}
	}
	return false
}
