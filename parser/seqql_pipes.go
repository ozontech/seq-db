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

// pipeOrder defines the allowed order of pipes in a SeqQL query:
// stats | fields | sort | limit | offset. Any pipe may be omitted, but the
// present ones must appear in this exact sequence. The value is the position of
// the pipe in that sequence.
var pipeOrder = map[string]int{
	"stats":  0,
	"fields": 1,
	"sort":   2,
	"limit":  3,
	"offset": 4,
}

var pipeNameFromOrder = map[int]string{
	0: "stats",
	1: "fields",
	2: "sort",
	3: "limit",
	4: "offset",
}

// parsePipes parses the pipe stage of a SeqQL query. The pipes must appear in
// the fixed order stats | fields | sort | limit | offset; pipes may be omitted,
// but none may appear out of order.
func parsePipes(lex *lexer) ([]Pipe, error) {
	seen := make(map[string]struct{})
	lastOrder := -1

	var pipes []Pipe
	for !lex.IsEnd() {
		if !lex.IsKeyword("|") {
			return nil, fmt.Errorf("expect pipe separator '|', got %s", lex.Token)
		}
		lex.Next()

		var name string

		switch {
		case lex.IsKeyword("fields"):
			name = "fields"
			p, err := parsePipeFields(lex)
			if err != nil {
				return nil, fmt.Errorf("parsing 'fields' pipe: %s", err)
			}
			pipes = append(pipes, p)
		case lex.IsKeyword("stats"):
			name = "stats"
			p, err := parsePipeStats(lex)
			if err != nil {
				return nil, fmt.Errorf("parsing 'stats' pipe: %s", err)
			}
			pipes = append(pipes, p)
		case lex.IsKeyword("sort"):
			name = "sort"
			p, err := parsePipeSort(lex)
			if err != nil {
				return nil, fmt.Errorf("parsing 'sort' pipe: %s", err)
			}
			pipes = append(pipes, p)
		case lex.IsKeyword("limit"):
			name = "limit"
			p, err := parsePipeLimit(lex)
			if err != nil {
				return nil, fmt.Errorf("parsing 'limit' pipe: %s", err)
			}
			pipes = append(pipes, p)
		case lex.IsKeyword("offset"):
			name = "offset"
			p, err := parsePipeOffset(lex)
			if err != nil {
				return nil, fmt.Errorf("parsing 'offset' pipe: %s", err)
			}
			pipes = append(pipes, p)
		default:
			return nil, fmt.Errorf("unknown pipe: %s", lex.Token)
		}

		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("multiple '%s' pipes are not allowed", name)
		}
		if order := pipeOrder[name]; order <= lastOrder {
			return nil, fmt.Errorf("pipe '%s' must come before '%s'", pipeNameFromOrder[lastOrder], name)
		}

		seen[name] = struct{}{}
		lastOrder = pipeOrder[name]
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
	Agg StatsAgg
}

func (p *PipeStats) Name() string {
	return "stats"
}

func (p *PipeStats) DumpSeqQL(o *strings.Builder) {
	o.WriteString("stats ")
	agg := p.Agg
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

	agg, err := parseStatsAgg(lex)
	if err != nil {
		return nil, err
	}

	if lex.IsKeyword(",") {
		return nil, fmt.Errorf("stats pipe allows only one aggregation")
	}

	return &PipeStats{Agg: agg}, nil
}

type PipeLimit struct {
	Limit int
}

func (l *PipeLimit) Name() string {
	return "limit"
}

func (l *PipeLimit) DumpSeqQL(o *strings.Builder) {
	o.WriteString("limit ")
	o.WriteString(strconv.Itoa(l.Limit))
}

func parsePipeLimit(lex *lexer) (*PipeLimit, error) {
	if !lex.IsKeyword("limit") {
		return nil, fmt.Errorf("missing 'limit' keyword")
	}
	lex.Next()

	limitStr := lex.Token
	if limitStr == "" {
		return nil, fmt.Errorf("missing limit value")
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		return nil, fmt.Errorf("invalid limit value: %s", limitStr)
	}

	if limit <= 0 {
		return nil, fmt.Errorf("limit must be greater than 0, got %d", limit)
	}

	lex.Next()

	return &PipeLimit{Limit: limit}, nil
}

type PipeOffset struct {
	Offset int
}

func (l *PipeOffset) Name() string {
	return "offset"
}

func (l *PipeOffset) DumpSeqQL(o *strings.Builder) {
	o.WriteString("offset ")
	o.WriteString(strconv.Itoa(l.Offset))
}

func parsePipeOffset(lex *lexer) (*PipeOffset, error) {
	if !lex.IsKeyword("offset") {
		return nil, fmt.Errorf("missing 'offset' keyword")
	}
	lex.Next()

	offsetStr := lex.Token
	if offsetStr == "" {
		return nil, fmt.Errorf("missing offset value")
	}

	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		return nil, fmt.Errorf("invalid offset value: %s", offsetStr)
	}

	if offset <= 0 {
		return nil, fmt.Errorf("offset must be greater than 0, got %d", offset)
	}

	lex.Next()

	return &PipeOffset{Offset: offset}, nil
}

type PipeSort struct {
	Order string
}

func (s *PipeSort) Name() string {
	return "sort"
}

func (s *PipeSort) DumpSeqQL(o *strings.Builder) {
	o.WriteString("sort ")
	o.WriteString(s.Order)
}

func parsePipeSort(lex *lexer) (*PipeSort, error) {
	if !lex.IsKeyword("sort") {
		return nil, fmt.Errorf("missing 'sort' keyword")
	}
	lex.Next()

	if !lex.IsKeywords("asc", "desc") {
		return nil, fmt.Errorf("expected `asc` or `desc` order")
	}

	order := lex.Token
	lex.Next()

	return &PipeSort{
		Order: order,
	}, nil
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
	"fields", "except", "limit", "offset", "sort", "stats", "by", "interval", "unique_count", "asc", "desc",
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
