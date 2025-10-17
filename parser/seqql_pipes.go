package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ozontech/seq-db/util"
)

type Pipe interface {
	Name() string
	DumpSeqQL(*strings.Builder)
}

func parsePipes(lex *lexer) ([]Pipe, error) {
	// Counter of 'fields' pipes.
	fieldFilters := 0
	histograms := 0

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
		case lex.IsKeyword("histogram"):
			p, err := parsePipeHistogram(lex)
			if err != nil {
				return nil, fmt.Errorf("parsing 'histogram' pipe: %s", err)
			}
			pipes = append(pipes, p)
			histograms++
		default:
			return nil, fmt.Errorf("unknown pipe: %s", lex.Token)
		}

		if fieldFilters > 1 {
			return nil, fmt.Errorf("multiple field filters are not allowed")
		}
		if histograms > 1 {
			return nil, fmt.Errorf("multiple histograms are not allowed")
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

type PipeHistogram struct {
	Interval int64
}

func (h *PipeHistogram) Name() string {
	return "histogram"
}

func (h *PipeHistogram) DumpSeqQL(o *strings.Builder) {
	o.WriteString("histogram ")
	interval := time.Duration(h.Interval) * time.Millisecond
	o.WriteString(quoteTokenIfNeeded(interval.String()))
}

func parsePipeHistogram(lex *lexer) (*PipeHistogram, error) {
	if !lex.IsKeyword("histogram") {
		return nil, fmt.Errorf("missing 'histogram' keyword")
	}

	lex.Next()

	interval, err := parseInterval(lex)
	if err != nil {
		return nil, fmt.Errorf("can't parse histogram interval")
	}

	return &PipeHistogram{
		Interval: interval,
	}, nil
}

func parseInterval(lex *lexer) (int64, error) {
	interval, err := parseCompositeTokenReplaceWildcards(lex)
	if err != nil {
		return 0, err
	}

	parsed, err := util.ParseDuration(interval)
	if err != nil {
		return 0, err
	}

	return parsed.Milliseconds(), nil
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
	"fields", "except", "histogram",
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
