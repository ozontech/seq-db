package parser

import (
	"fmt"
	"regexp"
	"strings"
)

type Re struct {
	Field              string
	Expression         Term
	CompiledExpression *regexp.Regexp
}

func (r *Re) DumpSeqQL(b *strings.Builder) {
	b.WriteString(quoteTokenIfNeeded(r.Field))
	b.WriteString(`:re(`)
	b.WriteString(`"`)
	b.WriteString(r.CompiledExpression.String())
	b.WriteString(`"`)
	b.WriteString(`)`)
}

func parseReFilter(lex *lexer, fieldName string) (*Re, error) {
	if !lex.IsKeyword("(") {
		return nil, fmt.Errorf("expected '(', got %q", lex.Token)
	}

	if err := lex.rawStringLiteral(); err != nil {
		return nil, err
	}

	// TODO(dkharms): Split logic between lexer and parser.
	// Now lexer is aware of structure of inner structure of lexemes.
	//
	// For example, for string 'foo bar' it will return ['foo', 'bar'].
	// For wildcard it will return \uE000 codepoint.
	//
	// But in the case with regular expressions wildcard (*) is a wildcard (*), not
	// some before chosen placeholder.
	//
	// So we can say that lexer is context aware which is not great.
	// This behaviour has negative impact on language extendability.
	expr := lex.Token

	// Here are two important things to keep in mind:
	//  - We perform case-insensitive search by default;
	//  - We force anchoring for the expression;
	//
	// Case sensitivity can be overridden by the user with `(?-i)` inside the pattern.
	// Anchoring is necessary for prefix search optimization.
	//
	// See Prometheus TSDB `FastRegexMatcher` for a similar approach:
	// https://github.com/prometheus/prometheus/blob/19fd0b0b1dbfe01a5e49f5d04544a7c5853c12bb/model/labels/regexp.go#L70
	expr = "^(?i:" + expr + ")$"

	compiled, err := regexp.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf("invalid expression for `re` filter: %s", err)
	}

	lex.Next()
	if !lex.IsKeyword(")") {
		return nil, fmt.Errorf("expected ')', got %q", lex.Token)
	}

	lex.Next()
	return &Re{
		Field:              fieldName,
		Expression:         newTextTerm(expr),
		CompiledExpression: compiled,
	}, nil
}
