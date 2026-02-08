package parser

import (
	"errors"
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
	b.WriteString(`:ip_range(`)
	b.WriteString(r.Expression.Data)
	b.WriteString(`)`)
}

func parseReFilter(lex *lexer, fieldName string) (*Re, error) {
	if !lex.IsKeyword("(") {
		return nil, fmt.Errorf("expected '(', got %q", lex.Token)
	}

	lex.Next()
	if lex.IsKeyword(")") {
		return nil, errors.New("empty 're' filter")
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

	// NB(dkharms): Check [lexer.Next] and [unquoteChar].
	//
	// If lexer encounters wildcard symbol (e.g. '*') inside quoted string
	// it replaces it with [wildcardRune].
	//
	// If lexer encounters escaped wildcard symbol (e.g. '\*') inside quoted string
	// it replaces it with '*'.
	//
	// While this behaviour is correct for full-text search,
	// it is not correct for regular expressions.
	//
	// So we basically undo all previous transformations.
	// Please pay attention that order of undo transformations does matter.
	expr = strings.ReplaceAll(expr, "*", "\\*")
	expr = strings.ReplaceAll(expr, string(wildcardRune), "*")

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
