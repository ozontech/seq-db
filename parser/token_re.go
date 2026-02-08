package parser

import (
	"errors"
	"fmt"
	"strings"
)

var metacharacters = []rune{
	'\\', '?', '+', '*',
	'[', ']', '{', '}',
}

type Re struct {
	Field      string
	Expression Term
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

	tok, err := parseCompositeToken(lex, metacharacters...)
	if err != nil {
		return nil, err
	}

	if !lex.IsKeyword(")") {
		return nil, fmt.Errorf("expected ')', got %q", lex.Token)
	}

	lex.Next()

	return &Re{
		Field:      fieldName,
		Expression: newTextTerm(tok),
	}, nil
}
