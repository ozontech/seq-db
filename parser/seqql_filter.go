package parser

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/seq"
)

func parseSeqQLFieldFilter(lex *lexer, mapping seq.Mapping) (*ASTNode, error) {
	fieldName, err := parseCompositeTokenReplaceWildcards(lex)
	if err != nil {
		return nil, fmt.Errorf("parsing field name: %s", err)
	}
	if fieldName == "" {
		return nil, fmt.Errorf("empty field name")
	}

	t := indexType(mapping, fieldName)
	if t == seq.TokenizerTypeNoop {
		return nil, fmt.Errorf("field %q is not indexed", fieldName)
	}

	if !lex.IsKeyword(":") {
		return nil, fmt.Errorf("missing ':' after %q", fieldName)
	}

	lex.Next()
	if lex.IsKeyword("") {
		return nil, fmt.Errorf("missing filter value for field %q", fieldName)
	}

	caseSensitive := config.CaseSensitive
	if fieldName == seq.TokenExists {
		caseSensitive = true
	}

	// Parse range filter.
	if lex.IsKeywords("[", "(") {
		r, err := parseSeqQLTokenRange(fieldName, lex, caseSensitive)
		if err != nil {
			return nil, fmt.Errorf("parsing range for field %q: %s", fieldName, err)
		}
		return &ASTNode{Value: r}, nil
	}

	if lex.IsKeyword("ip_range") {
		if t != seq.TokenizerTypeKeyword {
			return nil, fmt.Errorf("'ip_range' filter is supported only for keyword fields")
		}

		lex.Next()
		r, err := parseFilterIPRange(lex, fieldName)
		if err != nil {
			return nil, fmt.Errorf("parsing 'ip_range' filter: %s", err)
		}
		return &ASTNode{Value: r}, nil
	}

	if lex.IsKeyword("in") {
		lex.Next()
		ast, err := parseFilterIn(lex, fieldName, t, caseSensitive)
		if err != nil {
			return nil, fmt.Errorf("parsing 'in' filter: %s", err)
		}
		return ast, nil
	}

	ast, err := parseFulltextSearchFilter(lex, fieldName, t, caseSensitive)
	if err != nil {
		return nil, err
	}
	return ast, nil
}

func parseFulltextSearchFilter(lex *lexer, fieldName string, t seq.TokenizerType, caseSensitive bool) (*ASTNode, error) {
	value, err := parseCompositeToken(lex)
	if err != nil {
		return nil, fmt.Errorf("parsing filter value for field %q: %s", fieldName, err)
	}
	switch t {
	case seq.TokenizerTypeKeyword, seq.TokenizerTypePath:
		terms, err := parseSeqQLKeyword(value, caseSensitive)
		if err != nil {
			return nil, fmt.Errorf("parsing keyword for field %q: %s", fieldName, err)
		}
		return &ASTNode{Value: &Literal{Field: fieldName, Terms: terms}}, nil
	case seq.TokenizerTypeText:
		tokens, err := parseSeqQLText(fieldName, value, caseSensitive)
		if err != nil {
			return nil, fmt.Errorf("parsing text for field %q: %s", fieldName, err)
		}
		return buildAndTree(tokens), nil
	default:
		panic(fmt.Errorf("BUG: unexpected index type: %d", t))
	}
}

// parseFilterIn parses 'in' filter.
// Filter 'in' is a logical OR of multiple Literal.
// It supports all forms of seq-ql string literals.
// Example queries:
//
//	service:in(auth-api, api-gateway, clickhouse-shard-*)
//	phone:in(`+7 999 ** **`, '+995'*)
func parseFilterIn(lex *lexer, fieldName string, t seq.TokenizerType, caseSensitive bool) (*ASTNode, error) {
	if !lex.IsKeyword("(") {
		return nil, fmt.Errorf("expected '(', got %q", lex.Token)
	}
	lex.Next()

	if lex.IsKeyword(")") {
		return nil, fmt.Errorf("empty 'in' filter")
	}

	textFilter, err := parseFulltextSearchFilter(lex, fieldName, t, caseSensitive)
	if err != nil {
		return nil, err
	}
	root := textFilter
	for lex.IsKeyword(",") {
		lex.Next()
		textFilter, err := parseFulltextSearchFilter(lex, fieldName, t, caseSensitive)
		if err != nil {
			return nil, err
		}
		root = newLogicalNode(LogicalOr, root, textFilter)
	}

	if !lex.IsKeyword(")") {
		return nil, fmt.Errorf("expected ')', got %q", lex.Token)
	}

	lex.Next()
	return root, nil
}

var bytesBufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

// parseCompositeToken parses composite token.
//
// Composite token is a continuous sequence of letters, numbers, '_', '*', '-', and wildcards,
// that continuous until the end of the query or next token is space.
//
// Token may be quoted, but the lexer returns unquoted tokens,
// so query: "foo: 'foo bar'_'buz'" is correct. See lexer.Next for details.
//
// For example, list of composite tokens:
//
//	"foo", "foo-bar", "foo_bar", "foo_bar-and", "foo*bar", "`foo bar``buz`"
//
// List of non-composite tokens:
//
//	"foo bar", "$foo", "foo$", "f$$", "@gmail.com".
func parseCompositeToken(lex *lexer, addons ...rune) (string, error) {
	if lex.IsKeyword("") {
		return "", fmt.Errorf("unexpected end of query")
	}
	if !isCompositeToken(lex, addons...) {
		// Disallow unquoted tokens that starts with non-letter and non-number symbols.
		return "", fmt.Errorf("unexpected symbol %q", lex.Token)
	}

	firstToken := lex.Token
	lex.Next()
	if lex.SpaceSkipped || !isCompositeToken(lex, addons...) {
		// Token is single word and not composite.
		// Return it as is.
		return firstToken, nil
	}

	b := bytesBufferPool.Get().(*bytes.Buffer)
	defer bytesBufferPool.Put(b)
	b.Reset()

	// Join tokens to single composite token.
	b.WriteString(firstToken)
	for ; !lex.SpaceSkipped && isCompositeToken(lex, addons...); lex.Next() {
		b.WriteString(lex.Token)
	}
	return b.String(), nil
}

func isCompositeToken(lex *lexer, addons ...rune) bool {
	if lex.IsKeyword("") {
		// End of query.
		return false
	}
	if lex.Token == "" {
		// Empty token.
		return true
	}

	r, size := utf8.DecodeRuneInString(lex.Token)

	hasMoreSymbols := len(lex.Token[size:]) > 1
	if hasMoreSymbols || lex.TokenQuoted {
		// Quoted tokens and tokens with more than one character are part of composite token.
		return true
	}

	return isTokenRune(r) || r == '-' || r == '*' || r == wildcardRune || slices.Contains(addons, r)
}

func parseCompositeTokenReplaceWildcards(lex *lexer) (string, error) {
	s, err := parseCompositeToken(lex)
	if err != nil {
		return "", err
	}
	return strings.ReplaceAll(s, string(wildcardRune), "*"), nil
}

func parseSeqQLKeyword(token string, caseSensitive bool) ([]Term, error) {
	if token == "" {
		return []Term{newTextTerm("")}, nil
	}

	b := bytesBufferPool.Get().(*bytes.Buffer)
	defer bytesBufferPool.Put(b)
	b.Reset()

	var terms []Term
	for token != "" {
		r, size := utf8.DecodeRuneInString(token)
		if r == wildcardRune {
			if data := b.String(); data != "" {
				terms = append(terms, newTextTermCaseSensitive(data, caseSensitive))
			}
			b.Reset()
			terms = append(terms, newSymbolTerm('*'))
			token = token[size:]
			continue
		}
		b.WriteRune(r)
		token = token[size:]
	}
	if data := b.String(); data != "" {
		terms = append(terms, newTextTermCaseSensitive(data, caseSensitive))
	}

	return terms, nil
}

func parseSeqQLText(field, token string, sensitive bool) ([]Token, error) {
	if token == "" {
		return []Token{&Literal{Field: field, Terms: []Term{newTextTerm("")}}}, nil
	}
	var tokens []Token
	current := &Literal{Field: field}

	term := Term{Kind: TermText}
	for token != "" {
		r, size := utf8.DecodeRuneInString(token)
		if unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_' || r == '*' {
			term.Data += string(r)
			token = token[size:]
			continue
		}

		// Term is done.
		if term.Data != "" {
			current.appendTerm(term, sensitive)
			term = Term{Kind: TermText}
		}

		if r == wildcardRune {
			current.Terms = append(current.Terms, newSymbolTerm('*'))
			token = token[size:]
			continue
		}

		// This is separator character like ':', '&', ' ', emoji, etc.
		// So create new literal.

		if len(current.Terms) != 0 {
			tokens = append(tokens, current)
			current = &Literal{Field: field}
		}
		token = token[size:]
	}
	if term.Data != "" {
		current.appendTerm(term, sensitive)
	}

	if current != nil && len(current.Terms) > 0 {
		tokens = append(tokens, current)
	}

	if len(tokens) == 0 {
		// There are no tokens to search, return an empty filter.
		tokens = append(tokens, &Literal{
			Field: field,
			Terms: []Term{{Kind: TermText, Data: ""}},
		})
	}
	return tokens, nil
}
