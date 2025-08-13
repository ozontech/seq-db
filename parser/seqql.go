package parser

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/ozontech/seq-db/seq"
)

type SeqQLQuery struct {
	Root  *ASTNode
	Pipes []Pipe
}

func (q *SeqQLQuery) SeqQLString() string {
	b := &strings.Builder{}
	q.Root.DumpSeqQL(b)
	for _, p := range q.Pipes {
		b.WriteString(" | ")
		p.DumpSeqQL(b)
	}
	return b.String()
}

func ParseSeqQL(q string, mapping seq.Mapping) (SeqQLQuery, error) {
	lex := newLexer(q)

	lex.Next()
	root, err := parseSeqQLFilter(&lex, mapping, 0)
	if err != nil {
		return SeqQLQuery{}, err
	}

	var pipes []Pipe
	if lex.IsKeyword("|") {
		pipes, err = parsePipes(&lex)
		if err != nil {
			return SeqQLQuery{}, err
		}
	}

	if !lex.IsEnd() {
		panic(fmt.Errorf("BUG: lexer is not end: %+v", lex))
	}

	root, not := propagateNot(root)
	if not {
		root = newNotNode(root)
	}

	return SeqQLQuery{
		Root:  root,
		Pipes: pipes,
	}, nil
}

// wildcardRune is unicode symbol from Private Use Area to represent wildcard.
// It uses in parser to distinguish wildcard from asterisk.
const wildcardRune = '\uE000'

type lexer struct {
	// q is query tail.
	q string

	// Token is current token.
	Token string
	// SpaceSkipped is true if space was skipped before current token.
	SpaceSkipped bool
	// TokenQuoted is true if current token is quoted.
	TokenQuoted bool
	// rawString is true if current token is raw string (string quoted with `).
	rawString bool
}

func newLexer(q string) lexer {
	return lexer{
		q:            q,
		Token:        "",
		TokenQuoted:  false,
		SpaceSkipped: false,
		rawString:    false,
	}
}

// IsKeyword returns true if current token is keyword.
func (lex *lexer) IsKeyword(token string) bool {
	if lex.TokenQuoted {
		return false
	}
	return strings.EqualFold(lex.Token, token)
}

// IsKeywords returns true if current token is in tokens list.
func (lex *lexer) IsKeywords(tokens ...string) bool {
	if lex.TokenQuoted {
		return false
	}
	for _, t := range tokens {
		if strings.EqualFold(lex.Token, t) {
			return true
		}
	}
	return false
}

// IsKeywordSet returns true if current token is in tokens set.
func (lex *lexer) IsKeywordSet(tokens map[string]struct{}) bool {
	if lex.TokenQuoted {
		return false
	}
	token := strings.ToLower(lex.Token)
	_, ok := tokens[token]
	return ok
}

func (lex *lexer) IsEnd() bool {
	return lex.q == "" && lex.Token == "" && !lex.TokenQuoted
}

// IsRawString returns true if current token is raw string (string quoted with `).
func (lex *lexer) IsRawString() bool {
	return lex.rawString && lex.TokenQuoted
}

// Next moves lexer to next token.
// It skips spaces and comments (lines that starts with #).
//
// Token is sequence of letters, numbers, '_' and '.', other symbols are different tokens if it is not quoted.
// Token can be empty in case of quoted token or end of query.
//
// TokenQuoted is true if current token is quoted with ', " or `.
// Field rawString is true if current token is quoted with `. Escape sequences in raw strings are ignored.
//
// SpaceSkipped is true if space was skipped before current token.
func (lex *lexer) Next() {
	lex.Token = ""
	lex.TokenQuoted = false
	lex.SpaceSkipped = false
	lex.rawString = false

again:
	r, size := utf8.DecodeRuneInString(lex.q)
	if r == utf8.RuneError {
		// It is empty string or invalid UTF-8 sequence.
		lex.nextToken(size)
		return
	}

	// Skip spaces.
	for unicode.IsSpace(r) {
		lex.q = lex.q[size:]
		r, size = utf8.DecodeRuneInString(lex.q)
		lex.SpaceSkipped = true
	}

	// Skip comment line.
	if r == '#' {
		n := strings.IndexByte(lex.q[1:], '\n')
		if n == -1 {
			lex.q = ""
		} else {
			lex.q = lex.q[n+1:]
		}
		goto again
	}

	// Decode simple token.
	tokenLen := 0
	for isTokenRune(r) {
		tokenLen += size
		r, size = utf8.DecodeRuneInString(lex.q[tokenLen:])
	}
	if tokenLen > 0 {
		lex.nextToken(tokenLen)
		return
	}

	if r == '*' {
		lex.Token = string(wildcardRune)
		lex.q = lex.q[size:]
		return
	}

	switch r {
	case '\'', '"':
		out, rem, err := unquotePrefix(lex.q)
		if err != nil {
			lex.nextToken(1)
			return
		}
		lex.Token = out
		lex.q = rem
		lex.TokenQuoted = true
		return
	case '`':
		quotedPrefix, err := strconv.QuotedPrefix(lex.q)
		if err != nil {
			lex.nextToken(1)
			return
		}
		token := quotedPrefix[1 : len(quotedPrefix)-1]
		lex.Token = token
		lex.q = lex.q[len(quotedPrefix):]
		lex.TokenQuoted = true
		lex.rawString = true
		return
	default:
		// Consume only 1 rune.
		lex.nextToken(size)
		return
	}
}

func isTokenRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '.'
}

// unquotePrefix returns unquoted string prefix and remaining string.
// For example, if q is `"payment-api" and level:"info"` then out will be `payment-api` and rem will be ` and level:"info"`.
func unquotePrefix(q string) (out, rem string, _ error) {
	if len(q) < 2 {
		return "", "", strconv.ErrSyntax
	}
	quote := q[0]
	if quote != '"' && quote != '`' && quote != '\'' {
		return "", "", strconv.ErrSyntax
	}

	// Find end of the quoted token.
	end := strings.IndexByte(q[1:], quote)
	if end == -1 {
		return "", "", strconv.ErrSyntax
	}
	end++

	if !needUnquote(q[1:end]) {
		// No need to escape special characters.
		return q[1:end], q[end+1:], nil
	}

	// Unquote 'token' -> token.
	prefix := q[1:]
	b := make([]byte, 0, len(prefix))
	remIdx := 1
	for prefix != "" && prefix[0] != quote {
		ch, newTail, err := unquoteChar(prefix, quote)
		if err != nil {
			// Skip invalid escaped characters.
			b = append(b, '\\')
			prefix = prefix[1:]
			remIdx++
			continue
		}
		b = utf8.AppendRune(b, ch)
		remIdx += len(prefix) - len(newTail)
		prefix = newTail
	}
	remIdx++ // Skip last quote.

	// Verify prefix ends with quote.
	if prefix == "" || prefix[0] != quote {
		return "", "", strconv.ErrSyntax
	}

	return string(b), q[remIdx:], nil
}

// needUnquote returns true if q contains special characters that need to be unquoted.
// For example,
//
//	"payment-api" -> false
//	"pay*ment-api" -> true
//	"pay\\*ment-api" -> true
//	"\t" -> true
func needUnquote(q string) bool {
	return strings.IndexByte(q, '\\') != -1 || strings.IndexByte(q, '*') != -1
}

// unquoteChar is like strconv.UnquoteChar but also supports wildcard character.
func unquoteChar(s string, quote byte) (value rune, tail string, err error) {
	if strings.HasPrefix(s, `\*`) {
		return '*', s[2:], nil
	}
	if strings.HasPrefix(s, "*") {
		return wildcardRune, s[1:], nil
	}
	ch, _, newTail, err := strconv.UnquoteChar(s, quote)
	return ch, newTail, err
}

func (lex *lexer) nextToken(size int) {
	lex.Token = lex.q[:size]
	lex.q = lex.q[size:]
}

// parseSeqQLFilter parses SeqQL full text search filters like `service:payment-api and level:"info"` and returns AST node.
func parseSeqQLFilter(lex *lexer, mapping seq.Mapping, depth int) (*ASTNode, error) {
	var res *ASTNode

	cur, err := parseSeqQLSubexpr(lex, mapping, depth)
	if err != nil {
		return nil, err
	}

	for {
		var opKind logicalKind
		switch {
		case lex.IsKeyword("and"):
			opKind = LogicalAnd
		case lex.IsKeyword("or"):
			opKind = LogicalOr
		default:
			if lex.IsEnd() || lex.IsKeyword(")") && depth > 0 || lex.IsKeyword("|") {
				return joinOr(res, cur), nil
			}
			return nil, fmt.Errorf("expected 'and', 'or', 'not', got: %q", lex.Token)
		}

		lex.Next()
		next, err := parseSeqQLSubexpr(lex, mapping, depth)
		if err != nil {
			return nil, err
		}

		if opKind == LogicalAnd {
			cur = newLogicalNode(LogicalAnd, cur, next)
			continue
		}

		res = joinOr(res, cur)
		cur = next
	}

}

func joinOr(left, right *ASTNode) *ASTNode {
	if left == nil {
		return right
	}
	return newLogicalNode(LogicalOr, left, right)
}

func parseSeqQLSubexpr(lex *lexer, mapping seq.Mapping, depth int) (*ASTNode, error) {
	if lex.IsEnd() {
		return nil, fmt.Errorf("unexpected end of query")
	}

	if lex.IsKeyword(string(wildcardRune)) && depth == 0 {
		lex.Next()
		// Query is `*`.
		return &ASTNode{
			Value: &Literal{
				Field: seq.TokenAll,
				Terms: []Term{{Kind: TermSymbol, Data: "*"}},
			},
		}, nil
	}

	if lex.IsKeyword("(") {
		lex.Next()
		expr, err := parseSeqQLFilter(lex, mapping, depth+1)
		if err != nil {
			return nil, err
		}
		if !lex.IsKeyword(")") {
			return nil, fmt.Errorf("missing ')'")
		}
		lex.Next()
		return expr, nil
	}

	if lex.IsKeyword("not") {
		lex.Next()
		child, err := parseSeqQLSubexpr(lex, mapping, depth)
		if err != nil {
			return nil, err
		}
		return newNotNode(child), nil
	}

	ast, err := parseSeqQLFieldFilter(lex, mapping)
	if err != nil {
		return nil, err
	}
	return ast, nil
}

func uniqueTokens(s []string) map[string]struct{} {
	m := make(map[string]struct{})
	for _, v := range s {
		m[v] = struct{}{}
	}
	return m
}
