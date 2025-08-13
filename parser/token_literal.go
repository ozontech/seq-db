package parser

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/ozontech/seq-db/seq"
)

type Literal struct {
	Field string
	Terms []Term
}

func (n *Literal) Dump(builder *strings.Builder) {
	builder.WriteString(n.Field)
	builder.WriteString(`:`)
	for _, term := range n.Terms {
		term.Dump(builder)
	}
}

func (n *Literal) DumpSeqQL(o *strings.Builder) {
	if n.Field == seq.TokenAll && len(n.Terms) == 1 && n.Terms[0].IsWildcard() {
		o.WriteString("*")
		return
	}
	o.WriteString(quoteTokenIfNeeded(n.Field))
	o.WriteString(`:`)
	for _, term := range n.Terms {
		term.DumpSeqQL(o)
	}
}

func (n *Literal) String() string {
	builder := &strings.Builder{}
	n.Dump(builder)
	return builder.String()
}

func (n *Literal) appendTerm(term Term, sensitive bool) {
	if !sensitive {
		term.Data = strings.ToLower(term.Data)
	}
	n.Terms = append(n.Terms, term)
}

type TermKind int

const (
	TermText TermKind = iota
	TermSymbol
)

type Term struct {
	Kind TermKind
	Data string
}

func (t Term) Dump(builder *strings.Builder) {
	switch t.Kind {
	case TermText:
		if t.Data == "" {
			builder.WriteString(`""`)
			return
		}
		for _, c := range t.Data {
			if specialSymbol[c] || unicode.IsSpace(c) {
				builder.WriteByte('\\')
			}
			builder.WriteRune(c)
		}
	case TermSymbol:
		builder.WriteString(t.Data)
	default:
		panic("unknown term kind")
	}
}

func (t Term) IsWildcard() bool {
	return t.Kind == TermSymbol && t.Data == "*"
}

func (t Term) DumpSeqQL(b *strings.Builder) {
	if t.IsWildcard() {
		b.WriteString("*")
		return
	}
	b.WriteString(quoteTokenIfNeeded(t.Data))
}

var specialSymbol = map[rune]bool{
	'(':  true,
	')':  true,
	'{':  true,
	'}':  true,
	'[':  true,
	']':  true,
	'*':  true,
	'"':  true,
	'\\': true,
	':':  true,
}

var graylogEscapedSymbol = map[rune]bool{
	'-': true,
	'/': true,
}

var quoteEscapedSymbol = map[rune]bool{
	'"':  true,
	'\\': true,
	'*':  true,
}

func GetField(token Token) string {
	switch t := token.(type) {
	case *Literal:
		return t.Field
	case *Range:
		return t.Field
	case *IPRange:
		return t.Field
	}
	panic(fmt.Sprintf("unknown token type: %T", token))
}

func GetHint(token Token) string {
	switch t := token.(type) {
	case *Literal:
		if t.Terms[0].Kind == TermText {
			return t.Terms[0].Data
		}
	default:
	}
	return ""
}
