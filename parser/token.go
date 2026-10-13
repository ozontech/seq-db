package parser

import (
	"fmt"
	"strconv"
	"strings"
)

type Token any

// TokenKey returns a canonical string uniquely identifying the lookup
// semantics of the token: two tokens yield the same key if and only if they
// select the same field and match tokens the same way. This holds because
// every field a search reads (Literal.Terms, Range bounds, IPRange bounds,
// Re.Expression) is normalized at parse time, so the key depends only on
// parsed values, not on the identity of the AST node — unlike a direct map
// key, it is stable across independently parsed but equal queries.
//
// String parts are length-prefixed, so keys of different tokens cannot
// collide through concatenation.
func TokenKey(t Token) string {
	var b strings.Builder
	switch t := t.(type) {
	case *Literal:
		b.WriteString("literal")
		writeKeyPart(&b, t.Field)
		writeKeyPart(&b, strconv.Itoa(len(t.Terms)))
		for _, term := range t.Terms {
			if term.Kind == TermText {
				b.WriteByte('t')
			} else {
				b.WriteByte('s')
			}
			writeKeyPart(&b, term.Data)
		}
	case *Range:
		b.WriteString("range")
		writeKeyPart(&b, t.Field)
		writeTermKeyPart(&b, t.From)
		if t.IncludeFrom {
			b.WriteByte('I')
		}
		writeTermKeyPart(&b, t.To)
		if t.IncludeTo {
			b.WriteByte('I')
		}
	case *IPRange:
		// From and To are canonicalized to netip strings at parse time.
		b.WriteString("ip_range")
		writeKeyPart(&b, t.Field)
		writeKeyPart(&b, t.From.Data)
		writeKeyPart(&b, t.To.Data)
	case *Re:
		// CompiledExpression and Prefix/Middle/Suffix are derived from
		// Expression.Data at parse time, so the latter fully determines
		// the search behavior.
		b.WriteString("re")
		writeKeyPart(&b, t.Field)
		writeKeyPart(&b, t.Expression.Data)
	default:
		panic(fmt.Sprintf("unknown token type: %T", t))
	}
	return b.String()
}

func writeKeyPart(b *strings.Builder, s string) {
	b.WriteString(strconv.Itoa(len(s)))
	b.WriteByte(':')
	b.WriteString(s)
}

func writeTermKeyPart(b *strings.Builder, t Term) {
	if t.Kind == TermText {
		b.WriteByte('t')
	} else {
		b.WriteByte('s')
	}
	writeKeyPart(b, t.Data)
}
