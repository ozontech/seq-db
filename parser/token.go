package parser

import (
	"fmt"
	"strconv"
	"strings"
)

type Token any

// TokenKey returns a canonical string uniquely identifying the
// lookup semantics of the token. The mapping is injective — distinct
// tokens never produce the same key — because every variable part is
// length-prefixed (writeKeyPart) and type tags are mutually prefix-free;
// preserve both invariants when extending the switch below.
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
		b.WriteString("ip_range")
		writeKeyPart(&b, t.Field)
		writeKeyPart(&b, t.From.Data)
		writeKeyPart(&b, t.To.Data)
	case *Re:
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
