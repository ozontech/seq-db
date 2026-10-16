package parser

import (
	"fmt"
	"strings"
)

type Token any

// TokenKey returns a canonical string uniquely identifying the
// lookup semantics of the token. It delegates to DumpSeqQL, whose output
// reparses into the same token (parser round-trip), so equal keys imply
// equal lookup semantics and no false cache hits are possible.
func TokenKey(t Token) string {
	var b strings.Builder
	switch t.(type) {
	case *Literal, *Range, *IPRange, *Re:
		t.(interface{ DumpSeqQL(*strings.Builder) }).DumpSeqQL(&b)
	default:
		panic(fmt.Sprintf("unknown token type: %T", t))
	}
	return b.String()
}
