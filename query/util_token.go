package query

import (
	"strings"

	"github.com/ozontech/seq-db/util"
)

func Q(q string) []byte {
	return []byte(q)
}

func T(tokenStr string) Token {
	fieldPos := strings.IndexByte(tokenStr, ':')
	if fieldPos < 0 {
		return Token{
			Field: util.StringToByteUnsafe(tokenStr),
			Val:   []byte("some_val"),
		}
	}

	return Token{
		Field: util.StringToByteUnsafe(tokenStr[:fieldPos]),
		Val:   util.StringToByteUnsafe(tokenStr[fieldPos+1:]),
	}
}

func Tokens(tokens ...string) []Token {
	r := make([]Token, 0)
	for _, tokenStr := range tokens {
		r = append(r, T(tokenStr))
	}

	return r
}
