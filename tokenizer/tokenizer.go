package tokenizer

import (
	"strings"
	"unsafe"

	"github.com/ozontech/seq-db/frac"
)

type Tokenizer interface {
	Tokenize(tokens []frac.MetaToken, key, value []byte, maxLength int) []frac.MetaToken
}

func toLowerIfCaseInsensitive(isCaseSensitive bool, x []byte) []byte {
	if !isCaseSensitive {
		// Use strings.ToLower instead of bytes.ToLower to avoid allocating a new slice in case x is already in lower case.
		lowered := strings.ToLower(unsafe.String(unsafe.SliceData(x), len(x)))
		return unsafe.Slice(unsafe.StringData(lowered), len(lowered))
	}
	return x
}
