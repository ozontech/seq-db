package tokenizer

/*
#cgo CFLAGS: -O3 -msse3 -g -Wall -Wextra
#include "tokenize.h"
*/
import "C"

import (
	"sync"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/ozontech/seq-db/metric"
)

var spanBufPool = sync.Pool{
	New: func() any {
		buf := make([]C.span, 64)
		return &buf
	},
}

type TextTokenizer struct {
	maxTokenSize               int
	caseSensitive              bool
	partialIndexing            bool
	defaultMaxFieldValueLength int
}

func NewTextTokenizer(maxTokenSize int, caseSensitive, partialIndexing bool, maxFieldValueLength int) *TextTokenizer {
	return &TextTokenizer{
		maxTokenSize:               maxTokenSize,
		caseSensitive:              caseSensitive,
		defaultMaxFieldValueLength: maxFieldValueLength,
		partialIndexing:            partialIndexing,
	}
}

func (t *TextTokenizer) Tokenize(tokens []MetaToken, name, value []byte, maxFieldValueLength int) []MetaToken {
	metric.TokenizerIncomingTextLen.Observe(float64(len(value)))

	if maxFieldValueLength == 0 {
		maxFieldValueLength = t.defaultMaxFieldValueLength
	}

	if len(value) > maxFieldValueLength && !t.partialIndexing {
		metric.SkippedIndexesText.Inc()
		metric.SkippedIndexesBytesText.Add(float64(len(value)))
		return tokens
	}

	if len(value) == 0 {
		tokens = append(tokens, MetaToken{Key: name, Value: value})
		return tokens
	}

	maxLength := min(len(value), maxFieldValueLength)

	metric.SkippedIndexesBytesText.Add(float64(len(value[maxLength:])))
	value = value[:maxLength]
	k := 0

	bufp := spanBufPool.Get().(*[]C.span)
	spans, ok := tokenize(value, *bufp)

	if ok {
		for _, s := range spans {
			start, length := uint32(s.start), uint32(s.len)
			token := value[start : start+length]
			tokens = append(tokens, MetaToken{Key: name, Value: token})
		}
		*bufp = spans[:cap(spans)]
		spanBufPool.Put(bufp)
		return tokens
	}

	panic("unreachable")

	hasUpper := false
	asciiOnly := true
	// Loop over the string looking for tokens.
	// Token of TextTokenizer is a string that contains only letters, numbers, '*' or '_'.
	for i := 0; i < len(value); {
		c := value[i]
		var runeLength int
		if c < utf8.RuneSelf {
			runeLength = 1
			// Fast path: c is ASCII, check it directly using isTextToken.

			// Save information about uppercase letters to skip ToLower stage.
			hasUpper = hasUpper || isUpperASCII[c]
			if isTextToken[c] {
				i++
				continue
			}
		} else {
			// Slow path: c is utf8, decode it.
			asciiOnly = false
			var r rune
			r, runeLength = utf8.DecodeRune(value[i:])
			if unicode.IsLetter(r) || unicode.IsNumber(r) {
				i += runeLength
				continue
			}
		}

		token := value[k:i]
		i += runeLength
		k = i

		if len(token) != 0 && len(token) <= t.maxTokenSize {
			if !t.caseSensitive && (!asciiOnly || hasUpper) {
				// We can skip the ToLower call if we are sure that there are only ASCII characters and no uppercase letters.
				token = toLowerTryInplace(token)
			}
			tokens = append(tokens, MetaToken{Key: name, Value: token})
		}

		hasUpper = false
		asciiOnly = true
	}

	if k == len(value) || len(value[k:]) > t.maxTokenSize {
		return tokens
	}

	token := value[k:]
	if !t.caseSensitive && (asciiOnly && hasUpper || !asciiOnly) {
		token = toLowerTryInplace(token)
	}
	tokens = append(tokens, MetaToken{Key: name, Value: token})

	return tokens
}

func asciiOnly(s []byte) bool {
	return int32(C.asciionly(
		(*C.char)(unsafe.Pointer(unsafe.SliceData(s))),
		C.size_t(len(s)),
	)) == 1
}

func tokenize(text []byte, buf []C.span) ([]C.span, bool) {
	if len(text) == 0 {
		return buf[:0], true
	}

	required := len(text)/2 + 1
	if cap(buf) < required {
		buf = make([]C.span, required)
	} else {
		buf = buf[:required]
	}

	n := C.tokenize(
		(*C.char)(unsafe.Pointer(&text[0])),
		C.size_t(len(text)),
		&buf[0],
		+C.int(required),
	)

	if n < 0 {
		return nil, false
	}

	return buf[:n], true
}
