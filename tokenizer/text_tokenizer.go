package tokenizer

import (
	"bytes"
	"unicode"
	"unicode/utf8"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/metric"
)

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

func (t *TextTokenizer) Tokenize(tokens []frac.MetaToken, name, value []byte, maxFieldValueLength int) []frac.MetaToken {
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
		tokens = append(tokens, frac.MetaToken{Key: name, Value: value})
		return tokens
	}

	maxLength := min(len(value), maxFieldValueLength)

	metric.SkippedIndexesBytesText.Add(float64(len(value[maxLength:])))
	value = value[:maxLength]
	k := 0

	hasUpper := false
	asciiOnly := true
	// Loop over the string looking for tokens.
	// Token of TextTokenizer is a string that contains only letters, numbers, '*' or '_'.
	for i := 0; i < len(value); {
		c := value[i]
		var runeLength int
		if c < utf8.RuneSelf {
			runeLength = 1
			// Fast path: c is ASCII, check it directly using allTextTokenChars.

			// Save information about uppercase letters to skip ToLower stage.
			hasUpper = hasUpper || upperCaseMap[c]
			if allTextTokenChars[c] {
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
				token = bytes.ToLower(token)
			}
			tokens = append(tokens, frac.MetaToken{Key: name, Value: token})
		}

		hasUpper = false
		asciiOnly = true
	}

	if k == len(value) || len(value[k:]) > t.maxTokenSize {
		return tokens
	}

	token := value[k:]
	if !t.caseSensitive && (asciiOnly && hasUpper || !asciiOnly) {
		token = bytes.ToLower(token)
	}
	tokens = append(tokens, frac.MetaToken{Key: name, Value: token})

	return tokens
}

// allTextTokenChars contains text token symbols that are ASCII.
// 128 bytes is enough for them, but we use 256 to skip bound checks when we use allTextTokenChars[byte(i)].
var allTextTokenChars = [256]bool{
	'0': true, '1': true, '2': true, '3': true, '4': true, '5': true, '6': true, '7': true, '8': true, '9': true,

	'a': true, 'b': true, 'c': true, 'd': true, 'e': true, 'f': true, 'g': true, 'h': true, 'i': true, 'j': true,
	'k': true, 'l': true, 'm': true, 'n': true, 'o': true, 'p': true, 'q': true, 'r': true, 's': true, 't': true,
	'u': true, 'v': true, 'w': true, 'x': true, 'y': true, 'z': true,
	'A': true, 'B': true, 'C': true, 'D': true, 'E': true, 'F': true, 'G': true, 'H': true, 'I': true, 'J': true,
	'K': true, 'L': true, 'M': true, 'N': true, 'O': true, 'P': true, 'Q': true, 'R': true, 'S': true, 'T': true,
	'U': true, 'V': true, 'W': true, 'X': true, 'Y': true, 'Z': true,

	'_': true, '*': true,
}

var upperCaseMap = [256]bool{
	'A': true, 'B': true, 'C': true, 'D': true, 'E': true, 'F': true, 'G': true, 'H': true, 'I': true, 'J': true,
	'K': true, 'L': true, 'M': true, 'N': true, 'O': true, 'P': true, 'Q': true, 'R': true, 'S': true, 'T': true,
	'U': true, 'V': true, 'W': true, 'X': true, 'Y': true, 'Z': true,
}
