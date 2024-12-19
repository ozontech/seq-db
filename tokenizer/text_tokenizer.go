package tokenizer

import (
	"unicode"
	"unicode/utf8"

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
