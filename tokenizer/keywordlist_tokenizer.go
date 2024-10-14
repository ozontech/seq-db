package tokenizer

import (
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/metric"
)

type KeywordListTokenizer struct {
	maxTokenSize               int
	caseSensitive              bool
	partialIndexing            bool
	defaultMaxFieldValueLength int
}

func NewKeywordListTokenizer(maxTokenSize int, caseSensitive, partialIndexing bool, maxFieldValueLength int) *KeywordListTokenizer {
	return &KeywordListTokenizer{
		maxTokenSize:               maxTokenSize,
		caseSensitive:              caseSensitive,
		defaultMaxFieldValueLength: maxFieldValueLength,
		partialIndexing:            partialIndexing,
	}
}

func (t *KeywordListTokenizer) Tokenize(tokens []frac.MetaToken, name, value []byte, maxFieldValueLength int) []frac.MetaToken {
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
		tokens = append(tokens, frac.MetaToken{Key: name, Value: []byte{}})
		return tokens
	}

	maxLength := min(len(value), maxFieldValueLength)

	metric.SkippedIndexesBytesText.Add(float64(len(value[maxLength:])))
	value = value[:maxLength]
	k := 0

	for i := 0; i < len(value); i++ {
		c := value[i]
		if c != ' ' {
			continue
		}
		x := i - k

		if x > 0 && x <= t.maxTokenSize {
			tokens = append(tokens, frac.MetaToken{Key: name, Value: toLowerIfCaseInsensitive(t.caseSensitive, value[k:i])})
		}
		k = i + 1
	}

	if k == len(value) || len(value[k:]) > t.maxTokenSize {
		return tokens
	}

	tokens = append(tokens, frac.MetaToken{Key: name, Value: toLowerIfCaseInsensitive(t.caseSensitive, value[k:])})
	return tokens
}
