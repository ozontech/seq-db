package tokenizer

import (
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/metric"
)

type KeywordTokenizer struct {
	defaultMaxTokenSize int
	caseSensitive       bool
	partialIndexing     bool
}

func NewKeywordTokenizer(maxTokenSize int, caseSensitive, partialIndexing bool) *KeywordTokenizer {
	return &KeywordTokenizer{
		defaultMaxTokenSize: maxTokenSize,
		caseSensitive:       caseSensitive,
		partialIndexing:     partialIndexing,
	}
}

func (t *KeywordTokenizer) Tokenize(tokens []frac.MetaToken, name, value []byte, maxTokenSize int) []frac.MetaToken {
	if maxTokenSize == 0 {
		maxTokenSize = t.defaultMaxTokenSize
	}

	if len(value) > maxTokenSize && !t.partialIndexing {
		metric.SkippedIndexesKeyword.Inc()
		metric.SkippedIndexesBytesKeyword.Add(float64(len(value)))
		return tokens
	}

	maxLength := min(len(value), maxTokenSize)
	metric.SkippedIndexesBytesKeyword.Add(float64(len(value[maxLength:])))
	value = value[:maxLength]

	tokens = append(tokens, frac.MetaToken{
		Key:   name,
		Value: toLowerIfCaseInsensitive(t.caseSensitive, value),
	})
	return tokens
}
