package tokenizer

import (
	"bytes"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/metric"
)

const defaultSeparator = '/'

type PathTokenizer struct {
	separator           byte
	defaultMaxTokenSize int
	caseSensitive       bool
	partialIndexing     bool
}

func NewPathTokenizer(
	maxTokenSize int,
	caseSensitive bool,
	partialIndexing bool,
) *PathTokenizer {
	return &PathTokenizer{
		separator:           defaultSeparator,
		defaultMaxTokenSize: maxTokenSize,
		caseSensitive:       caseSensitive,
		partialIndexing:     partialIndexing,
	}
}

func (t *PathTokenizer) Tokenize(tokens []frac.MetaToken, name, value []byte, maxTokenSize int) []frac.MetaToken {
	if maxTokenSize == 0 {
		maxTokenSize = t.defaultMaxTokenSize
	}

	if len(value) > maxTokenSize && !t.partialIndexing {
		metric.SkippedIndexesPath.Inc()
		metric.SkippedIndexesBytesPath.Add(float64(len(value)))
		return tokens
	}

	maxLength := min(len(value), maxTokenSize)
	metric.SkippedIndexesBytesPath.Add(float64(len(value[maxLength:])))
	value = value[:maxLength]

	var i int

	if len(value) != 0 && value[0] == t.separator {
		i++
	}

	for ; i < len(value); i++ {
		sepIndex := bytes.IndexByte(value[i:], t.separator)
		if sepIndex == -1 {
			break
		}
		i += sepIndex

		tokens = append(tokens, frac.MetaToken{
			Key:   name,
			Value: toLowerIfCaseInsensitive(t.caseSensitive, value[:i]),
		})
	}

	tokens = append(tokens, frac.MetaToken{
		Key:   name,
		Value: toLowerIfCaseInsensitive(t.caseSensitive, value),
	})
	return tokens
}
