package bulk

import (
	"bytes"
	"slices"
	"unsafe"

	insaneJSON "github.com/vitkovskii/insane-json"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
)

// indexer indexes input values, knows the mapping
// uses specific tokenizers depending on the key value
// keeps a list of extracted tokens
type indexer struct {
	tokenizers map[query.TokenizerType]tokenizer.Tokenizer
	mapping    query.DocStructMapping

	tokensPerMetadata [][]frac.MetaToken
}

func newIndexer(mapping query.DocStructMapping, maxTokenSize int, caseSensitive, partialIndexing bool) *indexer {
	return &indexer{
		mapping: mapping,
		tokenizers: map[query.TokenizerType]tokenizer.Tokenizer{
			query.TokenizerTypeText:        tokenizer.NewTextTokenizer(maxTokenSize, caseSensitive, partialIndexing, consts.MaxTextFieldValueLength),
			query.TokenizerTypeKeyword:     tokenizer.NewKeywordTokenizer(maxTokenSize, caseSensitive, partialIndexing),
			query.TokenizerTypeKeywordList: tokenizer.NewKeywordListTokenizer(maxTokenSize, caseSensitive, partialIndexing, consts.MaxKeywordListFieldValueLength),
			query.TokenizerTypePath:        tokenizer.NewPathTokenizer(maxTokenSize, caseSensitive, partialIndexing),
		},
	}
}

func (i *indexer) Index(node *insaneJSON.Node, id seq.ID, size uint32, appendMeta func(frac.MetaData)) {
	i.tokensPerMetadata = i.tokensPerMetadata[:0]
	i.appendTokensPerMetadata()

	i.decodeInternal(node, nil, 0)

	appendMeta(frac.MetaData{
		ID:     id,
		Size:   size,
		Tokens: i.tokensPerMetadata[0],
	})
	// Special case: nested metadata has zero size to handle nested behavior in the storage.
	const nestedMetadataSize = 0
	for _, tokens := range i.tokensPerMetadata[1:] {
		appendMeta(frac.MetaData{
			ID:     id,
			Size:   nestedMetadataSize,
			Tokens: tokens,
		})
	}
}

var fieldSeparator = []byte(".")

func (i *indexer) decodeInternal(n *insaneJSON.Node, name []byte, metaIndex int) {
	for _, field := range n.AsFields() {
		fieldName := field.AsBytes()

		if len(name) != 0 {
			fieldName = bytes.Join([][]byte{name, fieldName}, fieldSeparator)
		}

		mappingTypes := i.mapping[string(fieldName)]
		mainType := mappingTypes.Main.TokenizerType

		if mainType == query.TokenizerTypeNoop {
			// Field is not in the mapping.
			continue
		}

		if mainType == query.TokenizerTypeObject && field.AsFieldValue().IsObject() {
			i.decodeInternal(field.AsFieldValue(), fieldName, metaIndex)
			continue
		}

		if mainType == query.TokenizerTypeTags && field.AsFieldValue().IsArray() {
			i.decodeTags(field.AsFieldValue(), fieldName, metaIndex)
			continue
		}

		if mainType == query.TokenizerTypeNested && field.AsFieldValue().IsArray() {
			for _, nested := range field.AsFieldValue().AsArray() {
				// Each nested value has its own metadata.
				i.appendTokensPerMetadata()
				nestedTokensIndex := len(i.tokensPerMetadata) - 1

				i.decodeInternal(nested, fieldName, nestedTokensIndex)
			}
			continue
		}

		i.tokensPerMetadata[metaIndex] = i.index(mappingTypes, i.tokensPerMetadata[metaIndex], fieldName, encodeInsaneNode(field.AsFieldValue()))
	}
}

func (i *indexer) index(tokenTypes query.MappingTypes, tokens []frac.MetaToken, key, value []byte) []frac.MetaToken {
	for _, tokenType := range tokenTypes.All {
		if _, has := i.tokenizers[tokenType.TokenizerType]; !has {
			continue
		}

		title := key
		if tokenType.Title != "" {
			title = unsafe.Slice(unsafe.StringData(tokenType.Title), len(tokenType.Title))
		}

		tokens = i.tokenizers[tokenType.TokenizerType].Tokenize(tokens, title, value, tokenType.MaxSize)
		tokens = append(tokens, frac.MetaToken{
			Key:   query.ExistsTokenName,
			Value: title,
		})
	}
	return tokens
}

func (i *indexer) decodeTags(n *insaneJSON.Node, name []byte, tokensIndex int) {
	for _, tag := range n.AsArray() {
		fieldName := tag.Dig("key").AsBytes()
		fieldName = bytes.Join([][]byte{name, fieldName}, fieldSeparator)
		i.tokensPerMetadata[tokensIndex] = i.index(i.mapping[string(fieldName)], i.tokensPerMetadata[tokensIndex], fieldName, encodeInsaneNode(tag.Dig("value")))
	}
}

// appendTokensPerMetadata increases tokensPerMetadata size by 1 and reuses the underlying slices capacity.
func (i *indexer) appendTokensPerMetadata() {
	n := len(i.tokensPerMetadata)
	// Unlike append(), slices.Grow() copies the underlying slices if any.
	i.tokensPerMetadata = slices.Grow(i.tokensPerMetadata, 1)[:n+1]
	// Reuse the underlying slice capacity.
	i.tokensPerMetadata[n] = i.tokensPerMetadata[n][:0]

	i.tokensPerMetadata[n] = append(i.tokensPerMetadata[n], frac.MetaToken{
		Key:   query.AllTokenName,
		Value: []byte{},
	})
}

func encodeInsaneNode(field *insaneJSON.Node) []byte {
	if field.IsArray() || field.IsObject() {
		return field.Encode(nil)
	}
	return field.AsBytes()
}
