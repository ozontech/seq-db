package bulk

import (
	"bytes"
	"slices"
	"unsafe"

	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
)

// indexer indexes input values, knows the mapping
// uses specific tokenizers depending on the key value
// keeps a list of extracted tokens
type indexer struct {
	tokenizers map[seq.TokenizerType]tokenizer.Tokenizer
	mapping    seq.Mapping

	metas []frac.MetaData
}

// Index returns a list of metadata of the given json node.
// Return at least one metadata. May return more if there are nested fields in the mapping.
// Each metadata has special token _all_, we use to find stored documents.
// Each indexed field has special token _exists_,
// we use it to find documents that have this field in search and aggregate requests.
//
// Careful: any reference to i.metas will be invalidated after the call.
func (i *indexer) Index(node *insaneJSON.Node, id seq.ID, size uint32) {
	// Reset previous state.
	i.metas = i.metas[:0]

	i.appendMeta(id, size)

	i.decodeInternal(node, id, nil, 0)

	m := i.metas
	parent := m[0]
	for j := 1; j < len(m); j++ {
		// Copy tokens from the parent, except of the _all_ token.
		m[j].Tokens = append(m[j].Tokens, parent.Tokens[1:]...)
	}
}

func (i *indexer) Metas() []frac.MetaData {
	return i.metas
}

var fieldSeparator = []byte(".")

func (i *indexer) decodeInternal(n *insaneJSON.Node, id seq.ID, name []byte, metaIndex int) {
	for _, field := range n.AsFields() {
		fieldName := field.AsBytes()

		if len(name) != 0 {
			fieldName = bytes.Join([][]byte{name, fieldName}, fieldSeparator)
		}

		var (
			mappingTypes seq.MappingTypes
			mainType     seq.TokenizerType
		)

		switch i.mapping {
		case nil:
			mappingType := seq.MappingType{
				Title:         string(fieldName),
				TokenizerType: seq.TokenizerTypeKeyword,
			}
			mappingTypes = seq.MappingTypes{
				Main: mappingType,
				All:  []seq.MappingType{mappingType},
			}
		default:
			mappingTypes = i.mapping[string(fieldName)]
		}

		mainType = mappingTypes.Main.TokenizerType
		if mainType == seq.TokenizerTypeNoop {
			// Field is not in the mapping.
			continue
		}

		if mainType == seq.TokenizerTypeObject && field.AsFieldValue().IsObject() {
			i.decodeInternal(field.AsFieldValue(), id, fieldName, metaIndex)
			continue
		}

		if mainType == seq.TokenizerTypeTags && field.AsFieldValue().IsArray() {
			i.decodeTags(field.AsFieldValue(), fieldName, metaIndex)
			continue
		}

		if mainType == seq.TokenizerTypeNested && field.AsFieldValue().IsArray() {
			for _, nested := range field.AsFieldValue().AsArray() {
				i.appendNestedMeta()
				nestedMetaIndex := len(i.metas) - 1

				i.decodeInternal(nested, id, fieldName, nestedMetaIndex)
			}
			continue
		}

		nodeValue := encodeInsaneNode(field.AsFieldValue())
		i.metas[metaIndex].Tokens = i.index(mappingTypes, i.metas[metaIndex].Tokens, fieldName, nodeValue)
	}
}

func (i *indexer) index(tokenTypes seq.MappingTypes, tokens []frac.MetaToken, key, value []byte) []frac.MetaToken {
	for _, tokenType := range tokenTypes.All {
		if _, has := i.tokenizers[tokenType.TokenizerType]; !has {
			continue
		}

		title := key
		if tokenType.Title != "" {
			title = unsafe.Slice(unsafe.StringData(tokenType.Title), len(tokenType.Title))
		}

		// value can be nil (not the same as empty) in case of tags indexer type,
		// so don't tokenize it.
		if value != nil {
			tokens = i.tokenizers[tokenType.TokenizerType].Tokenize(tokens, title, value, tokenType.MaxSize)
		}
		tokens = append(tokens, frac.MetaToken{
			Key:   seq.ExistsTokenName,
			Value: title,
		})
	}
	return tokens
}

func (i *indexer) decodeTags(n *insaneJSON.Node, name []byte, tokensIndex int) {
	for _, tag := range n.AsArray() {
		fieldName := tag.Dig("key").AsBytes()
		fieldName = bytes.Join([][]byte{name, fieldName}, fieldSeparator)
		nodeValue := encodeInsaneNode(tag.Dig("value"))
		i.metas[tokensIndex].Tokens = i.index(i.mapping[string(fieldName)], i.metas[tokensIndex].Tokens, fieldName, nodeValue)
	}
}

// appendMeta increases metas size by 1 and reuses the underlying slices capacity.
func (i *indexer) appendMeta(id seq.ID, size uint32) {
	n := len(i.metas)
	// Unlike append(), slices.Grow() copies the underlying slices if any.
	i.metas = slices.Grow(i.metas, 1)[:n+1]
	// Reuse the underlying slice capacity.
	i.metas[n].Tokens = i.metas[n].Tokens[:0]

	i.metas[n].ID = id
	i.metas[n].Size = size

	i.metas[n].Tokens = append(i.metas[n].Tokens, frac.MetaToken{
		Key:   seq.AllTokenName,
		Value: []byte{},
	})
}

func (i *indexer) appendNestedMeta() {
	parent := i.metas[0]
	// Special case: nested metadata has zero size to handle nested behavior in the storage.
	const nestedMetadataSize = 0
	i.appendMeta(parent.ID, nestedMetadataSize)
}

func encodeInsaneNode(field *insaneJSON.Node) []byte {
	if field.IsNil() {
		return nil
	}
	if field.IsArray() || field.IsObject() || field.IsNull() || field.IsTrue() || field.IsFalse() {
		return field.Encode(nil)
	}
	return field.AsBytes()
}
