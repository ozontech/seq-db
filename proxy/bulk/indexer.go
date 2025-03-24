package bulk

import (
	"bytes"
	"slices"
	"unsafe"

	"github.com/go-faster/jx"

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
func (i *indexer) Index(d *jx.Decoder, id seq.ID, size uint32) {
	// Reset previous state.
	i.metas = i.metas[:0]

	i.appendMeta(id, size)
	i.decodeObj(d, nil, 0)

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

// rawField returns cropped field, in case of strings
// using default d.Raw() will cause returning string value as a quoted string e.g. `"somevalue"`
func rawField(d *jx.Decoder) ([]byte, error) {
	switch d.Next() {
	case jx.String:
		return d.StrBytes()
	default:
		return d.Raw()
	}
}

func (i *indexer) decodeObj(d *jx.Decoder, prevName []byte, metaIndex int) {
	_ = d.ObjBytes(func(d *jx.Decoder, fieldName []byte) error {
		// first level of nesting
		if prevName != nil {
			fieldName = bytes.Join([][]byte{prevName, fieldName}, fieldSeparator)
		}

		var mappingTypes seq.MappingTypes

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

		switch mappingTypes.Main.TokenizerType {
		// Field is not in the mapping.
		case seq.TokenizerTypeNoop:
			return d.Skip()

		case seq.TokenizerTypeObject:
			if d.Next() == jx.Object {
				i.decodeObj(d, fieldName, metaIndex)
				return nil
			}

		case seq.TokenizerTypeTags:
			if d.Next() == jx.Array {
				i.decodeTags(d, fieldName, metaIndex)
				return nil
			}

		case seq.TokenizerTypeNested:
			if d.Next() == jx.Array {
				_ = d.Arr(func(d *jx.Decoder) error {
					i.appendNestedMeta()
					nestedMetaIndex := len(i.metas) - 1

					i.decodeObj(d, fieldName, nestedMetaIndex)
					return nil
				})
				return nil
			}
		}

		fieldValue, err := rawField(d)
		// in case of error, do not index field
		if err != nil {
			return err
		}

		i.metas[metaIndex].Tokens = i.index(mappingTypes, i.metas[metaIndex].Tokens, fieldName, fieldValue)
		return nil
	})
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

func (i *indexer) decodeTags(d *jx.Decoder, name []byte, tokensIndex int) {
	_ = d.Arr(func(d *jx.Decoder) error {
		var fieldName, fieldValue []byte
		err := d.Obj(func(d *jx.Decoder, objKey string) error {
			switch objKey {
			case "key":
				v, err := rawField(d)
				if err != nil {
					return err
				}
				fieldName = v

			case "value":
				v, err := rawField(d)
				if err != nil {
					return err
				}
				fieldValue = v

			default:
				return d.Skip()
			}

			return nil
		})
		// ignoring data in case of parsing error
		if err != nil {
			return err
		}

		fieldName = bytes.Join([][]byte{name, fieldName}, fieldSeparator)
		i.metas[tokensIndex].Tokens = i.index(i.mapping[string(fieldName)], i.metas[tokensIndex].Tokens, fieldName, fieldValue)

		return nil
	})
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
