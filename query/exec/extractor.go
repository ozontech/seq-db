package exec

import (
	"slices"

	"github.com/buger/jsonparser"

	"github.com/ozontech/seq-db/query"
)

var nullFieldValue = []byte("null")

type JSONFieldsExtractor struct {
	input query.RecordProducer
	col   query.Column[[]byte]

	extractFields []string
	paths         [][]string
}

func NewJSONFieldsExtractor(
	input query.RecordProducer,
	col query.Column[[]byte],
	extractFields ...string,
) *JSONFieldsExtractor {
	paths := make([][]string, len(extractFields))
	for i, f := range extractFields {
		paths[i] = []string{f}
	}
	return &JSONFieldsExtractor{
		input:         input,
		col:           col,
		extractFields: extractFields,
		paths:         paths,
	}
}

func (e *JSONFieldsExtractor) Next() *query.Record {
	r := e.input.Next()
	if r == nil {
		return nil
	}

	doc := e.col.Val(r)
	base := len(r.Vals)
	r.Vals = slices.Grow(r.Vals, len(e.extractFields))

	// pre fill vals in case of not found paths
	for range e.extractFields {
		// now treat everything as strings, but later will support other types as well
		r.Vals = append(r.Vals, query.NewRecordVals(query.DataTypeString, nil))
	}

	jsonparser.EachKey(doc, func(i int, value []byte, dataType jsonparser.ValueType, _ error) {
		if i >= 0 {
			r.Vals[base+i] = query.NewRecordVals(query.DataTypeString, jsonFieldValue(value, dataType))
		}
	}, e.paths...)

	return r
}

func (e *JSONFieldsExtractor) Finalize() *query.Summary {
	return e.input.Finalize()
}

func jsonFieldValue(value []byte, dataType jsonparser.ValueType) []byte {
	switch dataType {
	case jsonparser.String:
		unescaped, err := jsonparser.Unescape(value, nil)
		if err != nil {
			return nil
		}
		return unescaped
	case jsonparser.Null:
		return nullFieldValue
	default:
		return value
	}
}
