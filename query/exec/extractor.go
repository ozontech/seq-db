package exec

import (
	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/query"
)

type DocFieldsExtractor struct {
	input  query.RecordProducer
	docCol query.Column[*insaneJSON.Root]

	// extractFields lists scalar JSON fields to extract out of the document column as string values.
	extractFields []string

	// roots holds insaneJSON.Root for every record whose document val has been decoded
	roots []*query.Record
}

func NewDocFieldsExtractor(
	input query.RecordProducer,
	docCol query.Column[*insaneJSON.Root],
	extractFields []string,
) *DocFieldsExtractor {
	return &DocFieldsExtractor{
		input:         input,
		docCol:        docCol,
		extractFields: extractFields,
	}
}

func (e *DocFieldsExtractor) Next() *query.Record {
	r := e.input.Next()
	if r == nil {
		return nil
	}

	root := e.docCol.Val(r)
	for _, field := range e.extractFields {
		// now we treat everything as strings, but later will support other types as well
		fieldVal := root.Dig(field).AsBytes()
		r.Vals = append(r.Vals, query.NewRecordVals(query.DataTypeString, fieldVal))
	}
	e.roots = append(e.roots, r)

	return r
}

func (e *DocFieldsExtractor) Finalize() *query.Summary {
	for _, r := range e.roots {
		r.Release()
	}
	e.roots = nil
	return e.input.Finalize()
}
