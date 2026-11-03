package exec

import (
	"slices"

	"github.com/buger/jsonparser"

	"github.com/ozontech/seq-db/query"
)

type FieldsFilter struct {
	Fields    []string
	AllowList bool
}

type JSONProjector struct {
	input  query.RecordProducer
	col    query.Column[[]byte]
	filter *FieldsFilter

	quoted [][]byte // escaped keys
	paths  [][]string
}

func NewJSONProjector(
	input query.RecordProducer,
	col query.Column[[]byte],
	filter *FieldsFilter,
) *JSONProjector {
	p := &JSONProjector{
		input:  input,
		col:    col,
		filter: filter,
	}

	if filter.AllowList {
		p.paths = make([][]string, len(filter.Fields))
		p.quoted = make([][]byte, len(filter.Fields))
		for i, f := range filter.Fields {
			p.paths[i] = []string{f}
			p.quoted[i] = jsonparser.Escape(f)
		}
	}

	return p
}

func (p *JSONProjector) Next() *query.Record {
	r := p.input.Next()
	if r == nil {
		return nil
	}

	doc := p.col.Val(r)

	var newDoc []byte
	if p.filter.AllowList {
		newDoc = p.projectAllowList(doc)
	} else {
		newDoc = doc
		for _, f := range p.filter.Fields {
			newDoc = jsonparser.Delete(newDoc, f)
		}
	}

	vals := slices.Clone(r.Vals)
	vals[p.col.Idx()] = query.NewJSONVals(newDoc)

	return query.NewRecord(vals)
}

func (p *JSONProjector) Finalize() *query.Summary {
	return p.input.Finalize()
}

func (p *JSONProjector) projectAllowList(doc []byte) []byte {
	buf := make([]byte, 0, len(doc))
	buf = append(buf, '{')
	first := true

	jsonparser.EachKey(doc, func(i int, value []byte, dataType jsonparser.ValueType, _ error) {
		if dataType == jsonparser.Unknown || dataType == jsonparser.NotExist {
			return
		}

		if !first {
			buf = append(buf, ',')
		}
		first = false

		buf = append(buf, p.quoted[i]...)
		buf = append(buf, ':')

		if dataType == jsonparser.String {
			buf = append(buf, '"')
			buf = append(buf, value...)
			buf = append(buf, '"')
		} else {
			buf = append(buf, value...)
		}
	}, p.paths...)

	return append(buf, '}')
}
