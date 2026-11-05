package exec

import (
	"slices"

	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/query"
)

type FieldsFilter struct {
	Fields    []string
	AllowList bool
}

type DocProjector struct {
	input  query.RecordProducer
	col    query.Column[*insaneJSON.Root]
	filter *FieldsFilter

	// roots holds every record whose doc val has been decoded (spawned an insaneJSON root).
	// They are released back to the library pool in Finalize.
	roots []*query.Record
}

func NewDocProjector(
	input query.RecordProducer,
	col query.Column[*insaneJSON.Root],
	filter *FieldsFilter,
) *DocProjector {
	return &DocProjector{
		input:  input,
		col:    col,
		filter: filter,
	}
}

func (p *DocProjector) Next() *query.Record {
	r := p.input.Next()
	if r == nil {
		return nil
	}

	decoder := p.col.Val(r)

	var newRecord *query.Record
	if !p.filter.AllowList {
		// It is block list, so remove given fields from document.
		for _, field := range p.filter.Fields {
			decoder.Dig(field).Suicide()
		}
		newRecord = p.makeRecordWithNewVals(r, decoder.Encode(nil))
	} else {
		// Keep only given fields.
		// fieldsToRemove contains fields that should be removed.
		// It is complex to do it in-place because decoder.Suicide makes decoder.AsFields() invalid.
		var fieldsToRemove []*insaneJSON.Node
		for _, field := range decoder.AsFields() {
			fieldName := field.AsString()
			if !slices.Contains(p.filter.Fields, fieldName) {
				fieldsToRemove = append(fieldsToRemove, field.AsFieldValue())
			}
		}
		for _, field := range fieldsToRemove {
			field.Suicide()
		}
		newRecord = p.makeRecordWithNewVals(r, decoder.Encode(nil))
	}

	p.roots = append(p.roots, r)

	return newRecord
}

func (p *DocProjector) Finalize() *query.Summary {
	for _, r := range p.roots {
		r.Release()
	}
	p.roots = nil
	return p.input.Finalize()
}

func (p *DocProjector) makeRecordWithNewVals(old *query.Record, newRawData []byte) *query.Record {
	newRecordVals := make([]*query.RecordVals, len(old.Vals))
	for i := range len(old.Vals) {
		rawData := old.Vals[i].RawData()
		if i == p.col.Idx() {
			rawData = newRawData
		}
		newRecordVals[i] = query.NewRecordVals(old.Vals[i].Type, rawData)
	}
	return query.NewRecord(newRecordVals)
}
