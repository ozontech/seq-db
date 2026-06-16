package exec

import (
	"slices"

	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/query"
)

type DocProjector struct {
	input      query.RecordProducer
	colIdx     int
	filter     *storeapi.FieldsFilter
	decoderBuf []byte
}

func NewDocProjector(
	input query.RecordProducer,
	colIdx int,
	filter *storeapi.FieldsFilter,
) *DocProjector {
	return &DocProjector{
		input:  input,
		colIdx: colIdx,
		filter: filter,
	}
}

func (p *DocProjector) Next() (*query.Record, *query.Metadata) {
	r, meta := p.input.Next()
	if meta != nil {
		return nil, meta
	}
	if r == nil {
		return nil, nil
	}

	decoder := r.Vals[p.colIdx].Decoded().(*insaneJSON.Root)

	if !p.filter.AllowList {
		// It is block list, so remove given fields from document.
		for _, field := range p.filter.Fields {
			decoder.Dig(field).Suicide()
		}
		return p.makeRecordWithNewVals(r, decoder.Encode(p.decoderBuf[:0])), nil
	}

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
	return p.makeRecordWithNewVals(r, decoder.Encode(p.decoderBuf[:0])), nil
}

func (p *DocProjector) Release() {
	// TODO: release insane json objects (???)
	p.input.Release()
}

func (p *DocProjector) makeRecordWithNewVals(old *query.Record, newRawData []byte) *query.Record {
	newRecordVals := make([]*query.RecordVals, len(old.Vals))
	for i := range len(old.Vals) {
		rawData := old.Vals[i].RawData()
		if i == p.colIdx {
			rawData = newRawData
		}
		newRecordVals[i] = query.NewRecordVals(old.Vals[i].Type, rawData)
	}
	return query.NewRecord(newRecordVals)
}
