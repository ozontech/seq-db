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
	input      query.RecordProducer
	colIdx     int
	filter     *FieldsFilter
	decoderBuf []byte

	// roots holds the input records whose decoded document root has been
	// mutated during projection. The mutated root lives only inside the input
	// record (the output record carries freshly encoded raw bytes with
	// decoded=nil), so the projector is the last owner and must release these
	// roots in Finalize.
	roots []*query.Record
}

func NewDocProjector(
	input query.RecordProducer,
	colIdx int,
	filter *FieldsFilter,
) *DocProjector {
	return &DocProjector{
		input:  input,
		colIdx: colIdx,
		filter: filter,
	}
}

func (p *DocProjector) Next() *query.Record {
	r := p.input.Next()
	if r == nil {
		return nil
	}

	decoder := r.Vals[p.colIdx].Decoded().(*insaneJSON.Root)

	var newRecord *query.Record
	if !p.filter.AllowList {
		// It is block list, so remove given fields from document.
		for _, field := range p.filter.Fields {
			decoder.Dig(field).Suicide()
		}
		newRecord = p.makeRecordWithNewVals(r, decoder.Encode(p.decoderBuf[:0]))
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
		newRecord = p.makeRecordWithNewVals(r, decoder.Encode(p.decoderBuf[:0]))
	}

	// The input record holds the mutated root and won't be seen downstream;
	// keep it for release in Finalize.
	p.roots = append(p.roots, r)
	return newRecord
}

func (p *DocProjector) Finalize() *query.Summary {
	for _, r := range p.roots {
		r.Release()
	}
	return p.input.Finalize()
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
