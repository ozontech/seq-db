package exec

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/query/encoding"
)

func TestDocFieldsExtractorExtractsScalars(t *testing.T) {
	input := &testProducer{data: makeExtractorInputRecords([]string{
		`{"service":"svc-1","level":3,"active":true,"absent":null}`,
		`{"service":"svc-2","level":7,"active":false}`,
	})}

	extractor := NewDocFieldsExtractor(
		input,
		query.DocColumn(1),
		[]string{"service", "level", "active", "absent"},
	)

	r1 := extractor.Next()
	assert.NotNil(t, r1)
	// original vals are untouched, extracted fields are appended in order
	assert.Equal(t, 6, len(r1.Vals))
	assert.Equal(t, "svc-1", r1.Vals[2].AsString())
	assert.Equal(t, "3", r1.Vals[3].AsString())
	assert.Equal(t, "true", r1.Vals[4].AsString())
	assert.Equal(t, "null", r1.Vals[5].AsString())

	r2 := extractor.Next()
	assert.NotNil(t, r2)
	assert.Equal(t, 6, len(r2.Vals))
	assert.Equal(t, "svc-2", r2.Vals[2].AsString())
	assert.Equal(t, "7", r2.Vals[3].AsString())
	assert.Equal(t, "false", r2.Vals[4].AsString())
	assert.Equal(t, "", r2.Vals[5].AsString()) // absent field is missing

	assert.Nil(t, extractor.Next())
}

func TestDocFieldsExtractorMissingFieldYieldsEmpty(t *testing.T) {
	input := &testProducer{data: makeExtractorInputRecords([]string{
		`{"service":"svc-1"}`,
	})}

	extractor := NewDocFieldsExtractor(input, query.DocColumn(1), []string{"nope"})

	r := extractor.Next()
	assert.NotNil(t, r)
	assert.Equal(t, 3, len(r.Vals))
	assert.Equal(t, "", r.Vals[2].AsString())
}

func TestDocFieldsExtractorObjectAndArrayFields(t *testing.T) {
	// This case documents the scalar-only contract: object and array fields yield an
	// empty string, not their JSON representation.
	input := &testProducer{data: makeExtractorInputRecords([]string{
		`{"service":"svc-1","obj":{"a":1},"arr":[1,2]}`,
	})}

	extractor := NewDocFieldsExtractor(input, query.DocColumn(1), []string{"obj", "arr"})

	r := extractor.Next()
	assert.NotNil(t, r)
	assert.Equal(t, "", r.Vals[2].AsString())
	assert.Equal(t, "", r.Vals[3].AsString())
}

func TestDocFieldsExtractorNoFields(t *testing.T) {
	docs := []string{`{"service":"svc-1"}`, `{"service":"svc-2"}`}
	input := &testProducer{data: makeExtractorInputRecords(docs)}

	extractor := NewDocFieldsExtractor(input, query.DocColumn(1), nil)

	count := 0
	for r := extractor.Next(); r != nil; r = extractor.Next() {
		// only the original two vals, nothing appended
		assert.Equal(t, 2, len(r.Vals))
		count++
	}
	assert.Equal(t, 2, count)
}

func TestDocFieldsExtractorFinalize(t *testing.T) {
	input := &testProducer{data: makeExtractorInputRecords([]string{
		`{"service":"svc-1"}`,
		`{"service":"svc-2"}`,
	}), total: 2}

	extractor := NewDocFieldsExtractor(input, query.DocColumn(1), []string{"service"})
	for r := extractor.Next(); r != nil; r = extractor.Next() {
	}

	summary := extractor.Finalize()
	assert.Equal(t, uint64(2), summary.Total)
	assert.Nil(t, extractor.roots)
}

func TestDocFieldsExtractorPipeline(t *testing.T) {
	// E2E check: a downstream reader uses the extractor's field
	// values via a string column appended right after the original vals.
	const field = "k8s_pod"
	inputData := makeTestInputRecords(10)
	input := &testProducer{data: inputData}

	extractor := NewDocFieldsExtractor(input, query.DocColumn(1), []string{field})
	podCol := query.StringColumn(2)

	for r := extractor.Next(); r != nil; r = extractor.Next() {
		want := fmt.Sprintf("pod-%d", r.Vals[0].AsUint32())
		assert.Equal(t, want, podCol.Val(r))
	}
}

func makeExtractorInputRecords(docs []string) []*query.Record {
	out := make([]*query.Record, 0, len(docs))
	for _, doc := range docs {
		out = append(out, &query.Record{
			Vals: []*query.RecordVals{
				query.NewRecordVals(query.DataTypeUint64, encoding.Uint64ToBytes(0)),
				query.NewRecordVals(query.DataTypeDocument, []byte(doc)),
			},
		})
	}
	return out
}
