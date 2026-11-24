package exec

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/query/encoding"
)

func TestJSONFieldsExtractorExtractsScalars(t *testing.T) {
	input := &testProducer{data: makeExtractorInputRecords([]string{
		`{"service":"svc-1","level":3,"active":true,"absent":null}`,
		`{"service":"svc-2","level":7,"active":false}`,
	})}

	extractor := NewJSONFieldsExtractor(
		input,
		query.JSONColumn(1),
		"service", "level", "active", "absent",
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

func TestJSONFieldsExtractorMissingFieldYieldsEmpty(t *testing.T) {
	input := &testProducer{data: makeExtractorInputRecords([]string{
		`{"service":"svc-1"}`,
	})}

	extractor := NewJSONFieldsExtractor(input, query.JSONColumn(1), "nope")

	r := extractor.Next()
	assert.NotNil(t, r)
	assert.Equal(t, 3, len(r.Vals))
	assert.Equal(t, "", r.Vals[2].AsString())
}

func TestJSONFieldsExtractorObjectAndArrayFields(t *testing.T) {
	input := &testProducer{data: makeExtractorInputRecords([]string{
		`{"service":"svc-1","obj":{"a":1,"b":"x"},"arr":[1,2]}`,
	})}

	extractor := NewJSONFieldsExtractor(input, query.JSONColumn(1), "obj", "arr")

	r := extractor.Next()
	assert.NotNil(t, r)
	assert.JSONEq(t, `{"a":1,"b":"x"}`, r.Vals[2].AsString())
	assert.JSONEq(t, `[1,2]`, r.Vals[3].AsString())
}

func TestJSONFieldsExtractorEscapedStrings(t *testing.T) {
	input := &testProducer{data: makeExtractorInputRecords([]string{
		`{"msg":"line1\nline2","quote":"say \"hi\""}`,
	})}

	extractor := NewJSONFieldsExtractor(input, query.JSONColumn(1), "msg", "quote")

	r := extractor.Next()
	assert.NotNil(t, r)
	assert.Equal(t, "line1\nline2", r.Vals[2].AsString())
	assert.Equal(t, `say "hi"`, r.Vals[3].AsString())
}

func TestJSONFieldsExtractorKeysWithSpecialChars(t *testing.T) {
	input := &testProducer{data: makeExtractorInputRecords([]string{
		`{"http.method":"GET","{a}":1,"a b":" "}`,
	})}

	extractor := NewJSONFieldsExtractor(input, query.JSONColumn(1), "http.method", "{a}", "a b")

	r := extractor.Next()
	assert.NotNil(t, r)
	assert.Equal(t, "GET", r.Vals[2].AsString())
	assert.Equal(t, "1", r.Vals[3].AsString())
	assert.Equal(t, " ", r.Vals[4].AsString())
}

func TestJSONFieldsExtractorNoFields(t *testing.T) {
	docs := []string{`{"service":"svc-1"}`, `{"service":"svc-2"}`}
	input := &testProducer{data: makeExtractorInputRecords(docs)}

	extractor := NewJSONFieldsExtractor(input, query.JSONColumn(1))

	count := 0
	for r := extractor.Next(); r != nil; r = extractor.Next() {
		// only the original two vals, nothing appended
		assert.Equal(t, 2, len(r.Vals))
		count++
	}
	assert.Equal(t, 2, count)
}

func TestJSONFieldsExtractorFinalize(t *testing.T) {
	input := &testProducer{data: makeExtractorInputRecords([]string{
		`{"service":"svc-1"}`,
		`{"service":"svc-2"}`,
	}), total: 2}

	extractor := NewJSONFieldsExtractor(input, query.JSONColumn(1), "service")
	for r := extractor.Next(); r != nil; r = extractor.Next() {
	}

	summary := extractor.Finalize()
	assert.Equal(t, uint64(2), summary.Total)
}

func TestJSONFieldsExtractorPipeline(t *testing.T) {
	// E2E check: a downstream reader uses the extractor's field
	// values via a string column appended right after the original vals.
	const field = "k8s_pod"
	inputData := makeTestInputRecords(10)
	input := &testProducer{data: inputData}

	extractor := NewJSONFieldsExtractor(input, query.JSONColumn(1), field)
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
				query.NewRecordVals(query.DataTypeJSON, []byte(doc)),
			},
		})
	}
	return out
}
