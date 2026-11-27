package exec

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/query"
)

func TestJSONProjectorFields(t *testing.T) {
	testJSONProjector(
		t,
		&FieldsFilter{Fields: []string{"service", "level"}, AllowList: true},
		[]string{
			`{"service":"service-0","level":3}`,
			`{"service":"service-1","level":3}`,
		},
	)
}

func TestProjectorFieldsExcept(t *testing.T) {
	testJSONProjector(
		t,
		&FieldsFilter{Fields: []string{"level"}, AllowList: false},
		[]string{
			`{"service":"service-0","k8s_pod":"pod-0"}`,
			`{"service":"service-1","k8s_pod":"pod-1"}`,
		},
	)
}

func TestJSONProjectorAllowListEscapedAndMissingFields(t *testing.T) {
	input := &testProducer{data: []*query.Record{
		makeDocRecord(`{"msg":"line1\nline2","service":"svc-1"}`),
		makeDocRecord(`{"service":"svc-2"}`),
	}}

	projector := NewJSONProjector(input, query.JSONColumn(1), &FieldsFilter{
		Fields:    []string{"service", "msg"},
		AllowList: true,
	})

	r1 := projector.Next()
	assert.NotNil(t, r1)
	assert.JSONEq(t, `{"service":"svc-1","msg":"line1\nline2"}`, string(r1.Vals[1].RawData()))

	r2 := projector.Next()
	assert.NotNil(t, r2)
	assert.JSONEq(t, `{"service":"svc-2"}`, string(r2.Vals[1].RawData()))
}

func TestJSONProjectorAllowListEmpty(t *testing.T) {
	input := &testProducer{data: []*query.Record{makeDocRecord(`{"service":"svc-1"}`)}}

	projector := NewJSONProjector(input, query.JSONColumn(1), &FieldsFilter{AllowList: true})

	r := projector.Next()
	assert.NotNil(t, r)
	assert.Equal(t, `{}`, string(r.Vals[1].RawData()))
}

func TestJSONProjectorObjectField(t *testing.T) {
	input := &testProducer{data: []*query.Record{makeDocRecord(`{"service":"svc-1","obj":{"a":1},"level":3}`)}}

	projector := NewJSONProjector(input, query.JSONColumn(1), &FieldsFilter{
		Fields:    []string{"obj"},
		AllowList: true,
	})

	r := projector.Next()
	assert.NotNil(t, r)
	assert.JSONEq(t, `{"obj":{"a":1}}`, string(r.Vals[1].RawData()))
}

func TestJSONProjectorExceptObjectField(t *testing.T) {
	input := &testProducer{data: []*query.Record{makeDocRecord(`{"service":"svc-1","obj":{"a":1},"level":3}`)}}

	projector := NewJSONProjector(input, query.JSONColumn(1), &FieldsFilter{
		Fields:    []string{"obj"},
		AllowList: false,
	})

	r := projector.Next()
	assert.NotNil(t, r)
	assert.JSONEq(t, `{"service":"svc-1","level":3}`, string(r.Vals[1].RawData()))
}

func TestJSONProjectorExceptAbsentField(t *testing.T) {
	input := &testProducer{data: []*query.Record{makeDocRecord(`{"service":"svc-1"}`)}}

	projector := NewJSONProjector(input, query.JSONColumn(1), &FieldsFilter{
		Fields:    []string{"nope"},
		AllowList: false,
	})

	r := projector.Next()
	assert.NotNil(t, r)
	assert.JSONEq(t, `{"service":"svc-1"}`, string(r.Vals[1].RawData()))
}

func makeDocRecord(doc string) *query.Record {
	return &query.Record{
		Vals: []*query.RecordVals{
			query.NewUint32Vals(0),
			query.NewJSONVals([]byte(doc)),
		},
	}
}

func testJSONProjector(t *testing.T, fieldsFilter *FieldsFilter, wantDocs []string) {
	t.Helper()

	inputData := makeTestInputRecords(2)
	input := testProducer{data: inputData}

	projector := NewJSONProjector(&input, query.JSONColumn(1), fieldsFilter)

	outputData := make([]*query.Record, 0)
	for r := projector.Next(); r != nil; r = projector.Next() {
		outputData = append(outputData, r)
	}

	outputDocs := make([]string, 0, len(outputData))
	for _, r := range outputData {
		outputDocs = append(outputDocs, string(r.Vals[1].RawData()))
	}

	assert.Equal(t, wantDocs, outputDocs)
}

func TestJSONProjectorKeepsUpstreamValsAlive(t *testing.T) {
	const field = "k8s_pod"
	input := &testProducer{data: makeTestInputRecords(10)}

	extractor := NewJSONFieldsExtractor(input, query.JSONColumn(1), field)
	projector := NewJSONProjector(extractor, query.JSONColumn(1), &FieldsFilter{
		Fields:    []string{"service", "level"},
		AllowList: false,
	})
	podCol := query.StringColumn(2)

	records := make([]*query.Record, 0, 10)
	for r := projector.Next(); r != nil; r = projector.Next() {
		records = append(records, r)
	}

	for _, r := range records {
		want := fmt.Sprintf("pod-%d", r.Vals[0].AsUint32())
		assert.Equal(t, want, podCol.Val(r))
	}

	projector.Finalize()
}
