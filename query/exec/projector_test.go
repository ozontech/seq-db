package exec

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/query"
)

func TestDocProjectorFields(t *testing.T) {
	testDocProjector(
		t,
		&storeapi.FieldsFilter{Fields: []string{"service", "level"}, AllowList: true},
		[]string{
			`{"service":"service-0","level":3}`,
			`{"service":"service-1","level":3}`,
		},
	)
}

func TestProjectorFieldsExcepr(t *testing.T) {
	testDocProjector(
		t,
		&storeapi.FieldsFilter{Fields: []string{"level"}, AllowList: false},
		[]string{
			`{"service":"service-0","k8s_pod":"pod-0"}`,
			`{"service":"service-1","k8s_pod":"pod-1"}`,
		},
	)
}

func testDocProjector(t *testing.T, fieldsFilter *storeapi.FieldsFilter, wantDocs []string) {
	t.Helper()

	inputData := makeTestInputRecords(2)
	input := testProducer{data: inputData}

	projector := NewDocProjector(&input, 1, fieldsFilter)

	outputData := make([]*query.Record, 0)
	for r, _ := projector.Next(); r != nil; r, _ = projector.Next() {
		outputData = append(outputData, r)
	}

	outputDocs := make([]string, 0, len(outputData))
	for _, r := range outputData {
		outputDocs = append(outputDocs, string(r.Vals[1].RawData()))
	}

	assert.Equal(t, wantDocs, outputDocs)
}
