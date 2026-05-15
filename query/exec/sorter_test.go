package exec

import (
	"fmt"
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/query"
)

func TestSorterAsc(t *testing.T) {
	const field = "service"

	inputData := makeDocSorterTestRecords([]string{
		"service-03",
		"service-01",
		"service-02",
	})
	input := testProducer{data: inputData}

	sorter := NewDocSorter(&input, 1, field, OrderAsc)

	outputData := collectRecords(sorter)

	assert.Equal(t, []string{"service-01", "service-02", "service-03"}, extractFieldValues(outputData, field))
}

func TestSorterDesc(t *testing.T) {
	const field = "service"

	inputData := makeDocSorterTestRecords([]string{
		"service-01",
		"service-03",
		"service-02",
	})
	input := testProducer{data: inputData}

	sorter := NewDocSorter(&input, 1, field, OrderDesc)

	outputData := collectRecords(sorter)

	assert.Equal(t, []string{"service-03", "service-02", "service-01"}, extractFieldValues(outputData, field))
}

func TestSorterEmptyInput(t *testing.T) {
	const field = "service"

	inputData := make([]*query.Record, 0)
	input := testProducer{data: inputData}

	sorter := NewDocSorter(&input, 1, field, OrderAsc)

	outputData := collectRecords(sorter)

	assert.Empty(t, outputData)
}

func TestSorterEqualValues(t *testing.T) {
	const field = "service"

	inputData := makeDocSorterTestRecords([]string{
		"service-02",
		"service-02",
		"service-01",
		"service-02",
	})
	input := testProducer{data: inputData}

	sorter := NewDocSorter(&input, 1, field, OrderAsc)

	outputData := collectRecords(sorter)

	assert.Equal(t, []string{"service-01", "service-02", "service-02", "service-02"}, extractFieldValues(outputData, field))
}

func makeDocSorterTestRecords(values []string) []*query.Record {
	out := make([]*query.Record, 0, len(values))

	for _, v := range values {
		doc := fmt.Sprintf(`{"service":%q,"level":3}`, v)
		out = append(out, &query.Record{
			Vals: []*query.RecordVals{
				query.NewRecordVals(query.DataTypeUint32, Uint32ToBytes(1)),
				query.NewRecordVals(query.DataTypeDocument, []byte(doc)),
			},
		})
	}

	return out
}

func collectRecords(p query.RecordProducer) []*query.Record {
	out := make([]*query.Record, 0)
	for r, _ := p.Next(); r != nil; r, _ = p.Next() {
		out = append(out, r)
	}
	return out
}

func extractFieldValues(records []*query.Record, field string) []string {
	out := make([]string, 0, len(records))
	for _, r := range records {
		val := r.Vals[1].Decoded().(*insaneJSON.Root).Dig(field).AsString()
		out = append(out, val)
	}
	return out
}
