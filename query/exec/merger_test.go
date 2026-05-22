package exec

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/query"
)

func TestMergerAsc(t *testing.T) {
	const field = "service"

	leftInput := makeMergerTestRecords([]string{
		"service-01",
		"service-03",
		"service-05",
	})
	rightInput := makeMergerTestRecords([]string{
		"service-02",
		"service-04",
		"service-06",
	})

	merger := NewMerger(
		&testProducer{data: leftInput},
		&testProducer{data: rightInput},
		1,
		field,
		query.DataTypeDocument,
		OrderAsc,
	)

	outputData := collectRecords(merger)

	assert.Equal(t, []string{
		"service-01", "service-02", "service-03",
		"service-04", "service-05", "service-06",
	}, extractFieldValues(outputData, field))
}

func TestMergerDesc(t *testing.T) {
	const field = "service"

	leftInput := makeMergerTestRecords([]string{
		"service-06",
		"service-04",
		"service-02",
	})
	rightInput := makeMergerTestRecords([]string{
		"service-05",
		"service-03",
		"service-01",
	})

	merger := NewMerger(
		&testProducer{data: leftInput},
		&testProducer{data: rightInput},
		1,
		field,
		query.DataTypeDocument,
		OrderDesc,
	)

	outputData := collectRecords(merger)

	assert.Equal(t, []string{
		"service-06", "service-05", "service-04",
		"service-03", "service-02", "service-01",
	}, extractFieldValues(outputData, field))
}

func TestMergerLeftEmpty(t *testing.T) {
	const field = "service"

	leftInput := makeMergerTestRecords([]string{})
	rightInput := makeMergerTestRecords([]string{
		"service-01",
		"service-02",
	})

	merger := NewMerger(
		&testProducer{data: leftInput},
		&testProducer{data: rightInput},
		1,
		field,
		query.DataTypeDocument,
		OrderAsc,
	)

	outputData := collectRecords(merger)

	assert.Equal(t, []string{"service-01", "service-02"}, extractFieldValues(outputData, field))
}

func TestMergerRightEmpty(t *testing.T) {
	const field = "service"

	leftInput := makeMergerTestRecords([]string{
		"service-01",
		"service-02",
	})
	rightInput := makeMergerTestRecords([]string{})

	merger := NewMerger(
		&testProducer{data: leftInput},
		&testProducer{data: rightInput},
		1,
		field,
		query.DataTypeDocument,
		OrderAsc,
	)

	outputData := collectRecords(merger)

	assert.Equal(t, []string{"service-01", "service-02"}, extractFieldValues(outputData, field))
}

func TestMergerBothEmpty(t *testing.T) {
	const field = "service"

	leftInput := makeMergerTestRecords([]string{})
	rightInput := makeMergerTestRecords([]string{})

	merger := NewMerger(
		&testProducer{data: leftInput},
		&testProducer{data: rightInput},
		1,
		field,
		query.DataTypeDocument,
		OrderAsc,
	)

	outputData := collectRecords(merger)

	assert.Empty(t, outputData)
}

func TestMergerDuplicates(t *testing.T) {
	const field = "service"

	leftInput := makeMergerTestRecords([]string{
		"service-01",
		"service-01",
		"service-03",
	})
	rightInput := makeMergerTestRecords([]string{
		"service-01",
		"service-02",
		"service-03",
	})

	merger := NewMerger(
		&testProducer{data: leftInput},
		&testProducer{data: rightInput},
		1,
		field,
		query.DataTypeDocument,
		OrderAsc,
	)

	outputData := collectRecords(merger)

	assert.Equal(t, []string{
		"service-01", "service-01", "service-01",
		"service-02", "service-03", "service-03",
	}, extractFieldValues(outputData, field))
}

func TestMergerUint32(t *testing.T) {
	leftInput := makeMergerUint32Records([]uint32{1, 3, 5})
	rightInput := makeMergerUint32Records([]uint32{2, 4, 6})

	merger := NewMerger(
		&testProducer{data: leftInput},
		&testProducer{data: rightInput},
		0,
		"",
		query.DataTypeUint32,
		OrderAsc,
	)

	outputData := collectRecords(merger)

	assert.Equal(t, []uint32{1, 2, 3, 4, 5, 6}, extractUint32Values(outputData))
}

func TestMergerUint64(t *testing.T) {
	leftInput := makeMergerUint64Records([]uint64{10, 30, 50})
	rightInput := makeMergerUint64Records([]uint64{20, 40, 60})

	merger := NewMerger(
		&testProducer{data: leftInput},
		&testProducer{data: rightInput},
		0,
		"",
		query.DataTypeUint64,
		OrderAsc,
	)

	outputData := collectRecords(merger)

	assert.Equal(t, []uint64{10, 20, 30, 40, 50, 60}, extractUint64Values(outputData))
}

func TestMergerInt32(t *testing.T) {
	leftInput := makeMergerInt32Records([]int32{-5, 0, 5})
	rightInput := makeMergerInt32Records([]int32{-3, 2, 10})

	merger := NewMerger(
		&testProducer{data: leftInput},
		&testProducer{data: rightInput},
		0,
		"",
		query.DataTypeInt32,
		OrderAsc,
	)

	outputData := collectRecords(merger)

	assert.Equal(t, []int32{-5, -3, 0, 2, 5, 10}, extractInt32Values(outputData))
}

func TestMergerInt64(t *testing.T) {
	leftInput := makeMergerInt64Records([]int64{100, 0, -100})
	rightInput := makeMergerInt64Records([]int64{200, 50, -50})

	merger := NewMerger(
		&testProducer{data: leftInput},
		&testProducer{data: rightInput},
		0,
		"",
		query.DataTypeInt64,
		OrderDesc,
	)

	outputData := collectRecords(merger)

	assert.Equal(t, []int64{200, 100, 50, 0, -50, -100}, extractInt64Values(outputData))
}

func TestMergerFloat64(t *testing.T) {
	leftInput := makeMergerFloat64Records([]float64{1.5, 3.5, 5.5})
	rightInput := makeMergerFloat64Records([]float64{2.5, 4.5, 6.5})

	merger := NewMerger(
		&testProducer{data: leftInput},
		&testProducer{data: rightInput},
		0,
		"",
		query.DataTypeFloat64,
		OrderAsc,
	)

	outputData := collectRecords(merger)

	assert.Equal(t, []float64{1.5, 2.5, 3.5, 4.5, 5.5, 6.5}, extractFloat64Values(outputData))
}

func TestMergerString(t *testing.T) {
	leftInput := makeMergerStringRecords([]string{"apple", "banana", "cherry"})
	rightInput := makeMergerStringRecords([]string{"apricot", "date", "elder"})

	merger := NewMerger(
		&testProducer{data: leftInput},
		&testProducer{data: rightInput},
		0,
		"",
		query.DataTypeString,
		OrderAsc,
	)

	outputData := collectRecords(merger)

	assert.Equal(t, []string{"apple", "apricot", "banana", "cherry", "date", "elder"}, extractStringValues(outputData))
}

func makeMergerTestRecords(values []string) []*query.Record {
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

func makeMergerUint32Records(values []uint32) []*query.Record {
	out := make([]*query.Record, 0, len(values))

	for _, v := range values {
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, v)
		out = append(out, &query.Record{
			Vals: []*query.RecordVals{
				query.NewRecordVals(query.DataTypeUint32, b),
			},
		})
	}

	return out
}

func makeMergerUint64Records(values []uint64) []*query.Record {
	out := make([]*query.Record, 0, len(values))

	for _, v := range values {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, v)
		out = append(out, &query.Record{
			Vals: []*query.RecordVals{
				query.NewRecordVals(query.DataTypeUint64, b),
			},
		})
	}

	return out
}

func makeMergerInt32Records(values []int32) []*query.Record {
	out := make([]*query.Record, 0, len(values))

	for _, v := range values {
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, uint32(v))
		out = append(out, &query.Record{
			Vals: []*query.RecordVals{
				query.NewRecordVals(query.DataTypeInt32, b),
			},
		})
	}

	return out
}

func makeMergerInt64Records(values []int64) []*query.Record {
	out := make([]*query.Record, 0, len(values))

	for _, v := range values {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(v))
		out = append(out, &query.Record{
			Vals: []*query.RecordVals{
				query.NewRecordVals(query.DataTypeInt64, b),
			},
		})
	}

	return out
}

func makeMergerFloat64Records(values []float64) []*query.Record {
	out := make([]*query.Record, 0, len(values))

	for _, v := range values {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, math.Float64bits(v))
		out = append(out, &query.Record{
			Vals: []*query.RecordVals{
				query.NewRecordVals(query.DataTypeFloat64, b),
			},
		})
	}

	return out
}

func makeMergerStringRecords(values []string) []*query.Record {
	out := make([]*query.Record, 0, len(values))

	for _, v := range values {
		out = append(out, &query.Record{
			Vals: []*query.RecordVals{
				query.NewRecordVals(query.DataTypeString, []byte(v)),
			},
		})
	}

	return out
}

func extractUint32Values(records []*query.Record) []uint32 {
	out := make([]uint32, 0, len(records))
	for _, r := range records {
		out = append(out, r.Vals[0].Decoded().(uint32))
	}
	return out
}

func extractUint64Values(records []*query.Record) []uint64 {
	out := make([]uint64, 0, len(records))
	for _, r := range records {
		out = append(out, r.Vals[0].Decoded().(uint64))
	}
	return out
}

func extractInt32Values(records []*query.Record) []int32 {
	out := make([]int32, 0, len(records))
	for _, r := range records {
		out = append(out, r.Vals[0].Decoded().(int32))
	}
	return out
}

func extractInt64Values(records []*query.Record) []int64 {
	out := make([]int64, 0, len(records))
	for _, r := range records {
		out = append(out, r.Vals[0].Decoded().(int64))
	}
	return out
}

func extractFloat64Values(records []*query.Record) []float64 {
	out := make([]float64, 0, len(records))
	for _, r := range records {
		out = append(out, r.Vals[0].Decoded().(float64))
	}
	return out
}

func extractStringValues(records []*query.Record) []string {
	out := make([]string, 0, len(records))
	for _, r := range records {
		out = append(out, r.Vals[0].Decoded().(string))
	}
	return out
}

func TestNewNMergedProducersEmpty(t *testing.T) {
	const field = "service"

	producers := []query.RecordProducer{}

	merger := NewNMergedProducers(producers, 1, field, query.DataTypeDocument, OrderAsc)

	outputData := collectRecords(merger)

	assert.Empty(t, outputData)
}

func TestNewNMergedProducersSingle(t *testing.T) {
	const field = "service"

	input := makeMergerTestRecords([]string{
		"service-01",
		"service-02",
	})

	producers := []query.RecordProducer{
		&testProducer{data: input},
	}

	merger := NewNMergedProducers(producers, 1, field, query.DataTypeDocument, OrderAsc)

	outputData := collectRecords(merger)

	assert.Equal(t, []string{"service-01", "service-02"}, extractFieldValues(outputData, field))
}

func TestNewNMergedProducersThree(t *testing.T) {
	const field = "service"

	producer1 := makeMergerTestRecords([]string{"service-01", "service-04"})
	producer2 := makeMergerTestRecords([]string{"service-02", "service-05"})
	producer3 := makeMergerTestRecords([]string{"service-03", "service-06"})

	producers := []query.RecordProducer{
		&testProducer{data: producer1},
		&testProducer{data: producer2},
		&testProducer{data: producer3},
	}

	merger := NewNMergedProducers(producers, 1, field, query.DataTypeDocument, OrderAsc)

	outputData := collectRecords(merger)

	assert.Equal(t, []string{
		"service-01", "service-02", "service-03",
		"service-04", "service-05", "service-06",
	}, extractFieldValues(outputData, field))
}

func TestNewNMergedProducersWithEmpty(t *testing.T) {
	const field = "service"

	producer1 := makeMergerTestRecords([]string{"service-03", "service-01"})
	producer2 := makeMergerTestRecords([]string{})
	producer3 := makeMergerTestRecords([]string{"service-02"})

	producers := []query.RecordProducer{
		&testProducer{data: producer1},
		&testProducer{data: producer2},
		&testProducer{data: producer3},
	}

	merger := NewNMergedProducers(producers, 1, field, query.DataTypeDocument, OrderDesc)

	outputData := collectRecords(merger)

	assert.Equal(t, []string{
		"service-03", "service-02", "service-01",
	}, extractFieldValues(outputData, field))
}
