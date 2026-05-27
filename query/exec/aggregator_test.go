package exec

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/seq"
)

func TestDistributedAggregator_Count(t *testing.T) {
	records1 := []*query.Record{
		newAggRecord("key1", 10.0, 10.0, 10.0, 1),
		newAggRecord("key2", 20.0, 20.0, 20.0, 1),
	}

	records2 := []*query.Record{
		newAggRecord("key1", 30.0, 30.0, 30.0, 1),
		newAggRecord("key2", 40.0, 40.0, 40.0, 1),
	}

	input1 := testProducer{data: records1}
	input2 := testProducer{data: records2}
	agg := NewDistributedAggregator([]query.RecordProducer{&input1, &input2}, seq.AggFuncCount)

	results := collectRecords(agg)

	assert.Len(t, results, 2)
	assert.Equal(t, "key1", results[0].Vals[0].Decoded().(string))
	assert.Equal(t, float64(2), results[0].Vals[1].Decoded().(float64))
	assert.Equal(t, "key2", results[1].Vals[0].Decoded().(string))
	assert.Equal(t, float64(2), results[1].Vals[1].Decoded().(float64))
}

func TestDistributedAggregator_Sum(t *testing.T) {
	records1 := []*query.Record{
		newAggRecord("key1", 10.0, 10.0, 10.0, 1),
		newAggRecord("key2", 30.0, 30.0, 30.0, 1),
	}

	records2 := []*query.Record{
		newAggRecord("key1", 20.0, 20.0, 20.0, 1),
		newAggRecord("key2", 40.0, 40.0, 40.0, 1),
	}

	input1 := testProducer{data: records1}
	input2 := testProducer{data: records2}
	agg := NewDistributedAggregator([]query.RecordProducer{&input1, &input2}, seq.AggFuncSum)

	results := collectRecords(agg)

	assert.Len(t, results, 2)
	assert.Equal(t, "key2", results[0].Vals[0].Decoded().(string))
	assert.Equal(t, float64(70.0), results[0].Vals[1].Decoded().(float64))
	assert.Equal(t, "key1", results[1].Vals[0].Decoded().(string))
	assert.Equal(t, float64(30.0), results[1].Vals[1].Decoded().(float64))
}

func TestDistributedAggregator_Min(t *testing.T) {
	records1 := []*query.Record{
		newAggRecord("key1", 30.0, 30.0, 30.0, 1),
		newAggRecord("key2", 20.0, 20.0, 20.0, 1),
	}

	records2 := []*query.Record{
		newAggRecord("key1", 10.0, 10.0, 10.0, 1),
		newAggRecord("key2", 40.0, 40.0, 40.0, 1),
	}

	input1 := testProducer{data: records1}
	input2 := testProducer{data: records2}
	agg := NewDistributedAggregator([]query.RecordProducer{&input1, &input2}, seq.AggFuncMin)

	results := collectRecords(agg)

	assert.Len(t, results, 2)
	assert.Equal(t, "key1", results[0].Vals[0].Decoded().(string))
	assert.Equal(t, float64(10.0), results[0].Vals[1].Decoded().(float64))
	assert.Equal(t, "key2", results[1].Vals[0].Decoded().(string))
	assert.Equal(t, float64(20.0), results[1].Vals[1].Decoded().(float64))
}

func TestDistributedAggregator_Max(t *testing.T) {
	records1 := []*query.Record{
		newAggRecord("key1", 10.0, 10.0, 10.0, 1),
		newAggRecord("key2", 30.0, 30.0, 30.0, 1),
	}

	records2 := []*query.Record{
		newAggRecord("key1", 50.0, 50.0, 50.0, 1),
		newAggRecord("key2", 20.0, 20.0, 20.0, 1),
	}

	input1 := testProducer{data: records1}
	input2 := testProducer{data: records2}
	agg := NewDistributedAggregator([]query.RecordProducer{&input1, &input2}, seq.AggFuncMax)

	results := collectRecords(agg)

	assert.Len(t, results, 2)
	assert.Equal(t, "key1", results[0].Vals[0].Decoded().(string))
	assert.Equal(t, float64(50.0), results[0].Vals[1].Decoded().(float64))
	assert.Equal(t, "key2", results[1].Vals[0].Decoded().(string))
	assert.Equal(t, float64(30.0), results[1].Vals[1].Decoded().(float64))
}

func TestDistributedAggregator_Avg(t *testing.T) {
	records1 := []*query.Record{
		newAggRecord("key1", 10.0, 10.0, 10.0, 1),
		newAggRecord("key2", 20.0, 20.0, 20.0, 1),
	}

	records2 := []*query.Record{
		newAggRecord("key1", 30.0, 30.0, 30.0, 1),
		newAggRecord("key2", 40.0, 40.0, 40.0, 1),
	}

	input1 := testProducer{data: records1}
	input2 := testProducer{data: records2}
	agg := NewDistributedAggregator([]query.RecordProducer{&input1, &input2}, seq.AggFuncAvg)

	results := collectRecords(agg)

	assert.Len(t, results, 2)
	assert.Equal(t, "key2", results[0].Vals[0].Decoded().(string))
	assert.Equal(t, float64(30.0), results[0].Vals[1].Decoded().(float64))
	assert.Equal(t, "key1", results[1].Vals[0].Decoded().(string))
	assert.Equal(t, float64(20.0), results[1].Vals[1].Decoded().(float64))
}

func TestDistributedAggregator_EmptyInput(t *testing.T) {
	var records []*query.Record

	input := testProducer{data: records}
	agg := NewDistributedAggregator([]query.RecordProducer{&input}, seq.AggFuncCount)

	results := collectRecords(agg)

	assert.Len(t, results, 0)
}

func newAggRecord(key string, minVal, maxVal, sum float64, total uint64) *query.Record {
	return query.NewRecord([]*query.RecordVals{
		query.NewRecordVals(query.DataTypeString, []byte(key)),
		query.NewRecordVals(query.DataTypeFloat64, float64ToBytes(minVal)),
		query.NewRecordVals(query.DataTypeFloat64, float64ToBytes(maxVal)),
		query.NewRecordVals(query.DataTypeFloat64, float64ToBytes(sum)),
		query.NewRecordVals(query.DataTypeUint64, Uint64ToBytes(uint64(total))),
	})
}
