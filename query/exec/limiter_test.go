package exec

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/query/encoding"
)

func TestLimiterLessThanInputLen(t *testing.T) {
	const limit = 5
	testLimiter(t, limit, 0)
}

func TestLimiterGreaterThanInputLen(t *testing.T) {
	const limit = 50
	testLimiter(t, limit, 0)
}

func TestLimiterOffset(t *testing.T) {
	const limit = 10
	const offset = 20
	testLimiter(t, limit, offset)
}

func testLimiter(t *testing.T, limit, offset int) {
	t.Helper()

	inputData := makeTestInputRecords(100)
	input := testProducer{data: inputData}
	limiter := NewLimiter(&input, uint32(limit), uint32(offset))
	outputData := make([]*query.Record, 0)
	for r := limiter.Next(); r != nil; r = limiter.Next() {
		outputData = append(outputData, r)
	}

	assert.Equal(t, inputData[offset:min(len(inputData), int(limit))+int(offset)], outputData)
}

func TestLimiterNoLimit(t *testing.T) {
	t.Run("no offset", func(t *testing.T) {
		inputData := makeTestInputRecords(100)
		limiter := NewLimiter(&testProducer{data: inputData}, 0, 0)

		outputData := make([]*query.Record, 0)
		for r := limiter.Next(); r != nil; r = limiter.Next() {
			outputData = append(outputData, r)
		}
		assert.Equal(t, inputData, outputData)
	})

	t.Run("with offset", func(t *testing.T) {
		const offset = 20
		inputData := makeTestInputRecords(100)
		limiter := NewLimiter(&testProducer{data: inputData}, 0, uint32(offset))

		outputData := make([]*query.Record, 0)
		for r := limiter.Next(); r != nil; r = limiter.Next() {
			outputData = append(outputData, r)
		}
		assert.Equal(t, inputData[offset:], outputData)
	})
}

type testProducer struct {
	data  []*query.Record
	cur   int
	total uint64
	err   error
}

func (p *testProducer) Next() *query.Record {
	if p.cur >= len(p.data) {
		return nil
	}

	r := p.data[p.cur]
	p.cur++

	return r
}

func (p *testProducer) Finalize() *query.Summary {
	if p.total == 0 && p.err == nil {
		return nil
	}
	return &query.Summary{Total: p.total, Err: p.err}
}

func makeTestInputRecords(count int) []*query.Record {
	out := make([]*query.Record, 0, count)

	for i := range count {
		doc := fmt.Sprintf(`{"service":"service-%d","level":3,"k8s_pod":"pod-%d"}`, i, i)
		out = append(out, &query.Record{
			Vals: []*query.RecordVals{
				query.NewRecordVals(query.DataTypeUint32, encoding.Uint32ToBytes(uint32(i))),
				query.NewRecordVals(query.DataTypeDocument, []byte(doc)),
			},
		})
	}

	return out
}
