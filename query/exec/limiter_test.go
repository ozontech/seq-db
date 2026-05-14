package exec

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/query"
)

func TestLimiterLessThanInputLen(t *testing.T) {
	const limit = 5
	testLimiter(t, limit)
}

func TestLimiterGreaterThanInputLen(t *testing.T) {
	const limit = 50
	testLimiter(t, limit)
}

func testLimiter(t *testing.T, limit uint32) {
	t.Helper()

	inputData := makeTestInputRecords(10)
	input := testProducer{data: inputData}

	limiter := NewLimiter(&input, limit)

	outputData := make([]*query.Record, 0)
	for r, _ := limiter.Next(); r != nil; r, _ = limiter.Next() {
		outputData = append(outputData, r)
	}

	assert.Equal(t, inputData[:min(len(inputData), int(limit))], outputData)
}

type testProducer struct {
	data []*query.Record
	cur  int
}

func (p *testProducer) Next() (*query.Record, *query.Metadata) {
	if p.cur >= len(p.data) {
		return nil, nil
	}

	r := p.data[p.cur]
	p.cur++

	return r, nil
}

func makeTestInputRecords(count int) []*query.Record {
	out := make([]*query.Record, 0, count)

	for i := range count {
		doc := fmt.Sprintf(`{"service":"service-%d","level":3,"k8s_pod":"pod-%d"}`, i, i)
		out = append(out, &query.Record{
			Vals: []*query.RecordVals{
				query.NewRecordVals(query.DataTypeUint32, Uint32ToBytes(uint32(i))),
				query.NewRecordVals(query.DataTypeDocument, []byte(doc)),
			},
		})
	}

	return out
}

func Uint32ToBytes(val uint32) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint32(b, val)
	return b
}
