package exec

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/query"
)

func TestExecutorsCombination(t *testing.T) {
	inputData := makeTestInputRecords(10)
	producer := testProducer{data: inputData}

	const (
		cond  = 5
		limit = 2
	)

	wantData := make([]*query.Record, 0)
	for _, r := range inputData {
		if r.Vals[0].Decoded().(uint32) > uint32(cond) {
			wantData = append(wantData, r)
		}
	}

	filter := NewFilter(&producer, 0, NewGt[uint32](cond))
	limiter := NewLimiter(filter, limit)

	outputData := make([]*query.Record, 0)
	for r, _ := limiter.Next(); r != nil; r, _ = limiter.Next() {
		outputData = append(outputData, r)
	}

	assert.Equal(t, wantData[:limit], outputData)
}
