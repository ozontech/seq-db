package exec

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/query"
)

func TestFilterEq(t *testing.T) {
	const cond = 5

	filterExpr := NewEq[uint32](cond)

	testFilter(t, 0, filterExpr, func(r *query.Record) bool {
		return r.Vals[0].Decoded().(uint32) == uint32(cond)
	})
}

func TestFilterGt(t *testing.T) {
	const cond = 5

	filterExpr := NewGt[uint32](cond)

	testFilter(t, 0, filterExpr, func(r *query.Record) bool {
		return r.Vals[0].Decoded().(uint32) > uint32(cond)
	})
}

func TestFilterLt(t *testing.T) {
	const cond = 5

	filterExpr := NewLt[uint32](cond)

	testFilter(t, 0, filterExpr, func(r *query.Record) bool {
		return r.Vals[0].Decoded().(uint32) < uint32(cond)
	})
}

func testFilter(
	t *testing.T,
	colIdx int,
	filterExpr FilterExpr[uint32],
	wantFilterFunc func(*query.Record) bool,
) {
	t.Helper()

	inputData := makeTestInputRecords(10)
	input := testProducer{data: inputData}

	wantData := make([]*query.Record, 0)
	for _, r := range inputData {
		if wantFilterFunc(r) {
			wantData = append(wantData, r)
		}
	}

	filter := NewFilter(&input, colIdx, filterExpr)

	outputData := make([]*query.Record, 0)
	for r, has := filter.Next(); has; r, has = filter.Next() {
		outputData = append(outputData, r)
	}

	assert.Equal(t, wantData, outputData)
}
