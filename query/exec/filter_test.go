package exec

import (
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
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

func TestDocumentFilter(t *testing.T) {
	const (
		field = "service"
		cond  = "service-5"
	)

	filterExpr := NewDocFilter(field, NewEq[string](cond))

	testFilter(t, 1, filterExpr, func(r *query.Record) bool {
		field := r.Vals[1].Decoded().(*insaneJSON.Root).Dig(field)
		return field.AsString() == cond
	})
}

func testFilter[T any](
	t *testing.T,
	colIdx int,
	filterExpr FilterExpr[T],
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
	for r, _ := filter.Next(); r != nil; r, _ = filter.Next() {
		outputData = append(outputData, r)
	}

	assert.Equal(t, wantData, outputData)
}
