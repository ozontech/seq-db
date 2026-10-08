package exec

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/query"
)

var assertErr = errors.New("some error")

func TestFilterEq(t *testing.T) {
	const cond = 5

	filterExpr := NewEq[uint32](cond)

	testFilter(t, query.Uint32Column(0), filterExpr, func(r *query.Record) bool {
		return r.Vals[0].AsUint32() == uint32(cond)
	})
}

func TestFilterGt(t *testing.T) {
	const cond = 5

	filterExpr := NewGt[uint32](cond)

	testFilter(t, query.Uint32Column(0), filterExpr, func(r *query.Record) bool {
		return r.Vals[0].AsUint32() > uint32(cond)
	})
}

func TestFilterLt(t *testing.T) {
	const cond = 5

	filterExpr := NewLt[uint32](cond)

	testFilter(t, query.Uint32Column(0), filterExpr, func(r *query.Record) bool {
		return r.Vals[0].AsUint32() < uint32(cond)
	})
}

func TestDocumentFilter(t *testing.T) {
	const (
		field = "service"
		cond  = "service-5"
	)

	filterExpr := NewDocFilter(field, NewEq(cond))

	testFilter(t, query.DocColumn(1), filterExpr, func(r *query.Record) bool {
		field := r.Vals[1].AsDoc().Dig(field)
		return field.AsString() == cond
	})
}

func testFilter[T any](
	t *testing.T,
	col query.Column[T],
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

	filter := NewFilter(&input, col, filterExpr, false)

	outputData := make([]*query.Record, 0)
	for r := filter.Next(); r != nil; r = filter.Next() {
		outputData = append(outputData, r)
	}

	assert.Equal(t, wantData, outputData)
}

func TestFilterTotalDrainsInput(t *testing.T) {
	const cond = 5

	filterExpr := NewEq[uint32](cond)

	inputData := makeTestInputRecords(10)
	input := testProducer{data: inputData, total: uint64(len(inputData))}

	wantCount := 0
	wantData := make([]*query.Record, 0)
	for _, r := range inputData {
		if r.Vals[0].AsUint32() == uint32(cond) {
			wantCount++
			wantData = append(wantData, r)
		}
	}

	filter := NewFilter(&input, query.Uint32Column(0), filterExpr, true)
	outputData := make([]*query.Record, 0)
	for i := 0; i < len(wantData); i++ {
		r := filter.Next()
		assert.NotNil(t, r)
		outputData = append(outputData, r)
	}
	assert.Equal(t, wantData, outputData)

	summary := filter.Finalize()
	assert.Equal(t, uint64(wantCount), summary.Total)
}

func TestFilterTotalErrorPropagated(t *testing.T) {
	const cond = 5

	filterExpr := NewEq[uint32](cond)

	inputData := makeTestInputRecords(10)
	input := testProducer{
		data:  inputData,
		total: uint64(len(inputData)),
		err:   assertErr,
	}

	filter := NewFilter(&input, query.Uint32Column(0), filterExpr, true)
	for r := filter.Next(); r != nil; r = filter.Next() {
	}

	summary := filter.Finalize()
	assert.Equal(t, assertErr, summary.Err)
}
