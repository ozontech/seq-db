package node

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

type batchCase struct {
	name         string
	left         []uint32
	right        []uint32
	asc          bool
	wantResult   []uint32
	wantLeftRes  []uint32
	wantRightRes []uint32
}

type opsBatchFactory func([]uint32) LIDBatch

var opsBatchFactories = []struct {
	name string
	fn   opsBatchFactory
}{
	{name: "bitmap", fn: NewBitmapBatchFromLids},
	{name: "slice", fn: NewSliceBatch},
}

func TestLIDBatch_And(t *testing.T) {
	testCases := []batchCase{
		{
			name:         "asc overlap left has upper tail",
			left:         []uint32{1, 2, 3, 7, 8, 11, 15},
			right:        []uint32{1, 3, 7, 10},
			asc:          true,
			wantResult:   []uint32{1, 3, 7},
			wantLeftRes:  []uint32{1, 2, 3, 7, 8, 11, 15},
			wantRightRes: nil,
		},
		{
			name:         "asc overlap right has upper tail",
			left:         []uint32{1, 3, 7, 10},
			right:        []uint32{1, 2, 3, 7, 8, 11, 15},
			asc:          true,
			wantResult:   []uint32{1, 3, 7},
			wantLeftRes:  nil,
			wantRightRes: []uint32{1, 2, 3, 7, 8, 11, 15},
		},
		{
			name:         "asc disjoint lower vs upper",
			left:         []uint32{1, 2, 3},
			right:        []uint32{10, 11},
			asc:          true,
			wantResult:   nil,
			wantLeftRes:  nil,
			wantRightRes: []uint32{10, 11},
		},
		{
			name:         "desc overlap left has lower tail",
			left:         []uint32{1, 2, 3, 7, 8, 11, 15},
			right:        []uint32{1, 3, 7, 10},
			asc:          false,
			wantResult:   []uint32{1, 3, 7},
			wantLeftRes:  nil,
			wantRightRes: nil,
		},
		{
			name:         "desc overlap right has lower tail",
			left:         []uint32{1, 3, 7, 10},
			right:        []uint32{1, 2, 3, 7, 8, 11, 15},
			asc:          false,
			wantResult:   []uint32{1, 3, 7},
			wantLeftRes:  nil,
			wantRightRes: nil,
		},
		{
			name:         "desc disjoint lower vs upper",
			left:         []uint32{1, 2, 3},
			right:        []uint32{10, 11},
			asc:          false,
			wantResult:   nil,
			wantLeftRes:  []uint32{1, 2, 3},
			wantRightRes: nil,
		},
		{
			name:         "identical inputs have no residuals",
			left:         []uint32{2, 4, 9},
			right:        []uint32{2, 4, 9},
			asc:          true,
			wantResult:   []uint32{2, 4, 9},
			wantLeftRes:  nil,
			wantRightRes: nil,
		},
		{
			name:         "empty left",
			left:         nil,
			right:        []uint32{5, 6},
			asc:          true,
			wantResult:   nil,
			wantLeftRes:  nil,
			wantRightRes: nil,
		},
	}

	for _, impl := range opsBatchFactories {
		t.Run(impl.name, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					left := impl.fn(tc.left)
					right := impl.fn(tc.right)

					result, leftRes, rightRes := And(left, right, tc.asc)

					assertSameSet(t, tc.wantResult, toSlice(result))
					assertSameSet(t, tc.wantLeftRes, toSlice(leftRes))
					assertSameSet(t, tc.wantRightRes, toSlice(rightRes))
				})
			}
		})
	}
}

func TestLIDBatch_Or(t *testing.T) {
	testCases := []batchCase{
		{
			name:         "asc overlap left has upper tail",
			left:         []uint32{1, 2, 3, 7, 8, 11, 15},
			right:        []uint32{1, 3, 7, 10},
			asc:          true,
			wantResult:   []uint32{1, 2, 3, 7, 8, 10},
			wantLeftRes:  []uint32{11, 15},
			wantRightRes: nil,
		},
		{
			name:         "asc overlap right has upper tail",
			left:         []uint32{1, 3, 7, 10},
			right:        []uint32{1, 2, 3, 7, 8, 11, 15},
			asc:          true,
			wantResult:   []uint32{1, 2, 3, 7, 8, 10},
			wantLeftRes:  nil,
			wantRightRes: []uint32{11, 15},
		},
		{
			name:         "asc disjoint lower vs upper",
			left:         []uint32{1, 2, 3},
			right:        []uint32{10, 11},
			asc:          true,
			wantResult:   []uint32{1, 2, 3},
			wantLeftRes:  nil,
			wantRightRes: []uint32{10, 11},
		},
		{
			name:         "desc overlap left has lower tail",
			left:         []uint32{1, 2, 3, 7, 8, 11, 15},
			right:        []uint32{1, 3, 7, 10},
			asc:          false,
			wantResult:   []uint32{1, 2, 3, 7, 8, 10, 11, 15},
			wantLeftRes:  nil,
			wantRightRes: nil,
		},
		{
			name:         "desc overlap right has lower tail",
			left:         []uint32{1, 3, 7, 10},
			right:        []uint32{1, 2, 3, 7, 8, 11, 15},
			asc:          false,
			wantResult:   []uint32{1, 2, 3, 7, 8, 10, 11, 15},
			wantLeftRes:  nil,
			wantRightRes: nil,
		},
		{
			name:         "desc disjoint lower vs upper",
			left:         []uint32{1, 2, 3},
			right:        []uint32{10, 11},
			asc:          false,
			wantResult:   []uint32{10, 11},
			wantLeftRes:  []uint32{1, 2, 3},
			wantRightRes: nil,
		},
		{
			name:         "empty left",
			left:         nil,
			right:        []uint32{5, 6},
			asc:          false,
			wantResult:   []uint32{5, 6},
			wantLeftRes:  nil,
			wantRightRes: nil,
		},
	}

	for _, impl := range opsBatchFactories {
		t.Run(impl.name, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					left := impl.fn(tc.left)
					right := impl.fn(tc.right)

					result, leftRes, rightRes := Or(left, right, tc.asc)

					assertSameSet(t, tc.wantResult, toSlice(result))
					assertSameSet(t, tc.wantLeftRes, toSlice(leftRes))
					assertSameSet(t, tc.wantRightRes, toSlice(rightRes))
				})
			}
		})
	}
}

func TestLIDBatch_AndNot(t *testing.T) {
	testCases := []batchCase{
		{
			name:         "asc overlap reg has upper tail",
			left:         []uint32{1, 2, 3, 7, 8, 11, 15},
			right:        []uint32{1, 3, 7, 10},
			asc:          true,
			wantResult:   []uint32{2, 8},
			wantLeftRes:  []uint32{11, 15},
			wantRightRes: nil,
		},
		{
			name:         "asc overlap neg has upper tail",
			left:         []uint32{1, 3, 7, 10},
			right:        []uint32{1, 2, 3, 7, 8, 11, 15},
			asc:          true,
			wantResult:   []uint32{10},
			wantLeftRes:  nil,
			wantRightRes: []uint32{11, 15},
		},
		{
			name:         "asc disjoint lower reg vs upper neg",
			left:         []uint32{1, 2, 3},
			right:        []uint32{10, 11},
			asc:          true,
			wantResult:   []uint32{1, 2, 3},
			wantLeftRes:  nil,
			wantRightRes: []uint32{10, 11},
		},
		{
			name:         "desc overlap reg has lower tail",
			left:         []uint32{1, 2, 3, 7, 8, 11, 15},
			right:        []uint32{1, 3, 7, 10},
			asc:          false,
			wantResult:   []uint32{2, 8, 11, 15},
			wantLeftRes:  nil,
			wantRightRes: nil,
		},
		{
			name:         "desc overlap neg has lower tail",
			left:         []uint32{1, 3, 7, 10},
			right:        []uint32{1, 2, 3, 7, 8, 11, 15},
			asc:          false,
			wantResult:   []uint32{10},
			wantLeftRes:  nil,
			wantRightRes: nil,
		},
		{
			name:         "desc disjoint lower reg vs upper neg",
			left:         []uint32{1, 2, 3},
			right:        []uint32{10, 11},
			asc:          false,
			wantResult:   nil,
			wantLeftRes:  []uint32{1, 2, 3},
			wantRightRes: nil,
		},
		{
			name:         "empty reg",
			left:         nil,
			right:        []uint32{5, 6},
			asc:          false,
			wantResult:   nil,
			wantLeftRes:  nil,
			wantRightRes: []uint32{5, 6},
		},
		{
			name:         "empty neg",
			left:         []uint32{5, 6},
			right:        nil,
			asc:          false,
			wantResult:   []uint32{5, 6},
			wantLeftRes:  nil,
			wantRightRes: nil,
		},
	}

	for _, impl := range opsBatchFactories {
		t.Run(impl.name, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					reg := impl.fn(tc.left)
					neg := impl.fn(tc.right)

					result, regRes, negRes := AndNot(reg, neg, tc.asc)

					assertSameSet(t, tc.wantResult, toSlice(result))
					assertSameSet(t, tc.wantLeftRes, toSlice(regRes))
					assertSameSet(t, tc.wantRightRes, toSlice(negRes))
				})
			}
		})
	}
}

func TestLIDBatch_AndMixedTypes(t *testing.T) {
	left := NewSliceBatch([]uint32{1, 3, 7, 10})
	right := NewBitmapBatchFromLids([]uint32{1, 3, 7, 15})

	result, leftRes, rightRes := And(left, right, true)

	assertSameSet(t, []uint32{1, 3, 7}, toSlice(result))
	assertSameSet(t, nil, toSlice(leftRes))
	assertSameSet(t, []uint32{1, 3, 7, 15}, toSlice(rightRes))
}

func TestLIDBatch_OrMixedTypes(t *testing.T) {
	left := NewSliceBatch([]uint32{1, 3, 7, 10})
	right := NewBitmapBatchFromLids([]uint32{1, 3, 7, 15})

	result, leftRes, rightRes := Or(left, right, true)

	assertSameSet(t, []uint32{1, 3, 7, 10}, toSlice(result))
	assertSameSet(t, nil, toSlice(leftRes))
	assertSameSet(t, []uint32{15}, toSlice(rightRes))
}

func TestLIDBatch_OrMulti(t *testing.T) {
	type orMultiCase struct {
		name          string
		asc           bool
		inputs        [][]uint32
		wantResult    []uint32
		wantResiduals [][]uint32
	}

	testCases := []orMultiCase{
		{
			name:       "asc overlap with one residual",
			asc:        true,
			inputs:     [][]uint32{{1, 2, 3, 7, 8, 11, 15}, {1, 3, 7, 10}, {2, 3, 5, 8, 10}},
			wantResult: []uint32{1, 2, 3, 5, 7, 8, 10},
			wantResiduals: [][]uint32{
				{11, 15},
				nil,
				nil,
			},
		},
		{
			name:       "desc overlap with one residual",
			asc:        false,
			inputs:     [][]uint32{{1, 2, 3, 7, 8, 11, 15}, {1, 3, 7, 10}, {2, 3, 5, 8, 10}},
			wantResult: []uint32{2, 3, 5, 7, 8, 10, 11, 15},
			wantResiduals: [][]uint32{
				{1},
				{1},
				nil,
			},
		},
		{
			name:       "single non-empty behaves as pass-through",
			asc:        true,
			inputs:     [][]uint32{nil, {4, 7, 9}, nil},
			wantResult: []uint32{4, 7, 9},
			wantResiduals: [][]uint32{
				nil,
				nil,
				nil,
			},
		},
	}

	for _, impl := range opsBatchFactories {
		t.Run(impl.name, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					batches := make([]LIDBatch, len(tc.inputs))
					for i, lids := range tc.inputs {
						batches[i] = impl.fn(lids)
					}

					result, residuals := OrMulti(batches, tc.asc)

					assertSameSet(t, tc.wantResult, toSlice(result))
					assert.Len(t, residuals, len(tc.wantResiduals))
					for i := range tc.wantResiduals {
						assertSameSet(t, tc.wantResiduals[i], toSlice(residuals[i]))
					}
				})
			}
		})
	}
}

func assertSameSet(t *testing.T, want, got []uint32) {
	t.Helper()
	if len(want) == 0 {
		want = nil
	}
	if len(got) == 0 {
		got = nil
	}
	slices.Sort(want)
	slices.Sort(got)
	assert.Equal(t, want, got)
}
