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
	desc         bool
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
			name:         "desc overlap left has upper tail",
			left:         []uint32{1, 2, 3, 7, 8, 11, 15},
			right:        []uint32{1, 3, 7, 10},
			desc:         true,
			wantResult:   []uint32{1, 3, 7},
			wantLeftRes:  []uint32{1, 2, 3, 7, 8, 11, 15},
			wantRightRes: nil,
		},
		{
			name:         "desc overlap right has upper tail",
			left:         []uint32{1, 3, 7, 10},
			right:        []uint32{1, 2, 3, 7, 8, 11, 15},
			desc:         true,
			wantResult:   []uint32{1, 3, 7},
			wantLeftRes:  nil,
			wantRightRes: []uint32{1, 2, 3, 7, 8, 11, 15},
		},
		{
			name:         "desc disjoint lower vs upper",
			left:         []uint32{1, 2, 3},
			right:        []uint32{10, 11},
			desc:         true,
			wantResult:   nil,
			wantLeftRes:  nil,
			wantRightRes: []uint32{10, 11},
		},
		{
			name:         "asc overlap left has lower tail",
			left:         []uint32{1, 2, 3, 7, 8, 11, 15},
			right:        []uint32{1, 3, 7, 10},
			desc:         false,
			wantResult:   []uint32{1, 3, 7},
			wantLeftRes:  nil,
			wantRightRes: nil,
		},
		{
			name:         "asc overlap right has lower tail",
			left:         []uint32{1, 3, 7, 10},
			right:        []uint32{1, 2, 3, 7, 8, 11, 15},
			desc:         false,
			wantResult:   []uint32{1, 3, 7},
			wantLeftRes:  nil,
			wantRightRes: nil,
		},
		{
			name:         "asc disjoint lower vs upper",
			left:         []uint32{1, 2, 3},
			right:        []uint32{10, 11},
			desc:         false,
			wantResult:   nil,
			wantLeftRes:  []uint32{1, 2, 3},
			wantRightRes: nil,
		},
		{
			name:         "identical inputs have no residuals",
			left:         []uint32{2, 4, 9},
			right:        []uint32{2, 4, 9},
			desc:         true,
			wantResult:   []uint32{2, 4, 9},
			wantLeftRes:  nil,
			wantRightRes: nil,
		},
		{
			name:         "empty left",
			left:         nil,
			right:        []uint32{5, 6},
			desc:         true,
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

					result, leftRes, rightRes := And(left, right, tc.desc)

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
			name:         "desc overlap left has upper tail",
			left:         []uint32{1, 2, 3, 7, 8, 11, 15},
			right:        []uint32{1, 3, 7, 10},
			desc:         true,
			wantResult:   []uint32{1, 2, 3, 7, 8, 10},
			wantLeftRes:  []uint32{11, 15},
			wantRightRes: nil,
		},
		{
			name:         "desc overlap right has upper tail",
			left:         []uint32{1, 3, 7, 10},
			right:        []uint32{1, 2, 3, 7, 8, 11, 15},
			desc:         true,
			wantResult:   []uint32{1, 2, 3, 7, 8, 10},
			wantLeftRes:  nil,
			wantRightRes: []uint32{11, 15},
		},
		{
			name:         "desc disjoint lower vs upper",
			left:         []uint32{1, 2, 3},
			right:        []uint32{10, 11},
			desc:         true,
			wantResult:   []uint32{1, 2, 3},
			wantLeftRes:  nil,
			wantRightRes: []uint32{10, 11},
		},
		{
			name:         "asc overlap left has lower tail",
			left:         []uint32{1, 2, 3, 7, 8, 11, 15},
			right:        []uint32{1, 3, 7, 10},
			desc:         false,
			wantResult:   []uint32{1, 2, 3, 7, 8, 10, 11, 15},
			wantLeftRes:  nil,
			wantRightRes: nil,
		},
		{
			name:         "asc overlap right has lower tail",
			left:         []uint32{1, 3, 7, 10},
			right:        []uint32{1, 2, 3, 7, 8, 11, 15},
			desc:         false,
			wantResult:   []uint32{1, 2, 3, 7, 8, 10, 11, 15},
			wantLeftRes:  nil,
			wantRightRes: nil,
		},
		{
			name:         "asc disjoint lower vs upper",
			left:         []uint32{1, 2, 3},
			right:        []uint32{10, 11},
			desc:         false,
			wantResult:   []uint32{10, 11},
			wantLeftRes:  []uint32{1, 2, 3},
			wantRightRes: nil,
		},
		{
			name:         "empty left",
			left:         nil,
			right:        []uint32{5, 6},
			desc:         false,
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

					result, leftRes, rightRes := Or(left, right, tc.desc)

					assertSameSet(t, tc.wantResult, toSlice(result))
					assertSameSet(t, tc.wantLeftRes, toSlice(leftRes))
					assertSameSet(t, tc.wantRightRes, toSlice(rightRes))
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
	t.Run("desc overlap with one residual", func(t *testing.T) {
		batches := []LIDBatch{
			NewSliceBatch([]uint32{1, 2, 3, 7, 8, 11, 15}),
			NewBitmapBatchFromLids([]uint32{1, 3, 7, 10}),
			NewSliceBatch([]uint32{2, 3, 5, 8, 10}),
		}
		result, residuals := OrMulti(batches, true)
		assertSameSet(t, []uint32{1, 2, 3, 5, 7, 8, 10}, toSlice(result))
		assertSameSet(t, []uint32{11, 15}, toSlice(residuals[0]))
		assertSameSet(t, nil, toSlice(residuals[1]))
		assertSameSet(t, nil, toSlice(residuals[2]))
	})

	t.Run("asc overlap with one residual", func(t *testing.T) {
		batches := []LIDBatch{
			NewSliceBatch([]uint32{1, 2, 3, 7, 8, 11, 15}),
			NewBitmapBatchFromLids([]uint32{1, 3, 7, 10}),
			NewSliceBatch([]uint32{2, 3, 5, 8, 10}),
		}
		result, residuals := OrMulti(batches, false)
		assertSameSet(t, []uint32{2, 3, 5, 7, 8, 10, 11, 15}, toSlice(result))
		assertSameSet(t, []uint32{1}, toSlice(residuals[0]))
		assertSameSet(t, []uint32{1}, toSlice(residuals[1]))
		assertSameSet(t, nil, toSlice(residuals[2]))
	})

	t.Run("single non-empty behaves as pass-through", func(t *testing.T) {
		batches := []LIDBatch{
			EmptyBatch(),
			NewBitmapBatchFromLids([]uint32{4, 7, 9}),
			EmptyBatch(),
		}
		result, residuals := OrMulti(batches, true)
		assertSameSet(t, []uint32{4, 7, 9}, toSlice(result))
		assert.Len(t, residuals, 3)
		assertSameSet(t, nil, toSlice(residuals[0]))
		assertSameSet(t, nil, toSlice(residuals[1]))
		assertSameSet(t, nil, toSlice(residuals[2]))
	})
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
