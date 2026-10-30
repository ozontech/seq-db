package processor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/seq"
)

type stubIDsIndex struct {
	ids      []seq.ID
	lidsRead int
}

func (s *stubIDsIndex) LessOrEqual(lid seq.LID, id seq.ID) bool { panic("not used") }
func (s *stubIDsIndex) GetMID(lid seq.LID) seq.MID              { return s.ids[lid].MID }
func (s *stubIDsIndex) GetRID(lid seq.LID) seq.RID              { return s.ids[lid].RID }
func (s *stubIDsIndex) Len() int                                { return len(s.ids) }

func (s *stubIDsIndex) GetMIDs(lids []node.LID, out []seq.MID) []seq.MID {
	s.lidsRead += len(lids)
	for _, lid := range lids {
		out = append(out, s.ids[lid.Unpack()].MID)
	}
	return out
}

func (s *stubIDsIndex) GetRIDs(lids []node.LID, out []seq.RID) []seq.RID {
	for _, lid := range lids {
		out = append(out, s.ids[lid.Unpack()].RID)
	}
	return out
}

func TestIterateEvalTreeDuplicateIDsAtBatchBoundary(t *testing.T) {
	// LID 0 is unused; IDs descend as LID grows; LIDs 1 and 2 share one seq.ID.
	idx := &stubIDsIndex{ids: []seq.ID{
		1: {MID: 600, RID: 1},
		2: {MID: 600, RID: 1}, // duplicate of LID 1
		3: {MID: 500, RID: 1},
		4: {MID: 400, RID: 1},
		5: {MID: 300, RID: 1},
		6: {MID: 200, RID: 1},
	}}

	// Single batch with 6 LIDs, longer than the limit.
	evalTree := node.NewStaticBatched([]uint32{1, 2, 3, 4, 5, 6}, seq.DocsOrderDesc.IsDesc())

	params := SearchParams{
		Limit: 3,
		Order: seq.DocsOrderDesc,
	}

	total, ids, _, _, err := iterateEvalTree(context.Background(), params, idx, evalTree, nil, 1, 6, stopwatch.New())
	require.NoError(t, err)

	// 6 LIDs exist and the limit is not yet satisfied after the duplicate,
	// so the search must keep scanning: expected IDs are 600, 500, 400.
	got := make([]seq.MID, 0, len(ids))
	for _, id := range ids {
		got = append(got, id.ID.MID)
	}
	require.Equal(t, []seq.MID{600, 500, 400}, got)
	require.Equal(t, 4, total) // 4 LIDs scanned to produce 3 distinct IDs
}

func TestIterateEvalTreeBatchTotal(t *testing.T) {
	tests := []struct {
		name  string
		build func(bool) node.BatchedNode
		lids  []uint32
	}{
		{
			name: "range",
			build: func(asc bool) node.BatchedNode {
				return node.NewRangeBatched(1, 6, asc)
			},
			lids: []uint32{1, 2, 3, 4, 5, 6},
		},
		{
			name: "slice",
			build: func(asc bool) node.BatchedNode {
				return node.NewStaticBatched([]uint32{1, 2, 3, 4, 5, 6}, asc)
			},
			lids: []uint32{1, 2, 3, 4, 5, 6},
		},
		{
			name: "and",
			build: func(asc bool) node.BatchedNode {
				return node.NewAndBatched(node.NewRangeBatched(1, 6, asc),
					node.NewStaticBatched([]uint32{2, 4, 5}, asc), asc)
			},
			lids: []uint32{2, 4, 5},
		},
		{
			name: "or",
			build: func(asc bool) node.BatchedNode {
				return node.NewOrBatched(node.NewRangeBatched(2, 5, asc),
					node.NewStaticBatched([]uint32{1, 3, 6}, asc), asc)
			},
			lids: []uint32{1, 2, 3, 4, 5, 6},
		},
	}

	for _, tt := range tests {
		for _, order := range []seq.DocsOrder{seq.DocsOrderDesc, seq.DocsOrderAsc} {
			t.Run(tt.name+"/"+order.String(), func(t *testing.T) {
				idx := &stubIDsIndex{ids: []seq.ID{
					1: {MID: 600}, 2: {MID: 500}, 3: {MID: 400},
					4: {MID: 300}, 5: {MID: 200}, 6: {MID: 100},
				}}
				params := SearchParams{Limit: 1, WithTotal: true, Order: order}
				total, ids, _, _, err := iterateEvalTree(context.Background(), params, idx,
					tt.build(order.IsDesc()), nil, 1, 6, stopwatch.New())
				require.NoError(t, err)
				require.Equal(t, len(tt.lids), total)
				require.Len(t, ids, 1)
				wantLID := tt.lids[0]
				if order.IsReverse() {
					wantLID = tt.lids[len(tt.lids)-1]
				}
				require.Equal(t, idx.ids[wantLID], ids[0].ID)
				require.Equal(t, 1, idx.lidsRead)
			})
		}
	}
}

func TestIterateEvalTreeCountOnly(t *testing.T) {
	for _, downsample := range []uint32{0, 1, 2} {
		params := SearchParams{WithTotal: true, Downsample: downsample}
		total, ids, _, _, err := iterateEvalTree(context.Background(), params, &stubIDsIndex{},
			node.NewRangeBatched(1, 6, true), nil, 1, 6, stopwatch.New())
		require.NoError(t, err)
		require.Equal(t, 6, total)
		require.Empty(t, ids)
	}
}
