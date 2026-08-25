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
	ids []seq.ID
}

func (s *stubIDsIndex) LessOrEqual(lid seq.LID, id seq.ID) bool { panic("not used") }
func (s *stubIDsIndex) GetMID(lid seq.LID) seq.MID              { return s.ids[lid].MID }
func (s *stubIDsIndex) GetRID(lid seq.LID) seq.RID              { return s.ids[lid].RID }
func (s *stubIDsIndex) Len() int                                { return len(s.ids) }

func (s *stubIDsIndex) GetMIDs(lids []node.LID, out []seq.MID) []seq.MID {
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
	evalTree := node.NewStaticBatched([]uint32{1, 2, 3, 4, 5, 6}, seq.DocsOrderDesc.IsReverse())

	params := SearchParams{
		Limit: 3,
		Order: seq.DocsOrderDesc,
	}

	total, ids, _, _, err := iterateEvalTree(context.Background(), params, idx, evalTree, nil, stopwatch.New())
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
