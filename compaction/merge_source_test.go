package compaction

import (
	"iter"
	"slices"
	"testing"

	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/seq"
	"github.com/stretchr/testify/require"
)

// mockSealingSource is a test implementation of sealing.Source.
//
// IDs must be provided in descending order (MID DESC, RID DESC); the mock
// automatically prepends the system ID when iterating, matching the contract
// expected by MergeSource.ID().
//
// Fields maps field name → token value → list of 1-based LIDs.
// Fields and tokens are yielded in sorted order.
type mockSealingSource struct {
	ids    []seq.ID
	pos    []seq.DocPos
	blocks []uint64
	// docsOnDisk is the total compressed size of the .docs file,
	// used by MergeSource to adjust block offsets across sources.
	docsOnDisk uint64
	// fields maps field → token → lids (1-based).
	fields map[string]map[string][]uint32
}

func (m *mockSealingSource) Info() *common.Info {
	return &common.Info{
		DocsTotal:  uint32(len(m.ids)),
		DocsOnDisk: m.docsOnDisk,
	}
}

func (m *mockSealingSource) BlockOffsets() []uint64 {
	return m.blocks
}

func (m *mockSealingSource) ID() iter.Seq2[seq.ID, seq.DocPos] {
	return func(yield func(seq.ID, seq.DocPos) bool) {
		if !yield(seq.SystemID, seq.SystemDocPos) {
			return
		}
		for i, id := range m.ids {
			if !yield(id, m.pos[i]) {
				return
			}
		}
	}
}

func (m *mockSealingSource) TokenTriplet() iter.Seq2[string, iter.Seq2[[]byte, []uint32]] {
	fieldNames := make([]string, 0, len(m.fields))
	for f := range m.fields {
		fieldNames = append(fieldNames, f)
	}
	slices.Sort(fieldNames)

	return func(yield func(string, iter.Seq2[[]byte, []uint32]) bool) {
		for _, field := range fieldNames {
			tokens := make([]string, 0, len(m.fields[field]))
			for t := range m.fields[field] {
				tokens = append(tokens, t)
			}
			slices.Sort(tokens)

			if !yield(field, func(yield func([]byte, []uint32) bool) {
				for _, tok := range tokens {
					if !yield([]byte(tok), m.fields[field][tok]) {
						return
					}
				}
			}) {
				return
			}
		}
	}
}

func (m *mockSealingSource) DocBlock() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		if !yield(nil) {
			return
		}
	}
}

func (m *mockSealingSource) LastError() error {
	return nil
}

func TestMergeSource(t *testing.T) {
	first := &mockSealingSource{
		ids: []seq.ID{
			{MID: 3},
			{MID: 2},
			{MID: 1},
		},

		pos: []seq.DocPos{
			seq.PackDocPos(0, 0),
			seq.PackDocPos(0, 1024),
			seq.PackDocPos(0, 2048),
		},

		fields: map[string]map[string][]uint32{
			"level": {
				"error": {1, 3},
				"info":  {2, 3},
			},
		},

		blocks:     []uint64{0},
		docsOnDisk: 1024,
	}

	second := &mockSealingSource{
		ids: []seq.ID{
			{MID: 6},
			{MID: 5},
		},

		pos: []seq.DocPos{
			seq.PackDocPos(0, 0),
			seq.PackDocPos(0, 2048),
		},

		fields: map[string]map[string][]uint32{
			"level": {
				"debug": {1},
				"info":  {2},
			},
		},

		blocks:     []uint64{0},
		docsOnDisk: 2048,
	}

	source := NewMergeSource("inmemory", []Source{first, second})

	{
		// Validate correctness of [storage.DocBlock] calculation.
		offsets := source.BlockOffsets()
		require.Equal(t, []uint64{0, 1024}, offsets)
	}

	{
		var (
			ids    []seq.ID
			docpos []seq.DocPos
		)

		for id, dp := range source.ID() {
			ids = append(ids, id)
			docpos = append(docpos, dp)
		}

		require.Equal(t,
			[]seq.ID{
				seq.SystemID,
				// seq.ID from the second source
				{MID: 6},
				{MID: 5},
				// seq.ID from the first source
				{MID: 3},
				{MID: 2},
				{MID: 1},
			},
			ids,
		)

		require.Equal(t,
			[]seq.DocPos{
				seq.SystemDocPos,
				// seq.DocPos from the second source
				seq.PackDocPos(1, 0), seq.PackDocPos(1, 2048),
				// seq.DocPos from the first source
				seq.PackDocPos(0, 0), seq.PackDocPos(0, 1024), seq.PackDocPos(0, 2048),
			},
			docpos,
		)
	}

	{
		var (
			fields []string
			tokens [][]byte
			lids   [][]uint32
		)

		for field, fieldIt := range source.TokenTriplet() {
			fields = append(fields, field)

			for token, lidsbuf := range fieldIt {
				tokens = append(tokens, token)
				lids = append(lids, slices.Clone(lidsbuf))
			}
		}

		// Both sources have the same and the only field
		require.Equal(t, []string{"level"}, fields)

		// Ensure tokens are sorted in ascending order
		require.Equal(t,
			[][]byte{[]byte("debug"), []byte("error"), []byte("info")},
			tokens,
		)

		// Ensure correctness of lids remapping
		// 	-----------------
		// 	seq.MID 6 5 3 2 1
		// 	seq.LID 1 2 3 4 5
		// 	-----------------
		require.Equal(t,
			[][]uint32{
				// Sequence of [seq.LID] for token `debug`
				{1},
				// Sequence of [seq.LID] for token `error`
				{3, 5},
				// Sequence of [seq.LID] for token `info`
				{2, 4, 5},
			},
			lids,
		)
	}
}
