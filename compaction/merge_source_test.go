package compaction

import (
	"cmp"
	"fmt"
	"iter"
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/indexwriter"
	"github.com/ozontech/seq-db/seq"
)

type mockSealingSource struct {
	ids        []seq.ID
	pos        []seq.DocPos
	blocks     []uint64
	docsOnDisk uint64
	fields     map[string]map[string][]uint32
}

func (m *mockSealingSource) Info() *common.Info {
	return &common.Info{
		DocsRaw:    m.docsOnDisk,
		DocsTotal:  uint32(len(m.ids)),
		DocsOnDisk: m.docsOnDisk,

		From: slices.MinFunc(m.ids, func(x, y seq.ID) int {
			return cmp.Compare(x.MID, y.MID)
		}).MID,

		To: slices.MaxFunc(m.ids, func(x, y seq.ID) int {
			return cmp.Compare(x.MID, y.MID)
		}).MID,
	}
}

func (m *mockSealingSource) BlockOffsets() []uint64 {
	return m.blocks
}

func (m *mockSealingSource) IDs() iter.Seq2[indexwriter.DocLocation, error] {
	return func(yield func(indexwriter.DocLocation, error) bool) {
		docloc := indexwriter.DocLocation{First: seq.SystemID, Second: seq.SystemDocPos}
		if !yield(docloc, nil) {
			return
		}

		for i, id := range m.ids {
			docloc = indexwriter.DocLocation{First: id, Second: m.pos[i]}
			if !yield(docloc, nil) {
				return
			}
		}
	}
}

func (m *mockSealingSource) TokenTriplets() iter.Seq2[string, iter.Seq2[indexwriter.TokenLIDs, error]] {
	fields := make([]string, 0, len(m.fields))
	for f := range m.fields {
		fields = append(fields, f)
	}

	slices.Sort(fields)
	return func(yield func(string, iter.Seq2[indexwriter.TokenLIDs, error]) bool) {
		for _, field := range fields {
			if !yield(field, m.postingsForField(field)) {
				return
			}
		}
	}
}

func (m *mockSealingSource) postingsForField(field string) iter.Seq2[indexwriter.TokenLIDs, error] {
	return func(yield func(indexwriter.TokenLIDs, error) bool) {
		tokens := make([]string, 0, len(m.fields[field]))
		for t := range m.fields[field] {
			tokens = append(tokens, t)
		}

		slices.Sort(tokens)
		for _, tok := range tokens {
			posting := indexwriter.TokenLIDs{
				First:  []byte(tok),
				Second: m.fields[field][tok],
			}

			if !yield(posting, nil) {
				return
			}
		}
	}
}

func (m *mockSealingSource) DocBlocks() iter.Seq2[DocBlockLocation, error] {
	return func(yield func(DocBlockLocation, error) bool) {
		if !yield(DocBlockLocation{}, nil) {
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

	t.Run("offsets", func(t *testing.T) {
		// Validate correctness of [storage.DocBlock] calculation.
		offsets := source.BlockOffsets()
		require.Equal(t, []uint64{0, 1024}, offsets)
	})

	t.Run("ids", func(t *testing.T) {
		var (
			ids    []seq.ID
			docpos []seq.DocPos
		)

		for loc, err := range source.IDs() {
			require.NoError(t, err)
			ids = append(ids, loc.First)
			docpos = append(docpos, loc.Second)
		}

		require.Equal(t,
			[]seq.ID{
				seq.SystemID,
				// [seq.ID] from the second source.
				{MID: 6},
				{MID: 5},
				// [seq.ID] from the first source.
				{MID: 3},
				{MID: 2},
				{MID: 1},
			},
			ids,
		)

		require.Equal(t,
			[]seq.DocPos{
				seq.SystemDocPos,
				// [seq.DocPos] from the second source.
				seq.PackDocPos(1, 0), seq.PackDocPos(1, 2048),
				// [seq.DocPos] from the first source.
				seq.PackDocPos(0, 0), seq.PackDocPos(0, 1024), seq.PackDocPos(0, 2048),
			},
			docpos,
		)
	})

	t.Run("tokens-lids", func(t *testing.T) {
		var (
			fields []string
			tokens [][]byte
			lids   [][]uint32
		)

		for field, fieldIt := range source.TokenTriplets() {
			fields = append(fields, field)

			for posting, err := range fieldIt {
				require.NoError(t, err)
				tokens = append(tokens, posting.First)
				lids = append(lids, slices.Clone(posting.Second))
			}
		}

		// Both sources have the same and the only field.
		require.Equal(t, []string{"level"}, fields)

		// Ensure tokens are sorted in ascending order.
		require.Equal(t,
			[][]byte{[]byte("debug"), []byte("error"), []byte("info")},
			tokens,
		)

		// Ensure correctness of lids remapping:
		//  -------------------------
		//  seq.MID       6 5 | 3 2 1
		//  seq.LID (old) 1 2 | 1 2 3
		//  seq.LID (new) 1 2 | 3 4 5
		//  -------------------------
		require.Equal(t,
			[][]uint32{
				// Sequence of [seq.LID] for token `debug`.
				{1},
				// Sequence of [seq.LID] for token `error`.
				{3, 5},
				// Sequence of [seq.LID] for token `info`.
				{2, 4, 5},
			},
			lids,
		)
	})

	t.Run("info", func(t *testing.T) {
		merged := source.Info()
		finfo, sinfo := first.Info(), second.Info()

		// Validate correctness of fraction time-range.
		require.Equal(t, merged.From, min(finfo.From, sinfo.From))
		require.Equal(t, merged.To, max(finfo.To, sinfo.To))

		// Validate correctness of total documents of merged fractions.
		require.Equal(t, merged.DocsTotal, finfo.DocsTotal+sinfo.DocsTotal)
		require.Equal(t, merged.DocsOnDisk, finfo.DocsOnDisk+sinfo.DocsOnDisk)
		require.Equal(t, merged.DocsRaw, finfo.DocsRaw+sinfo.DocsRaw)

		// Validate correctness of distribution.
		require.NotNil(t, merged.Distribution)
		require.True(t, merged.IsIntersecting(finfo.From, finfo.To))
		require.True(t, merged.IsIntersecting(sinfo.From, sinfo.To))
		require.True(t, merged.IsIntersecting(min(finfo.From, sinfo.From), max(finfo.To, sinfo.To)))
	})
}

func BenchmarkMergeSource(b *testing.B) {
	const (
		numSources    = 4
		docsPerSource = 512_000

		// Total count of pairs of (field, token) will be
		// [numFields] * [numTokens].
		numFields = 512
		numTokens = 16384
	)

	rng := rand.New(rand.NewSource(42))

	fieldNames := make([]string, numFields)
	for i := range fieldNames {
		fieldNames[i] = fmt.Sprintf("field-%d", i)
	}

	tokenNames := make([]string, numTokens)
	for i := range tokenNames {
		tokenNames[i] = fmt.Sprintf("token-%d", i)
	}

	makeSource := func(midOffset seq.MID) Source {
		ids := make([]seq.ID, docsPerSource)
		pos := make([]seq.DocPos, docsPerSource)

		for j := range ids {
			// IDs must be in descending MID order within each source.
			ids[j] = seq.ID{MID: midOffset + seq.MID(docsPerSource-j)}
			pos[j] = seq.PackDocPos(0, uint64(j*64))
		}

		// Assign each lid to a random (field, token) pair from the vocabulary
		// so that total lids per source equals [docsPerSource].
		fields := make(map[string]map[string][]uint32)
		for lid := uint32(1); lid <= uint32(docsPerSource); lid++ {
			field := fieldNames[rng.Intn(numFields)]
			token := tokenNames[rng.Intn(numTokens)]

			if fields[field] == nil {
				fields[field] = make(map[string][]uint32)
			}

			fields[field][token] = append(fields[field][token], lid)
		}

		for _, tokens := range fields {
			for tok, lids := range tokens {
				slices.Sort(lids)
				tokens[tok] = lids
			}
		}

		return &mockSealingSource{
			ids:        ids,
			pos:        pos,
			blocks:     []uint64{0},
			docsOnDisk: docsPerSource * 64,
			fields:     fields,
		}
	}

	sources := make([]Source, numSources)
	for i := range sources {
		sources[i] = makeSource(seq.MID(i * docsPerSource))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		ms := NewMergeSource("bench", sources)

		ms.BlockOffsets()
		for range ms.IDs() {
		}

		for _, tokIt := range ms.TokenTriplets() {
			for range tokIt {
			}
		}
	}
}
