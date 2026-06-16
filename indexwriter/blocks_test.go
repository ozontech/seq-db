package indexwriter

import (
	"iter"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/seq"
)

type mockSource struct {
	tokens       [][]byte
	fields       []string
	fieldMaxTIDs []uint32
	ids          []seq.ID
	pos          []seq.DocPos
	tokenLIDs    [][]uint32
}

func (m *mockSource) TokenTriplet() iter.Seq2[string, iter.Seq2[TokenPosting, error]] {
	return func(yield func(string, iter.Seq2[TokenPosting, error]) bool) {
		start := 0
		for i, field := range m.fields {
			end := int(m.fieldMaxTIDs[i])
			if !yield(field, m.tokensForField(start, end)) {
				return
			}
			start = end
		}
	}
}

func (m *mockSource) tokensForField(start, end int) iter.Seq2[TokenPosting, error] {
	return func(yield func(TokenPosting, error) bool) {
		for j := start; j < end; j++ {
			var lidsbuf []uint32
			if j < len(m.tokenLIDs) {
				lidsbuf = m.tokenLIDs[j]
			}
			if !yield(TokenPosting{First: m.tokens[j], Second: lidsbuf}, nil) {
				return
			}
		}
	}
}

func (m *mockSource) ID() iter.Seq2[DocLocation, error] {
	return func(yield func(DocLocation, error) bool) {
		for i, id := range m.ids {
			if !yield(DocLocation{First: id, Second: m.pos[i]}, nil) {
				return
			}
		}
	}
}

func TestBlocksBuilder_BuildTokenBlocks(t *testing.T) {
	src := mockSource{
		tokens: [][]byte{
			[]byte("f1v1"), // 1
			[]byte("f1v2"), // 2, max TID for f1
			[]byte("f2v1"), // 3

			[]byte("f2v2"), // 4
			[]byte("f2v3"), // 5
			[]byte("f2v4"), // 6

			[]byte("f2v5"), // 7, max TID for f2
			[]byte("f3v1"), // 8
			[]byte("f3v2"), // 9, max TID for f3

			[]byte("f4v1"), // 10
			[]byte("f4v2"), // 11
			[]byte("f4v3"), // 12, max TID for f4

			[]byte("f5v1"), // 13, max TID for f5
			[]byte("f6v1"), // 14, max TID for f6
		},
		fields:       []string{"f1", "f2", "f3", "f4", "f5", "f6"},
		fieldMaxTIDs: []uint32{2, 7, 9, 12, 13, 14},
		tokenLIDs: [][]uint32{
			{10, 20, 30, 40}, // 1
			{2},              // 2
			{3},              // 3
			{4},              // 4
			{5},              // 5
			{6},              // 6
			{7},              // 7
			{8},              // 8
			{9},              // 9
			{10},             // 10
			{11},             // 11
			{12},             // 12
			{13},             // 13
			{14},             // 14
		},
	}

	// Block size in bytes.
	const blockSize = 24
	const lidBlockCap = 3

	var lidBlocks []unpackedLIDBlock
	lidAccumulator := newLIDAccumulator(
		lidBlockCap,
		func(block unpackedLIDBlock) error {
			block.payload.LIDs = slices.Clone(block.payload.LIDs)
			block.payload.Offsets = slices.Clone(block.payload.Offsets)
			lidBlocks = append(lidBlocks, block)
			return nil
		},
	)

	tokenBlocksIter := tokenBlock(
		src.TokenTriplet(),
		lidAccumulator.add,
		blockSize,
	)

	// In our test case, each token is 4 bytes long. Also for each token we use uint32 to encode the length.
	// So 3 tokens take up exactly 24 bytes. And we expect all token blocks to contain 3 tokens except the last one.
	expectedSizes := []int{3, 3, 3, 3, 2}

	tid := 0
	blockIndex := 0

	allFieldsTables := []token.FieldTable{}
	for pair, err := range tokenBlocksIter {
		assert.NoError(t, err)
		block, fieldsTables := pair.First, pair.Second
		assert.Equal(t, expectedSizes[blockIndex], block.payload.Len())
		for i := range block.payload.Len() {
			tid++
			assert.Equal(t, src.tokens[tid-1], block.payload.GetToken(i))
		}
		allFieldsTables = append(allFieldsTables, fieldsTables...)
		blockIndex++
	}

	actualTokenTable := token.TableBlock{FieldsTables: collapseOrderedFieldsTables(allFieldsTables)}
	assert.Equal(t, tid, len(src.tokens))

	expectedTokenTable := token.TableBlock{
		FieldsTables: []token.FieldTable{
			{
				Field: "f1",
				Entries: []*token.TableEntry{
					{
						StartIndex: 0,
						StartTID:   1,
						BlockIndex: 0,
						ValCount:   2,
						MinVal:     "f1v1",
						MaxVal:     "f1v2",
					},
				},
			}, {
				Field: "f2",
				Entries: []*token.TableEntry{
					{
						StartIndex: 2,
						StartTID:   3,
						BlockIndex: 0,
						ValCount:   1,
						MinVal:     "f2v1",
						MaxVal:     "f2v1",
					}, {
						StartIndex: 0,
						StartTID:   4,
						BlockIndex: 1,
						ValCount:   3,
						MinVal:     "f2v2",
						MaxVal:     "f2v4",
					}, {
						StartIndex: 0,
						StartTID:   7,
						BlockIndex: 2,
						ValCount:   1,
						MinVal:     "f2v5",
						MaxVal:     "f2v5",
					},
				},
			}, {
				Field: "f3",
				Entries: []*token.TableEntry{
					{
						StartIndex: 1,
						StartTID:   8,
						BlockIndex: 2,
						ValCount:   2,
						MinVal:     "f3v1",
						MaxVal:     "f3v2",
					},
				},
			}, {
				Field: "f4",
				Entries: []*token.TableEntry{
					{
						StartIndex: 0,
						StartTID:   10,
						BlockIndex: 3,
						ValCount:   3,
						MinVal:     "f4v1",
						MaxVal:     "f4v3",
					},
				},
			}, {
				Field: "f5",
				Entries: []*token.TableEntry{
					{
						StartIndex: 0,
						StartTID:   13,
						BlockIndex: 4,
						ValCount:   1,
						MinVal:     "f5v1",
						MaxVal:     "f5v1",
					},
				},
			}, {
				Field: "f6",
				Entries: []*token.TableEntry{
					{
						StartIndex: 1,
						StartTID:   14,
						BlockIndex: 4,
						ValCount:   1,
						MinVal:     "f6v1",
						MaxVal:     "f6v1",
					},
				},
			},
		},
	}
	assert.Equal(t, actualTokenTable.FieldsTables, expectedTokenTable.FieldsTables)
	assert.NoError(t, lidAccumulator.finalize())

	expectedLIDBlocks := []unpackedLIDBlock{
		{
			ext:     lidExt{minTID: 1, maxTID: 1, isContinued: false},
			payload: lids.UnpackedBlock{LIDs: []uint32{10, 20, 30}, Offsets: []uint32{0, 3}, IsLastLID: false},
		},
		{
			ext:     lidExt{minTID: 1, maxTID: 3, isContinued: true},
			payload: lids.UnpackedBlock{LIDs: []uint32{40, 2, 3}, Offsets: []uint32{0, 1, 2, 3}, IsLastLID: true},
		},
		{
			ext:     lidExt{minTID: 4, maxTID: 6, isContinued: false},
			payload: lids.UnpackedBlock{LIDs: []uint32{4, 5, 6}, Offsets: []uint32{0, 1, 2, 3}, IsLastLID: true},
		},
		{
			ext:     lidExt{minTID: 7, maxTID: 9, isContinued: false},
			payload: lids.UnpackedBlock{LIDs: []uint32{7, 8, 9}, Offsets: []uint32{0, 1, 2, 3}, IsLastLID: true},
		},
		{
			ext:     lidExt{minTID: 10, maxTID: 12, isContinued: false},
			payload: lids.UnpackedBlock{LIDs: []uint32{10, 11, 12}, Offsets: []uint32{0, 1, 2, 3}, IsLastLID: true},
		},
		{
			ext:     lidExt{minTID: 13, maxTID: 14, isContinued: false},
			payload: lids.UnpackedBlock{LIDs: []uint32{13, 14}, Offsets: []uint32{0, 1, 2}, IsLastLID: true},
		},
	}
	assert.Equal(t, expectedLIDBlocks, lidBlocks)
}

func TestBlocksBuilder_IDsBlocks(t *testing.T) {
	src := mockSource{
		ids: []seq.ID{
			{MID: 8, RID: 1},
			{MID: 7, RID: 1},
			{MID: 6, RID: 1},

			{MID: 5, RID: 1},
			{MID: 4, RID: 1},
			{MID: 3, RID: 1},

			{MID: 2, RID: 1},
			{MID: 1, RID: 1},
		},
		pos: []seq.DocPos{
			seq.PackDocPos(1, 0),
			seq.PackDocPos(1, 10),
			seq.PackDocPos(2, 0),

			seq.PackDocPos(2, 10),
			seq.PackDocPos(2, 20),
			seq.PackDocPos(3, 0),

			seq.PackDocPos(4, 0),
			seq.PackDocPos(4, 10),
		},
	}

	expectedSizes := []int{3, 3, 2}

	i := 0
	ids := []seq.ID{}
	pos := []seq.DocPos{}
	for block, err := range idBlock(src.ID(), 3) {
		assert.NoError(t, err)

		assert.Equal(t, expectedSizes[i], len(block.mids.Values))
		assert.Equal(t, expectedSizes[i], len(block.rids.Values))
		assert.Equal(t, expectedSizes[i], len(block.params.Values))

		i++
		j := 0
		for _, mid := range block.mids.Values {
			ids = append(ids, seq.ID{MID: seq.MID(mid), RID: seq.RID(block.rids.Values[j])})
			pos = append(pos, seq.DocPos(block.params.Values[j]))
			j++
		}
	}

	assert.Equal(t, src.ids, ids)
	assert.Equal(t, src.pos, pos)
}
