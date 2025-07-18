package sealing

import (
	"iter"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/seq"
)

type mockSource struct {
	info          common.Info
	tokens        [][]byte
	fields        []string
	fieldMaxTIDs  []uint32
	ids           []seq.ID
	pos           []seq.DocPos
	tokenLIDs     [][]uint32
	blocksOffsets []uint64
	lastError     error
}

func (m *mockSource) Info() common.Info { return m.info }

func (m *mockSource) Fields() iter.Seq2[string, uint32] {
	return func(yield func(string, uint32) bool) {
		for i := range len(m.fields) {
			if !yield(m.fields[i], m.fieldMaxTIDs[i]) {
				return
			}
		}
	}
}

func (m *mockSource) IDsBlocks(size int) iter.Seq2[[]seq.ID, []seq.DocPos] {
	return func(yield func([]seq.ID, []seq.DocPos) bool) {
		ids := make([]seq.ID, 0, size)
		pos := make([]seq.DocPos, 0, size)
		for i, id := range m.ids {
			if len(ids) == size {
				if !yield(ids, pos) {
					return
				}
				ids = ids[:0]
				pos = pos[:0]
			}
			ids = append(ids, id)
			pos = append(pos, m.pos[i])
		}
		yield(ids, pos)
	}
}

func (m *mockSource) TokenBlocks(size int) iter.Seq[[][]byte] {
	return func(yield func([][]byte) bool) {
		block := [][]byte{}
		blockSize := 0
		for _, token := range m.tokens {
			if blockSize >= size {
				if !yield(block) {
					return
				}
				blockSize = 0
				block = block[:0]
			}
			block = append(block, token)
			blockSize += len(token) + 4
		}
		yield(block)
	}
}

func (m *mockSource) TokenLIDs() iter.Seq[[]uint32] {
	return func(yield func([]uint32) bool) {
		for _, lids := range m.tokenLIDs {
			if !yield(lids) {
				return
			}
		}
	}
}

func (m *mockSource) BlocksOffsets() []uint64 { return m.blocksOffsets }
func (m *mockSource) LastError() error        { return m.lastError }

func TestBlocksBuilder_TokensBlocksWithTokenTable(t *testing.T) {
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
	}

	// Block size in bytes.
	const blockSize = 24

	bb := blocksBuilder{}
	tokenBlocks, tokenTableBlocks := bb.TokensBlocksWithTokenTable(src.TokenBlocks(blockSize), src.Fields())

	// In our test case, each token is 4 bytes long. Also for each token we use uint32 to encode the length.
	// So 3 tokens take up exactly 24 bytes. And we expect all token blocks to contain 3 tokens except the last one.
	expectedSizes := []int{3, 3, 3, 3, 2}

	tid := 0
	blockIndex := 0
	for block := range tokenBlocks {
		assert.Equal(t, expectedSizes[blockIndex], block.payload.Len())
		for i := range block.payload.Len() {
			tid++
			assert.Equal(t, src.tokens[tid-1], block.payload.GetToken(i))
		}
		blockIndex++
	}

	assert.Equal(t, tid, len(src.tokens))

	actualTokenTable := tokenTableBlocks()
	expectedTokenTable := token.TableBlock{
		FieldsTables: []token.FieldTable{
			{
				Field: "f1",
				Entries: []*token.TableEntry{
					{
						StartIndex: 0,
						StartTID:   1,
						BlockIndex: 1,
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
						BlockIndex: 1,
						ValCount:   1,
						MinVal:     "f2v1",
						MaxVal:     "f2v1",
					}, {
						StartIndex: 0,
						StartTID:   4,
						BlockIndex: 2,
						ValCount:   3,
						MinVal:     "f2v2",
						MaxVal:     "f2v4",
					}, {
						StartIndex: 0,
						StartTID:   7,
						BlockIndex: 3,
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
						BlockIndex: 3,
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
						BlockIndex: 4,
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
						BlockIndex: 5,
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
						BlockIndex: 5,
						ValCount:   1,
						MinVal:     "f6v1",
						MaxVal:     "f6v1",
					},
				},
			},
		},
	}
	assert.Equal(t, actualTokenTable.FieldsTables, expectedTokenTable.FieldsTables)
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
	for block := range makeIDsSealBlocks(src.IDsBlocks(3)) {
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

func TestBlocksBuilder_LIDsBlocks(t *testing.T) {
	src := mockSource{
		tokenLIDs: [][]uint32{
			{
				10, // block 1, tid 1
				20, // block 1, tid 1
				30, // block 1, tid 1

				40, // block 2, tid 1
			}, {
				11, // block 2, tid 2
				21, // block 2, tid 2

				31, // block 3, tid 2
				41, // block 3, tid 2
			}, {
				10, // block 3, tid 3

				11, // block 4, tid 3
				20, // block 4, tid 3
				21, // block 4, tid 3

			}, {
				30, // block 5, tid 4
				40, // block 5, tid 4
				50, // block 5, tid 4

				60, // block 6, tid 4
			},
		},
	}

	expected := []lidsSealBlock{{
		ext: lidsExt{
			minTID:      1,
			maxTID:      1,
			isContinued: false,
		},
		payload: lids.Block{
			LIDs:      []uint32{10, 20, 30},
			Offsets:   []uint32{0, 3},
			IsLastLID: false,
		},
	}, {
		ext: lidsExt{
			minTID:      1,
			maxTID:      2,
			isContinued: true,
		},
		payload: lids.Block{
			LIDs:      []uint32{40, 11, 21},
			Offsets:   []uint32{0, 1, 3},
			IsLastLID: false,
		},
	}, {
		ext: lidsExt{
			minTID:      2,
			maxTID:      3,
			isContinued: true,
		},
		payload: lids.Block{
			LIDs:      []uint32{31, 41, 10},
			Offsets:   []uint32{0, 2, 3},
			IsLastLID: false,
		},
	}, {
		ext: lidsExt{
			minTID:      3,
			maxTID:      3,
			isContinued: true,
		},
		payload: lids.Block{
			LIDs:      []uint32{11, 20, 21},
			Offsets:   []uint32{0, 3},
			IsLastLID: true,
		},
	}, {
		ext: lidsExt{
			minTID:      4,
			maxTID:      4,
			isContinued: false,
		},
		payload: lids.Block{
			LIDs:      []uint32{30, 40, 50},
			Offsets:   []uint32{0, 3},
			IsLastLID: false,
		},
	}, {
		ext: lidsExt{
			minTID:      4,
			maxTID:      4,
			isContinued: true,
		},
		payload: lids.Block{
			LIDs:      []uint32{60},
			Offsets:   []uint32{0, 1},
			IsLastLID: true,
		}},
	}
	bb := blocksBuilder{}
	blocks := []lidsSealBlock{}
	for block := range bb.LIDsBlocks(src.TokenLIDs(), 3) {
		block.payload.LIDs = slices.Clone(block.payload.LIDs)       // copy lids
		block.payload.Offsets = slices.Clone(block.payload.Offsets) // copy offsets
		blocks = append(blocks, block)
	}
	assert.Equal(t, expected, blocks)
}
