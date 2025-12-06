package active2

/*
import (
	"bytes"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
	"go.uber.org/zap"
)

func mergeIndexes2(src1, src2 *memIndex) *memIndex {
	docsCount := src1.docsCount + src2.docsCount
	blocksCount := len(src1.blocksOffsets) + len(src2.blocksOffsets)
	docsSize := src1.docsSize + src2.docsSize
	fieldsCount := len(src1.fields) + len(src2.fields)

	dst := &memIndex{
		ids:           make([]seq.ID, 0, docsCount),
		positions:     make(map[seq.ID]seq.DocPos, docsCount),
		fieldsTokens:  make(map[string]tokensRange, fieldsCount),
		blocksOffsets: make([]uint64, 0, blocksCount),
		docsSize:      docsSize,
	}

	lids1, lids2, doubles := mergeIDs1(src1, src2, dst)

	mergeTokens(dst, iterators)
	mergePositions(dst, iterators)

	dst.docsCount = uint32(len(dst.ids))
	dst.allTID = dst.fieldsTokens[seq.TokenAll].start

	if len(doubles) > 0 {
		logger.Warn("there are duplicate IDs when compaction", zap.Int("doubles", len(doubles)))
	}

	return dst
}

func mergeIDs1(src1, src2, dst *memIndex) ([]uint32, []uint32, []seq.ID) {
	doubles := []seq.ID{}

	newLIDs1 := []uint32{}
	newLIDs2 := []uint32{}

	var i1, i2 int
	for i1 < len(src1.ids) && i2 < len(src2.ids) {
		lid := uint32(len(dst.ids))
		id1 := src1.ids[i1]
		id2 := src2.ids[i2]

		if seq.Less(id2, id1) {
			dst.ids = append(dst.ids, id1)
			newLIDs1 = append(newLIDs1, lid)
			i1++
		} else if seq.Less(id1, id2) {
			dst.ids = append(dst.ids, id2)
			newLIDs2 = append(newLIDs2, lid)
			i2++
		} else { // id2 == id1
			dst.ids = append(dst.ids, id1)
			doubles = append(doubles, id1)
			newLIDs1 = append(newLIDs1, lid)
			newLIDs2 = append(newLIDs2, lid)
			i1++
			i2++
		}
	}

	for ; i1 < len(src1.ids); i1++ {
		newLIDs1 = append(newLIDs1, uint32(len(dst.ids)))
		dst.ids = append(dst.ids, src1.ids[i1])
	}
	for ; i2 < len(src1.ids); i2++ {
		newLIDs2 = append(newLIDs2, uint32(len(dst.ids)))
		dst.ids = append(dst.ids, src2.ids[i2])
	}

	return newLIDs1, newLIDs2, doubles
}

func mergeTokens(dst *memIndex, orig []mergeIterator) {
	// todo copy tokens to compact mem usage
	// todo allocate for all lids at once to optimize allocations
	var prevField []byte
	iterators := append([]mergeIterator{}, orig...) // make copy
	for len(iterators) > 0 {
		// try select first
		selected := []int{0}
		minToken := iterators[0].CurrentToken()

		for i := 1; i < len(iterators); i++ {
			cur := iterators[i].CurrentToken()
			if cmp := compareMetaToken(cur, minToken); cmp < 0 {
				minToken = cur
				selected = []int{i}
			} else if cmp == 0 {
				selected = append(selected, i)
			}
		}

		lids := make([][]uint32, 0, len(selected))
		for _, i := range selected {
			lids = append(lids, iterators[i].CurrentTokenLIDs())
			if !iterators[i].ShiftToken() {
				removeItem(iterators, i)
			}
		}

		if !bytes.Equal(prevField, minToken.Key) { // new field
			if tr, ok := dst.fieldsTokens[string(prevField)]; ok {
				tr.count = uint32(len(dst.tokens)) - tr.start
				dst.fieldsTokens[string(prevField)] = tr
			}
			dst.fields = append(dst.fields, minToken.Key)
			dst.fieldsTokens[string(minToken.Key)] = tokensRange{start: uint32(len(dst.tokens))}
			prevField = minToken.Key
		}

		dst.tokens = append(dst.tokens, minToken.Value)
		dst.tokenLIDs = append(dst.tokenLIDs, mergeLIDs(lids))
	}
	if tr, ok := dst.fieldsTokens[string(prevField)]; ok {
		tr.count = uint32(len(dst.tokens)) - tr.start
		dst.fieldsTokens[string(prevField)] = tr
	}
}

func mergePositions(dst *memIndex, orig []mergeIterator) {
	iterators := append([]mergeIterator{}, orig...) // make copy
	for len(iterators) > 0 {
		// try select first
		selected := []int{0}
		minOffset := iterators[0].CurrentBlocksOffset()

		for i := 1; i < len(iterators); i++ {
			if cur := iterators[i].CurrentBlocksOffset(); cur == minOffset {
				selected = append(selected, i)
			} else if cur < minOffset {
				minOffset = cur
				selected = []int{i}
			}
		}

		newBlockIndex := len(dst.blocksOffsets)
		dst.blocksOffsets = append(dst.blocksOffsets, minOffset)

		for _, i := range selected {
			iterators[i].AddNewBlockIndex(newBlockIndex)
			if !iterators[i].ShiftBlocksOffset() {
				removeItem(iterators, i)
			}
		}
	}

	for _, iterator := range orig {
		iterator.RepackDocPositions(dst.positions)
	}
}

func compareMetaToken(mt1, mt2 tokenizer.MetaToken) int {
	res := bytes.Compare(mt1.Key, mt2.Key)
	if res == 0 {
		return bytes.Compare(mt1.Value, mt2.Value)
	}
	return res
}

func mergeLIDs(lids [][]uint32) []uint32 {
	size := 0
	for i := range lids {
		size += len(lids[i])
	}
	res := make([]uint32, 0, size)

	for len(lids) > 0 {
		// try select first
		selected := []int{0}
		minLID := lids[0][0]

		for i := 1; i < len(lids); i++ {
			cur := lids[i][0]
			if cur == minLID { // can be doubles
				selected = append(selected, i)
			} else if cur < minLID {
				selected = []int{i}
				minLID = cur
			}
		}

		res = append(res, minLID)

		for _, i := range selected {
			if lids[i] = lids[i][1:]; len(lids[i]) == 0 {
				removeItem(lids, i)
			}
		}
	}

	return res
}

func removeItem[V any](items []V, i int) []V {
	last := len(items) - 1
	items[i] = items[last]
	items = items[:last]
	return items
}
*/
