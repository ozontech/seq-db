package active2

import (
	"bytes"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
	"go.uber.org/zap"
)

func mergeIndexes(indexes []*memIndex) *memIndex {
	docsCount := 0
	blocksCount := 0
	fieldsCount := 0
	docsSize := uint64(0)
	iterators := make([]*mergeIterator, 0, len(indexes))
	for _, index := range indexes {
		docsSize += index.docsSize
		docsCount += len(index.ids)
		fieldsCount += len(index.fields)
		blocksCount += len(index.blocksOffsets)
		iterators = append(iterators, newIndexIterator(index))
	}

	dst := &memIndex{
		ids:           make([]seq.ID, 0, docsCount),
		positions:     make([]seq.DocPos, 0, docsCount),
		idToLID:       make(map[seq.ID]uint32, docsCount),
		fieldsTokens:  make(map[string]tokenRange, fieldsCount),
		blocksOffsets: make([]uint64, 0, blocksCount),
		docsSize:      docsSize,
		docsCount:     uint32(docsCount),
	}

	mergeBlocksOffsets(dst, iterators)

	doubles := mergeIDs(dst, iterators)
	mergeTokens(dst, iterators)

	dst.allTID = dst.fieldsTokens[seq.TokenAll].start

	if len(doubles) > 0 {
		dst.docsCount = uint32(len(doubles))
		logger.Warn("there are duplicate IDs when compaction", zap.Int("doubles", len(doubles)))
	}

	return dst
}

func mergeIDs(dst *memIndex, orig []*mergeIterator) []seq.ID {
	doubles := []seq.ID{}
	iterators := append([]*mergeIterator{}, orig...) // make copy

	for len(iterators) > 0 {
		// try select first
		selected := []int{0}
		maxID := iterators[0].CurrentID()

		for i := 1; i < len(iterators); i++ {
			if cur := iterators[i].CurrentID(); cur == maxID {
				selected = append(selected, i)
			} else if seq.Less(maxID, cur) {
				maxID = cur
				selected = []int{i}
			}
		}

		lid := uint32(len(dst.ids)) + 1
		dst.ids = append(dst.ids, maxID)
		dst.idToLID[maxID] = lid
		dst.positions = append(dst.positions, iterators[selected[0]].CurrentPos())

		k := 0
		for _, i := range selected {
			iterators[i-k].AddNewLID(lid)
			if !iterators[i-k].ShiftID() {
				iterators = removeItem(iterators, i-k)
				k++
			}
		}

		if len(selected) > 1 {
			doubles = append(doubles, maxID)
		}
	}
	return doubles
}

func mergeTokens(dst *memIndex, orig []*mergeIterator) {
	// todo copy tokens to compact mem usage
	// todo allocate for all lids at once to optimize allocations
	var prevField []byte
	iterators := append([]*mergeIterator{}, orig...) // make copy
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

		k := 0
		lids := make([][]uint32, 0, len(selected))
		for _, i := range selected {
			lids = append(lids, iterators[i-k].CurrentTokenLIDs())
			if !iterators[i-k].ShiftToken() {
				iterators = removeItem(iterators, i-k)
				k++
			}
		}

		if !bytes.Equal(prevField, minToken.Key) { // new field
			if tr, ok := dst.fieldsTokens[string(prevField)]; ok {
				tr.count = uint32(len(dst.tokens)) - tr.start
				dst.fieldsTokens[string(prevField)] = tr
			}
			dst.fields = append(dst.fields, minToken.Key)
			dst.fieldsTokens[string(minToken.Key)] = tokenRange{start: uint32(len(dst.tokens))}
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

func mergeBlocksOffsets(dst *memIndex, src []*mergeIterator) {
	var offset uint32
	for _, it := range src {
		for _, offset := range it.index.blocksOffsets {
			dst.blocksOffsets = append(dst.blocksOffsets, offset)
		}
		for _, p := range it.index.positions {
			oldIdx, docOffset := p.Unpack()
			it.AddPos(seq.PackDocPos(oldIdx+offset, docOffset))
		}
		offset += uint32(len(it.index.blocksOffsets))
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

		k := 0
		for _, i := range selected {
			lids[i-k] = lids[i-k][1:]
			if len(lids[i-k]) == 0 {
				lids = removeItem(lids, i-k)
				k++
			}
		}
	}

	return res
}

func removeItem[V any](items []V, i int) []V {
	k := 0
	for j, v := range items {
		if i == j {
			continue
		}
		items[k] = v
		k++
	}
	items = items[:k]
	return items
}
