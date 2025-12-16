package active2

import (
	"bytes"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

func mergeIndexes(indexes []*memIndex) *memIndex {
	docsCount := 0
	blocksCount := 0
	fieldsCount := 0
	docsSize := uint64(0)
	for _, index := range indexes {
		docsSize += index.docsSize
		docsCount += len(index.ids)
		fieldsCount += len(index.fields)
		blocksCount += len(index.blocksOffsets)
	}

	dst := &memIndex{
		ids:           make([]seq.ID, 0, docsCount),
		positions:     make([]seq.DocPos, 0, docsCount),
		fieldsTokens:  make(map[string]tokenRange, fieldsCount),
		blocksOffsets: make([]uint64, 0, blocksCount),
		docsSize:      docsSize,
		docsCount:     uint32(docsCount),
	}

	posMap := mergeBlocksOffsets(dst, indexes)
	lidsMap := mergeIDs(dst, indexes, posMap)
	mergeTokens(dst, indexes, lidsMap)

	dst.allTID = dst.fieldsTokens[seq.TokenAll].start

	// todo
	// if len(doubles) > 0 {
	// 	dst.docsCount = uint32(len(doubles))
	// 	logger.Warn("there are duplicate IDs when compaction", zap.Int("doubles", len(doubles)))
	// }

	return dst
}

func mergeIDs(dst *memIndex, indexes []*memIndex, posMap [][]seq.DocPos) [][]uint32 {
	// todo doubles := []seq.ID{}

	lidsMap := make([][]uint32, len(indexes))
	iters := make([]IOrderedIterator[IDIteratorItem], len(indexes))
	for i, idx := range indexes {
		iters[i] = &IDIterator{
			i:      i,
			idx:    idx,
			posMap: posMap[i],
		}
		lidsMap[i] = make([]uint32, 0, len(idx.ids))
	}

	orderedIDs := MergeKSortIterators(iters, func(a, b IDIteratorItem) int { return seq.Compare(b.id, a.id) })

	cur, has := orderedIDs.Next()

	for has {
		dst.ids = append(dst.ids, cur.id)
		dst.positions = append(dst.positions, cur.pos)
		lid := uint32(len(dst.ids))
		lidsMap[cur.i] = append(lidsMap[cur.i], lid)
		cur, has = orderedIDs.Next()
	}
	return lidsMap
}

func mergeTokens(dst *memIndex, indexes []*memIndex, lidsMap [][]uint32) {
	allCount := 0
	totalTokens := 0
	totalLIDsSize := 0
	TokensIterators := make([]IOrderedIterator[TokenIteratorItem], len(indexes))
	for i, index := range indexes {
		allCount += len(index.ids)
		TokensIterators[i] = NewTokenIterator(index, lidsMap[i])
		totalTokens += len(index.tokens)
		for _, lids := range index.tokenLIDs {
			totalLIDsSize += len(lids)
		}
	}

	cmpToken := func(a, b TokenIteratorItem) int {
		r := bytes.Compare(a.Field(), b.Field())
		if r == 0 {
			return bytes.Compare(a.Token(), b.Token())
		}
		return r
	}

	orderedTokens := MergeKSortIterators(TokensIterators, cmpToken)

	uniqTokensSize := 0
	uniqTokensCount := 0

	uniqFieldsSize := 0
	uniqFieldsCount := 0

	var (
		prevField []byte
		prevToken TokenIteratorItem
	)

	borders := make([]uint8, 0, totalTokens)
	items := make([]TokenIteratorItem, 0, totalTokens)

	for cur, has := orderedTokens.Next(); has; cur, has = orderedTokens.Next() {
		var border uint8

		if prevToken.payload == nil || cmpToken(prevToken, cur) != 0 {
			uniqTokensCount++
			uniqTokensSize += len(cur.Token())
			border++

			field := cur.Field()
			if !bytes.Equal(prevField, field) {
				uniqFieldsCount++
				uniqFieldsSize += len(field)
				border++
				prevField = field
			}
		}

		borders = append(borders, border)
		items = append(items, cur)
		prevToken = cur
	}

	dst.fields = make([][]byte, 0, uniqFieldsCount)
	dst.tokens = make([][]byte, 0, uniqTokensCount)
	dst.tokenLIDs = make([][]uint32, 0, uniqTokensCount)

	allTokens := make([]byte, 0, uniqTokensSize)
	allFields := make([]byte, 0, uniqFieldsSize)
	tokenRanges := make([]tokenRange, 0, uniqFieldsCount)

	var isAllToken bool
	lidsCollector := NewLIDsCollector(totalLIDsSize, allCount)

	for i, item := range items {
		if borders[i] > 0 {

			if i > 0 {
				dst.tokenLIDs = append(dst.tokenLIDs, lidsCollector.GetSorted())
			}

			if borders[i] > 1 {

				tid := uint32(len(dst.tokens))

				if i > 0 {
					fieldStr := util.ByteToStringUnsafe(dst.fields[len(dst.fields)-1])
					tr := dst.fieldsTokens[fieldStr]
					tr.count = tid - tr.start
					dst.fieldsTokens[fieldStr] = tr
				}

				start := len(allFields)
				allFields = append(allFields, item.Field()...)
				field := allFields[start:]
				dst.fields = append(dst.fields, field)

				tokenRanges = append(tokenRanges, tokenRange{start: tid})
				fieldStr := util.ByteToStringUnsafe(field)
				tr := tokenRanges[len(tokenRanges)-1]
				dst.fieldsTokens[fieldStr] = tr

				isAllToken = fieldStr == seq.TokenAll
			}

			start := len(allTokens)
			allTokens = append(allTokens, item.Token()...)
			dst.tokens = append(dst.tokens, allTokens[start:])
		}

		if isAllToken {
			for range item.LIDs() {
				lidsCollector.Add(0)
			}
		} else {
			lidsMap := item.lidsMap()
			for _, oldLID := range item.LIDs() {
				newLID := lidsMap[oldLID-1]
				lidsCollector.Add(newLID)
			}
		}
	}

	dst.tokenLIDs = append(dst.tokenLIDs, lidsCollector.GetSorted())

	tid := uint32(len(dst.tokens)) - 1
	fieldStr := util.ByteToStringUnsafe(dst.fields[len(dst.fields)-1])
	tr := dst.fieldsTokens[fieldStr]
	tr.count = tid - tr.start + 1
	dst.fieldsTokens[fieldStr] = tr

}

type LIDsCollector struct {
	all    []uint32
	lids   []uint32
	offset int
	bitmap *roaring.Bitmap
}

func NewLIDsCollector(size, allCount int) *LIDsCollector {
	ls := &LIDsCollector{
		lids:   make([]uint32, 0, size),
		all:    make([]uint32, allCount),
		bitmap: roaring.New(),
	}
	for i := range allCount {
		ls.all[i] = uint32(i) + 1
	}
	return ls
}

func (s *LIDsCollector) Add(lid uint32) {
	s.lids = append(s.lids, lid)
}

func (s *LIDsCollector) GetSorted() (dst []uint32) {
	dst = s.lids[s.offset:]

	if len(dst) == len(s.all) {
		dst = s.all
		s.lids = s.lids[:s.offset]
		return dst
	}

	if len(dst) > 64_000 {
		s.bitmap.AddMany(dst)
		s.bitmap.ToExistingArray(&dst)
		s.bitmap.Clear()
		s.offset = len(s.lids)
		return dst
	}

	slices.Sort(dst)
	s.offset = len(s.lids)
	return dst
}

func mergeBlocksOffsets(dst *memIndex, indexes []*memIndex) [][]seq.DocPos {
	var offset uint32
	positions := make([][]seq.DocPos, len(indexes))
	for i, index := range indexes {
		dst.blocksOffsets = append(dst.blocksOffsets, index.blocksOffsets...)
		positions[i] = make([]seq.DocPos, 0, len(index.positions))
		for _, p := range index.positions {
			oldIdx, docOffset := p.Unpack()
			positions[i] = append(positions[i], seq.PackDocPos(oldIdx+offset, docOffset))
		}
		offset += uint32(len(index.blocksOffsets))
	}
	return positions
}
