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
	docsSize := uint64(0)
	for _, index := range indexes {
		docsSize += index.docsSize
		docsCount += len(index.ids)
		blocksCount += len(index.blocksOffsets)
	}

	res, release := AcquireResources()
	defer release()

	dst := newMemIndex()
	dst.docsCount = uint32(docsCount)
	dst.ids = dst.res.AllocIDs(docsCount)[:0]
	dst.positions = dst.res.AllocDocPos(docsCount)[:0]
	dst.blocksOffsets = dst.res.AllocUint64s(blocksCount)[:0]
	dst.docsSize = docsSize

	posMap := mergeBlocksOffsets(dst, res, indexes)
	lidsMap := mergeIDs(dst, res, indexes, posMap)
	mergeTokens(dst, res, indexes, lidsMap)

	dst.allTID = dst.fieldsTokens[seq.TokenAll].start

	return dst
}

func mergeIDs(dst *memIndex, res *Resources, indexes []*memIndex, posMap [][]seq.DocPos) [][]uint32 {
	lidsMap := res.AllocUint32Slices(len(indexes))
	iters := make([]IOrderedIterator[IDIteratorItem], len(indexes))
	for i, idx := range indexes {
		iters[i] = &IDIterator{
			i:      i,
			idx:    idx,
			posMap: posMap[i],
		}
		lidsMap[i] = res.uint32s.AllocSlice(int(idx.docsCount))[:0]
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

func mergeTokens(dst *memIndex, res *Resources, indexes []*memIndex, lidsMap [][]uint32) {
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

	borders := res.AllocBytes(totalTokens)[:0]
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

	dst.fieldsTokens = make(map[string]tokenRange, uniqFieldsCount)
	dst.fields = dst.res.AllocBytesSlices(uniqFieldsCount)[:0]
	dst.tokens = dst.res.AllocBytesSlices(uniqTokensCount)[:0]
	dst.tokenLIDs = dst.res.AllocUint32Slices(uniqTokensCount)[:0]

	allTokens := dst.res.AllocBytes(uniqTokensSize)[:0]
	allFields := dst.res.AllocBytes(uniqFieldsSize)[:0]
	allTokenLIDs := dst.res.AllocUint32s(totalLIDsSize)[:0]

	lidsCollector := NewLIDsCollector(allTokenLIDs, genAllLIDs(res, allCount))

	var isAllToken bool
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

				fieldStr := util.ByteToStringUnsafe(field)
				dst.fieldsTokens[fieldStr] = tokenRange{start: tid}

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

func genAllLIDs(res *Resources, s int) []uint32 {
	all := res.AllocUint32s(s)
	for i := range all {
		all[i] = uint32(i) + 1
	}
	return all
}

func NewLIDsCollector(allTokenLIDs, all []uint32) *LIDsCollector {
	return &LIDsCollector{
		lids:   allTokenLIDs[:0],
		all:    all,
		bitmap: roaring.New(),
	}
}

func (s *LIDsCollector) Add(lid uint32) {
	s.lids = append(s.lids, lid)
}

func (s *LIDsCollector) GetSorted() (dst []uint32) {
	dst = s.lids[s.offset:]

	if len(dst) == len(s.all) {
		s.lids = append(s.lids[:s.offset], s.all...)
		s.offset = len(s.lids)
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

func mergeBlocksOffsets(dst *memIndex, res *Resources, indexes []*memIndex) [][]seq.DocPos {
	var offset uint32
	positions := res.AllocDocPosSlices(len(indexes))
	for i, index := range indexes {
		dst.blocksOffsets = append(dst.blocksOffsets, index.blocksOffsets...)
		positions[i] = res.AllocDocPos(len(index.positions))[:0]
		for _, p := range index.positions {
			oldIdx, docOffset := p.Unpack()
			positions[i] = append(positions[i], seq.PackDocPos(oldIdx+offset, docOffset))
		}
		offset += uint32(len(index.blocksOffsets))
	}
	return positions
}
