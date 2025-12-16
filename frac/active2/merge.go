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

	newPositions := mergeBlocksOffsets(dst, indexes)

	reLIDs := mergeIDs(dst, indexes, newPositions)
	mergeTokens(dst, indexes, reLIDs)

	dst.allTID = dst.fieldsTokens[seq.TokenAll].start

	// todo
	// if len(doubles) > 0 {
	// 	dst.docsCount = uint32(len(doubles))
	// 	logger.Warn("there are duplicate IDs when compaction", zap.Int("doubles", len(doubles)))
	// }

	return dst
}

type IDIteratorItem struct {
	i  int
	id seq.ID
}

type IDIterator struct {
	i      int
	offset int
	idx    *memIndex
}

func (i *IDIterator) Next() (v IDIteratorItem, has bool) {
	if i.offset < len(i.idx.ids) {
		v.i = i.i
		v.id = i.idx.ids[i.offset]
		has = true
		i.offset++
	}
	return v, has
}

func mergeIDs(dst *memIndex, indexes []*memIndex, newPositions [][]seq.DocPos) [][]uint32 {
	// todo doubles := []seq.ID{}

	newLIDs := make([][]uint32, len(indexes))
	iters := make([]IOrderedIterator[IDIteratorItem], len(indexes))
	for i, idx := range indexes {
		iters[i] = &IDIterator{idx: idx, i: i}
		newLIDs[i] = make([]uint32, 0, len(idx.ids))
	}

	orderedIDs := MergeKSortIterators(iters, func(a, b IDIteratorItem) int { return seq.Compare(b.id, a.id) })

	cur, has := orderedIDs.Next()

	for has {
		dst.ids = append(dst.ids, cur.id)
		dst.positions = append(dst.positions, newPositions[cur.i][len(newLIDs[cur.i])])
		lid := uint32(len(dst.ids))
		newLIDs[cur.i] = append(newLIDs[cur.i], lid)
		cur, has = orderedIDs.Next()
	}
	return newLIDs
}

type TokenIteratorPayload struct {
	idx     *memIndex
	newLIDs []uint32
}

type TokenIteratorItem struct {
	tid     uint32
	fid     uint32
	payload *TokenIteratorPayload
}

func (i *TokenIteratorItem) Field() []byte {
	return i.payload.idx.fields[i.fid]
}

func (i *TokenIteratorItem) Token() []byte {
	return i.payload.idx.tokens[i.tid]
}

func (i *TokenIteratorItem) LIDs() []uint32 {
	return i.payload.idx.tokenLIDs[i.tid]
}

func (i *TokenIteratorItem) NewLIDs() []uint32 {
	return i.payload.newLIDs
}

type TokenIterator struct {
	tid          uint32
	fid          uint32
	fieldLastTID uint32
	payload      TokenIteratorPayload
}

func NewTokenIterator(idx *memIndex, newLIDs []uint32) *TokenIterator {
	return &TokenIterator{
		fieldLastTID: idx.fieldsTokens[string(idx.fields[0])].count - 1,
		payload: TokenIteratorPayload{
			idx:     idx,
			newLIDs: newLIDs,
		},
	}
}

func (it *TokenIterator) Next() (v TokenIteratorItem, has bool) {
	if int(it.tid) < len(it.payload.idx.tokens) {
		v.tid = uint32(it.tid)
		v.fid = uint32(it.fid)
		v.payload = &it.payload
		has = true
		it.tid++

		if it.tid > it.fieldLastTID {
			it.fid++
			if int(it.fid) < len(it.payload.idx.fields) {
				it.fieldLastTID += it.payload.idx.fieldsTokens[string(it.payload.idx.fields[it.fid])].count
			}
		}
	}
	return v, has
}

func mergeTokens(dst *memIndex, indexes []*memIndex, reLIDs [][]uint32) {
	allCount := 0
	totalTokens := 0
	totalLIDsSize := 0
	TokensIterators := make([]IOrderedIterator[TokenIteratorItem], len(indexes))
	for i, index := range indexes {
		allCount += len(index.ids)
		TokensIterators[i] = NewTokenIterator(index, reLIDs[i])
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

	prv := TokenIteratorItem{}
	var prevField []byte
	cur, has := orderedTokens.Next()
	items := make([]TokenIteratorItem, 0, totalTokens)
	borders := make([]uint8, 0, totalTokens)
	for has {
		var border uint8

		if prv.payload == nil || cmpToken(prv, cur) != 0 {
			uniqTokensCount++
			uniqTokensSize += len(cur.Token())
			border++

			if !bytes.Equal(prevField, cur.Field()) {
				uniqFieldsCount++
				uniqFieldsSize += len(cur.Field())
				border++
				prevField = cur.Field()
			}
		}

		prv = cur
		items = append(items, cur)
		borders = append(borders, border)
		cur, has = orderedTokens.Next()
	}

	dst.fields = make([][]byte, 0, uniqFieldsCount)
	dst.tokens = make([][]byte, 0, uniqTokensCount)
	dst.tokenLIDs = make([][]uint32, 0, uniqTokensCount)

	allTokens := make([]byte, 0, uniqTokensSize)
	allFields := make([]byte, 0, uniqFieldsSize)
	tokenRanges := make([]tokenRange, 0, uniqFieldsCount)

	var all bool
	lidsSorter := NewLIDsSorter(totalLIDsSize, allCount)

	for i, item := range items {
		if borders[i] > 0 {

			if i > 0 {
				dst.tokenLIDs = append(dst.tokenLIDs, lidsSorter.Get())
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

				all = fieldStr == "_all_"
			}

			start := len(allTokens)
			allTokens = append(allTokens, item.Token()...)
			dst.tokens = append(dst.tokens, allTokens[start:])
		}

		if all {
			for range item.LIDs() {
				lidsSorter.Add(0)
			}
		} else {
			newLIDs := item.NewLIDs()
			for _, oldLID := range item.LIDs() {
				newLID := newLIDs[oldLID-1]
				lidsSorter.Add(newLID)
			}
		}
	}

	dst.tokenLIDs = append(dst.tokenLIDs, lidsSorter.Get())

	tid := uint32(len(dst.tokens)) - 1
	fstr := util.ByteToStringUnsafe(dst.fields[len(dst.fields)-1])
	tr := dst.fieldsTokens[fstr]
	tr.count = tid - tr.start + 1
	dst.fieldsTokens[fstr] = tr

}

type LIDsSorter struct {
	all    []uint32
	lids   []uint32
	offset int
	bitmap *roaring.Bitmap
}

func NewLIDsSorter(size, allCount int) *LIDsSorter {
	ls := &LIDsSorter{
		lids:   make([]uint32, 0, size),
		all:    make([]uint32, allCount),
		bitmap: roaring.New(),
	}
	for i := range allCount {
		ls.all[i] = uint32(i) + 1
	}
	return ls
}

func (s *LIDsSorter) Add(lid uint32) {
	s.lids = append(s.lids, lid)
}

func (s *LIDsSorter) Get() (dst []uint32) {
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
