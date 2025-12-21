package active2

import (
	"bytes"
	"slices"

	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

func mergeIndexes(indexes []*memIndex) *memIndex {
	blocksCount := 0
	dst := newMemIndex()
	for _, idx := range indexes {
		dst.docsSize += idx.docsSize
		dst.docsCount += idx.docsCount
		dst.allTokenLIDsCount += idx.allTokenLIDsCount
		blocksCount += len(idx.blocksOffsets)
	}

	res, release := NewResources()
	defer release()

	dst.ids = dst.res.GetIDs(int(dst.docsCount))[:0]
	dst.positions = dst.res.GetDocPos(int(dst.docsCount))[:0]
	dst.blocksOffsets = dst.res.GetUint64s(blocksCount)[:0]

	posMap := mergeBlocksOffsets(dst, res, indexes)
	lidsMap := mergeIDs(dst, res, indexes, posMap)
	mergeTokens(dst, res, indexes, lidsMap)

	dst.allTID = dst.fieldsTokens[seq.TokenAll].start

	return dst
}

func mergeIDs(dst *memIndex, res *Resources, indexes []*memIndex, posMap [][]seq.DocPos) [][]uint32 {
	lidsMap := res.GetUint32Slices(len(indexes))
	iters := make([]OrderedStream[DocRef], len(indexes))
	for i, idx := range indexes {
		iters[i] = &DocStream{
			i:      i,
			idx:    idx,
			posMap: posMap[i],
		}
		lidsMap[i] = res.GetUint32s(int(idx.docsCount) + 1)[:1] // 1-based
	}

	orderedIDs := MergeSortedStreams(iters, func(a, b DocRef) int { return seq.Compare(b.id, a.id) })

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
	totalTokens := 0
	tokensIterators := make([]OrderedStream[TokenRef], len(indexes))
	for i, idx := range indexes {
		totalTokens += len(idx.tokens)
		tokensIterators[i] = NewTokenStream(idx, lidsMap[i])
	}

	cmpToken := func(a, b TokenRef) int {
		r := bytes.Compare(a.Field(), b.Field())
		if r == 0 {
			return bytes.Compare(a.Value(), b.Value())
		}
		return r
	}

	orderedTokens := MergeSortedStreams(tokensIterators, cmpToken)

	uniqTokensSize := 0
	uniqTokensCount := 0

	uniqFieldsSize := 0
	uniqFieldsCount := 0

	var (
		prevField []byte
		prevToken TokenRef
	)

	borders := res.GetBytes(totalTokens)[:0]
	tokens := make([]TokenRef, 0, totalTokens)

	for cur, has := orderedTokens.Next(); has; cur, has = orderedTokens.Next() {
		var border uint8

		if prevToken.payload == nil || cmpToken(prevToken, cur) != 0 {
			uniqTokensCount++
			uniqTokensSize += len(cur.Value())
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
		tokens = append(tokens, cur)
		prevToken = cur
	}

	dst.fieldsTokens = make(map[string]tokenRange, uniqFieldsCount)
	dst.fields = dst.res.GetBytesSlices(uniqFieldsCount)[:0]
	dst.tokens = dst.res.GetBytesSlices(uniqTokensCount)[:0]
	dst.tokenLIDs = dst.res.GetUint32Slices(uniqTokensCount)[:0]

	allTokens := dst.res.GetBytes(uniqTokensSize)[:0]
	allFields := dst.res.GetBytes(uniqFieldsSize)[:0]

	lidsCollector := NewLIDsCollector(
		res.GetUint32s(int(dst.docsCount)),                                 // tmp buf
		dst.res.GetUint32s(dst.allTokenLIDsCount - int(dst.docsCount))[:0], // all token LIDs
		dst.res.GetUint32s(int(dst.docsCount)),                             // ALL LIDs for token _all_
		res.GetBytes((int(dst.docsCount) + 1)),                             // sort buffer
	)

	var isAllToken bool
	for i, token := range tokens {
		token := token
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
				allFields = append(allFields, token.Field()...)
				field := allFields[start:]
				dst.fields = append(dst.fields, field)

				fieldStr := util.ByteToStringUnsafe(field)
				dst.fieldsTokens[fieldStr] = tokenRange{start: tid}

				isAllToken = fieldStr == seq.TokenAll
			}

			start := len(allTokens)
			allTokens = append(allTokens, token.Value()...)
			dst.tokens = append(dst.tokens, allTokens[start:])
		}

		if isAllToken {
			for range token.LIDs() {
				lidsCollector.Add(0) // stub
			}
		} else {
			newLIDsMap := token.lidsMap()
			for _, oldLID := range token.LIDs() {
				lidsCollector.Add(newLIDsMap[oldLID])
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
	tmp  []uint32
	lids []uint32
	all  []uint32
	buf  []uint8
}

func NewLIDsCollector(tmp, lids, all []uint32, buf []uint8) *LIDsCollector {
	clear(buf)
	for i := range all {
		all[i] = uint32(i) + 1
	}
	return &LIDsCollector{
		tmp:  tmp[:0],
		lids: lids[:0],
		all:  all,
		buf:  buf,
	}
}

func (s *LIDsCollector) Add(lid uint32) {
	s.tmp = append(s.tmp, lid)
}

func (s *LIDsCollector) GetSorted() (dst []uint32) {
	n := len(s.tmp)

	if n == len(s.all) {
		s.tmp = s.tmp[:0]
		return s.all
	}

	if 100*n/len(s.all) > 50 {
		for _, v := range s.tmp {
			s.buf[v] = 1
		}
		start := len(s.lids)
		for lid, ok := range s.buf {
			if ok == 1 {
				s.buf[lid] = 0
				s.lids = append(s.lids, uint32(lid))
			}
		}
		s.tmp = s.tmp[:0]
		return s.lids[start:]
	}

	if n > 1 {
		slices.Sort(s.tmp)
	}
	start := len(s.lids)
	s.lids = append(s.lids, s.tmp...)
	s.tmp = s.tmp[:0]
	return s.lids[start:]
}

func mergeBlocksOffsets(dst *memIndex, res *Resources, indexes []*memIndex) [][]seq.DocPos {
	var offset uint32
	positions := res.GetDocPosSlices(len(indexes))
	for i, index := range indexes {
		dst.blocksOffsets = append(dst.blocksOffsets, index.blocksOffsets...)
		positions[i] = res.GetDocPos(len(index.positions))[:0]
		for _, p := range index.positions {
			oldIdx, docOffset := p.Unpack()
			positions[i] = append(positions[i], seq.PackDocPos(oldIdx+offset, docOffset))
		}
		offset += uint32(len(index.blocksOffsets))
	}
	return positions
}
