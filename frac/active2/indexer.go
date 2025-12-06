package active2

import (
	"cmp"
	"encoding/binary"
	"slices"
	"unsafe"

	"github.com/ozontech/seq-db/indexer"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/tokenizer"
	"github.com/ozontech/seq-db/util"
)

const uint32Len = int(unsafe.Sizeof(uint32(0)))

type Indexer struct {
	sem chan struct{}
}

func NewIndexer(workersCount int) *Indexer {
	return &Indexer{
		sem: make(chan struct{}, workersCount),
	}
}

func (s *Indexer) Index(meta storage.DocBlock, applyFn func(index *memIndex, err error)) {
	s.sem <- struct{}{}
	go func() {
		applyFn(NewMemIndex(meta))
		<-s.sem
	}()
}

func NewMemIndex(metaBlock storage.DocBlock) (*memIndex, error) {
	sw := stopwatch.New()

	res := newIndexerResources()
	defer res.releaseAll()

	payload, err := decompressionMeta(metaBlock, res, sw)
	if err != nil {
		return nil, err
	}

	meta, err := decodeMeta(payload, res, sw)
	if err != nil {
		return nil, err
	}

	idx := &memIndex{
		idToLID:       make(map[seq.ID]uint32, len(meta)),
		docsCount:     uint32(len(meta)),
		fieldsTokens:  make(map[string]tokensRange),
		blocksOffsets: []uint64{metaBlock.GetExt2()}, // only one block per bulk
	}

	tids, lids, tokens, err := extractTokensFromMetadata(meta, idx, res)
	if err != nil {
		return nil, err
	}

	tokenLIDs := groupLIDsByToken(res, tids, lids, len(tokens))

	organizeTokensAndFields(idx, tokens, tokenLIDs)

	idx.allTID = uint32(idx.fieldsTokens[seq.TokenAll].start)

	return idx, nil
}

type tokenKey struct {
	v, k string
}

func convertMetaToken(t tokenizer.MetaToken) tokenKey {
	return tokenKey{
		k: util.ByteToStringUnsafe(t.Key),
		v: util.ByteToStringUnsafe(t.Value),
	}
}

func extractTokensFromMetadata(
	meta []indexer.MetaData,
	idx *memIndex,
	res *indexerResources,
) ([]uint32, []uint32, []tokenKey, error) {
	var lidsSize uint32
	var docOffset uint64

	localRes := newIndexerResources()
	defer localRes.releaseAll()

	// scan in orig order to calc offsets and size
	positions := localRes.newDocPos(len(meta))
	prev := seq.PackDocPos(0, docOffset)
	positions = positions[:len(meta)] // inBoubds
	for i, docMeta := range meta {
		if docMeta.Size > 0 {
			prev = seq.PackDocPos(0, docOffset)
			docOffset += uint64(docMeta.Size) + uint64(uint32Len)
		}
		positions[i] = prev
		lidsSize += docMeta.TokensCount()
	}

	lids := res.newUint32s(int(lidsSize))
	tids := res.newUint32s(int(lidsSize))

	order := localRes.newUint32s(len(meta))
	for i := range order {
		order[i] = uint32(i)
	}
	slices.SortFunc(order, func(a, b uint32) int { return seq.Compare(meta[b].ID, meta[a].ID) })

	ids := make([]seq.ID, len(order))
	pos := make([]seq.DocPos, len(order))

	for lid, i := range order {
		docMeta := meta[i]
		ids[lid] = docMeta.ID
		idx.docsSize += uint64(docMeta.Size)
		idx.idToLID[docMeta.ID] = uint32(lid)
		pos[lid] = positions[i]
	}

	idx.ids = ids
	idx.positions = pos

	var err error
	var mt tokenKey

	tids = tids[:0]
	lids = lids[:0]

	tokenToTID := localRes.newMetaTokenMap(1000)
	tokens, release := localRes.newTokenizerMetaTokens(1000)

	for lid, i := range order {
		docMeta := meta[i]
		if tokens, err = docMeta.DecodeTokens(tokens[:0]); err != nil {
			return nil, nil, nil, err
		}
		for _, t := range tokens {
			mt = convertMetaToken(t)
			tid, ok := tokenToTID[mt]
			if !ok {
				tid = uint32(len(tokenToTID))
				tokenToTID[mt] = tid
			}
			tids = append(tids, tid)
			lids = append(lids, uint32(lid+1))
		}
	}

	release(tokens)

	tidToToken := res.newMetaTokens(len(tokenToTID))
	for mt, tid := range tokenToTID {
		tidToToken[tid] = mt
	}

	return tids, lids, tidToToken, nil
}

func groupLIDsByToken(res *indexerResources, tids, lids []uint32, tokensCnt int) [][]uint32 {
	// считаем размеры токенлидсов
	localRes := newIndexerResources()
	defer localRes.releaseAll()

	lens := localRes.newUint32s(tokensCnt)
	clear(lens)
	for _, tid := range tids {
		lens[tid]++
	}

	// нарезаем токенлидсы
	tokenLIDs := res.newUint32Slices(tokensCnt)
	lidsBuffer := make([]uint32, len(lids))
	for tid, cnt := range lens {
		tokenLIDs[tid] = lidsBuffer[:cnt][:0]
		lidsBuffer = lidsBuffer[cnt:]
	}

	// заполняем токенлидсы
	lids = lids[:len(tids)] // isInBounds
	for i, tid := range tids {
		tokenLIDs[tid] = append(tokenLIDs[tid], lids[i])
	}
	return tokenLIDs
}

func organizeTokensAndFields(idx *memIndex, tokens []tokenKey, tokenLIDs [][]uint32) {
	localRes := newIndexerResources()
	defer localRes.releaseAll()

	order := localRes.newUint32s(len(tokens))
	for i := range order {
		order[i] = uint32(i)
	}

	slices.SortFunc(order, func(a, b uint32) int {
		aToken, bToken := tokens[a], tokens[b]
		return cmp.Or(
			cmp.Compare(aToken.k, bToken.k),
			cmp.Compare(aToken.v, bToken.v),
		)
	})

	tokensSize := 0
	for _, t := range tokens {
		tokensSize += len(t.v)
	}

	prevField := ""
	fieldsSize := 0
	fields := localRes.newStrings(100)[:0]
	fieldsTIDs := localRes.newUint32s(100)[:0]

	bufferTokens := make([]byte, 0, tokensSize)

	orderedTokens := make([][]byte, len(order))
	orderedTokenLIDs := make([][]uint32, len(order))

	for tid, i := range order {
		mt := tokens[i]
		if mt.k != prevField || prevField == "" {
			// collect uniq fields values
			fieldsSize += len(mt.k)
			fields = append(fields, mt.k)
			fieldsTIDs = append(fieldsTIDs, uint32(tid))
		}
		prevField = mt.k

		// copy tokens
		p := len(bufferTokens)
		bufferTokens = append(bufferTokens, mt.v...)

		// fill tokens ordered
		orderedTokens[tid] = bufferTokens[p:]
		orderedTokenLIDs[tid] = tokenLIDs[i]
	}

	idx.tokens = orderedTokens
	idx.tokenLIDs = orderedTokenLIDs

	fieldsTIDs = append(fieldsTIDs, uint32(len(tokens)))

	bufferFields := make([]byte, 0, fieldsSize)
	idx.fields = make([][]byte, len(fields))
	for i, field := range fields {
		// copy fields
		p := len(bufferFields)
		bufferFields = append(bufferFields, field...)
		idx.fields[i] = bufferFields[p:]

		// fill field range
		tid1 := fieldsTIDs[i]
		tid2 := fieldsTIDs[i+1]
		idx.fieldsTokens[util.ByteToStringUnsafe(bufferFields[p:])] = tokensRange{
			start: tid1,
			count: tid2 - tid1,
		}
	}
}

func decompressionMeta(meta storage.DocBlock, ia *indexerResources, sw *stopwatch.Stopwatch) ([]byte, error) {
	m := sw.Start("decompress_meta")
	defer m.Stop()

	payload, err := meta.DecompressTo(ia.newBytes(int(meta.RawLen())))
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func decodeMeta(payload []byte, ia *indexerResources, sw *stopwatch.Stopwatch) ([]indexer.MetaData, error) {
	m := sw.Start("decode_meta")
	defer m.Stop()

	// scan to get length
	offset := 0
	offsets := ia.newInts(1000)[:0]
	for offset < len(payload) {
		size := binary.LittleEndian.Uint32(payload[offset:])
		offset += uint32Len + int(size)
		offsets = append(offsets, int(size))
	}

	// decode
	meta := ia.newMetaData(len(offsets))
	for i, size := range offsets {
		bin := payload[uint32Len : size+uint32Len]
		if err := meta[i].UnmarshalBinaryLazy(bin); err != nil {
			return nil, err
		}
		payload = payload[size+uint32Len:]
	}

	return meta, nil
}
