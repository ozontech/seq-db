package active2

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"sort"
	"unsafe"

	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/seq"
)

type tokensRange struct {
	start int
	count int
}

type tokenFetcher struct {
	tokens [][]byte
}

func (tf *tokenFetcher) FetchToken(i int) []byte {
	return tf.tokens[i]
}

type Index struct {
	ids           []seq.ID               // IDs ordered DESC
	tokens        [][]byte               // tokens ordered ASC by field:token
	tokenLIDs     [][]uint32             // LIDs list for each token from `tokens`
	fieldsTokens  map[string]tokensRange // tokens locator for each field
	fields        [][]byte               // fields ordered ASC
	blocksOffsets []uint64               // blocks offsets ordered by offset
	positions     map[seq.ID]seq.DocPos  // just map seq.ID => seq.DocPos
	allTID        uint32

	docsSize  uint64
	docsCount uint32
}

func (index *Index) Hash() [16]byte {
	h := fnv.New128a()
	for _, id := range index.ids {
		bin := id.Bin()
		h.Write(bin[:])
	}
	return [16]byte(h.Sum(nil))
}

func (index *Index) getTokenFetcher(data tokensRange) *tokenFetcher {
	return &tokenFetcher{
		tokens: index.tokens[data.start : data.start+data.count],
	}
}

func (index *Index) IsIntersecting(from, to seq.MID) bool {
	maxMID := index.ids[0].MID
	minMID := index.ids[len(index.ids)-1].MID
	if to < minMID || maxMID < from {
		return false
	}
	return true
}

func CreateIndex(meta disk.DocBlock, blockOffset uint64) (*Index, error) {
	const uint32Len = uint64(unsafe.Sizeof(uint32(0)))

	payload, err := disk.DocBlock(meta).DecompressTo(nil)
	if err != nil {
		return nil, err
	}

	index := &Index{
		blocksOffsets: []uint64{blockOffset},
		positions:     make(map[seq.ID]seq.DocPos),
	}

	fieldsSize := 0
	tokensSize := 0
	totalTokens := 0

	tokenLIDsTotal := 0
	nextDocOffset := uint64(0)

	all := []uint32{}
	ids := []seq.ID{}

	fieldsIndex := map[string]map[string][]uint32{
		seq.TokenAll: {"": {}}, // empty _all_ entry
	}

	for len(payload) > 0 {
		n := binary.LittleEndian.Uint32(payload) + 4
		docMeta := payload[4:n]
		payload = payload[n:]

		var meta frac.MetaData
		if err := meta.UnmarshalBinary(docMeta); err != nil {
			return nil, err
		}

		lid := uint32(len(ids))
		all = append(all, lid)
		ids = append(ids, meta.ID)

		// build token index map
		for _, mt := range meta.Tokens {
			if bytes.Equal(mt.Key, seq.AllTokenName) { // skip if _ALL_ comes from seq-proxy
				tokenLIDsTotal--
				continue
			}
			tokensIndex, ok := fieldsIndex[string(mt.Key)]
			if !ok {
				fieldsSize += len(mt.Key)
				tokensIndex = make(map[string][]uint32)
			}
			tokensLIDs, ok := tokensIndex[string(mt.Value)]
			if !ok {
				totalTokens++
				tokensSize += len(mt.Value)
			}
			tokensIndex[string(mt.Value)] = append(tokensLIDs, lid)
			fieldsIndex[string(mt.Key)] = tokensIndex
		}

		if meta.Size == 0 { // this is a nested document
			continue
		}

		index.positions[meta.ID] = seq.PackDocPos(0, nextDocOffset)
		nextDocOffset += uint64(meta.Size) + uint32Len
		index.docsCount++
	}

	index.docsSize = nextDocOffset - uint64(index.docsCount-1)*uint32Len

	index.fields = make([][]byte, 0, len(fieldsIndex))
	index.tokens = make([][]byte, 0, totalTokens)
	index.tokenLIDs = make([][]uint32, 0, totalTokens)

	lidsData := make([]uint32, 0, tokenLIDsTotal)
	tokensData := make([]byte, 0, tokensSize+fieldsSize)

	inverser := index.sortIDs(all, ids)
	index.sortTokens(fieldsIndex, tokensData, lidsData, inverser)

	index.allTID = uint32(index.fieldsTokens[seq.TokenAll].start)

	return index, nil
}

func (index *Index) sortIDs(all []uint32, ids []seq.ID) []uint32 {
	// sort all
	sort.Slice(all, func(i, j int) bool { return seq.Less(ids[int(all[i])], ids[int(all[j])]) })

	// sort ids and make inverser
	index.ids = make([]seq.ID, len(ids))
	inverser := make([]uint32, len(ids))
	for new, old := range all {
		index.ids[new] = ids[old]
		inverser[old] = uint32(new)
	}
	return inverser
}

func (index *Index) sortTokens(fieldsIndex map[string]map[string][]uint32, tokensData []byte, lidsData, inverser []uint32) {
	// copy fields and sort
	for field := range fieldsIndex {
		tokensData = append(tokensData, field...) // copy field
		index.fields = append(index.fields, tokensData[len(tokensData)-len(field):])
	}
	sort.Slice(index.fields, func(i, j int) bool { return bytes.Compare(index.fields[i], index.fields[j]) < 0 })

	// copy tokens and sort
	for _, field := range index.fields {
		start := len(index.tokens)
		for token, tokenLIDs := range fieldsIndex[string(field)] {
			sortLIDs(tokenLIDs, inverser)
			lidsData = append(lidsData, tokenLIDs...) // copy lids
			tokensData = append(tokensData, token...) // copy token
			index.tokens = append(index.tokens, tokensData[len(tokensData)-len(token):])
			index.tokenLIDs = append(index.tokenLIDs, lidsData[len(lidsData)-len(tokenLIDs):])
		}
		tokens := index.tokens[start:]
		sort.Slice(tokens, func(i, j int) bool { return bytes.Compare(tokens[i], tokens[j]) < 0 })
		index.fieldsTokens[string(field)] = tokensRange{start: start, count: len(tokens)}
	}
}

func sortLIDs(lids, inverser []uint32) {
	for i, old := range lids {
		lids[i] = inverser[old] + 1 // inverse: LIDs starts from 1 not 0
	}
	sort.Slice(lids, func(i, j int) bool { return lids[i] < lids[j] }) // sort
}
