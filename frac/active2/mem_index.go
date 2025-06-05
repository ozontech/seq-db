package active2

import (
	"bytes"
	"sort"

	"github.com/ozontech/seq-db/seq"
)

type tokensRange struct {
	start int
	count int
}

type memIndex struct {
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

func (index *memIndex) getTokenProvider(field string) *tokenProvider {
	r := index.fieldsTokens[field]
	return &tokenProvider{
		firstTID: uint32(r.start),
		lastTID:  uint32(r.start + r.count - 1),
		tokens:   index.tokens,
	}
}

func (index *memIndex) IsIntersecting(from, to seq.MID) bool {
	maxMID := index.ids[0].MID
	minMID := index.ids[len(index.ids)-1].MID
	if to < minMID || maxMID < from {
		return false
	}
	return true
}

func (index *memIndex) sortIDs(all []uint32, ids []seq.ID) []uint32 {
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

func (index *memIndex) sortTokens(fieldsIndex map[string]map[string][]uint32, tokensData []byte, lidsData, inverser []uint32) {
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

type tokenProvider struct {
	firstTID uint32
	lastTID  uint32
	tokens   [][]byte
}

func (p *tokenProvider) GetToken(tid uint32) []byte {
	return p.tokens[tid]
}

func (p *tokenProvider) FirstTID() uint32 {
	return p.firstTID
}

func (p *tokenProvider) LastTID() uint32 {
	return p.lastTID
}

func (p *tokenProvider) Ordered() bool {
	return true
}
