package token

import "sort"

type Provider struct {
	loader  *BlockLoader
	entries []*TableEntry // continuous monotonic sequence of token table entries

	curEntryIndex int
	curEntryData  *entryData
}

func NewProvider(loader *BlockLoader, entries []*TableEntry) *Provider {
	return &Provider{
		loader:        loader,
		entries:       entries,
		curEntryIndex: -1,
	}
}

func (tp *Provider) FirstTID() uint32 {
	return tp.entries[0].StartTID
}

func (tp *Provider) LastTID() uint32 {
	return tp.entries[len(tp.entries)-1].getLastTID()
}

func (tp *Provider) Ordered() bool {
	return true
}

func (tp *Provider) findEntry(tid uint32) int {
	if tp.curEntryIndex >= 0 && tp.entries[tp.curEntryIndex].checkTIDInBlock(tid) { // fast path
		return tp.curEntryIndex
	}

	return sort.Search(len(tp.entries), func(blockIndex int) bool { return tid <= tp.entries[blockIndex].getLastTID() })
}

func (tp *Provider) GetToken(tid uint32) []byte {
	entryIndex := tp.findEntry(tid)
	if entryIndex != tp.curEntryIndex {
		tp.curEntryIndex = entryIndex
		tp.curEntryData = tp.loader.Load(tp.entries[entryIndex])
	}
	return tp.curEntryData.GetValByTID(tid)
}
