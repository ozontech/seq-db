package token

import (
	"sort"
)

type Provider struct {
	loader   *BlockLoader
	entries  []*TableEntry // continuous monotonic sequence of token table entries
	curEntry *TableEntry

	curBlock      *Block
	curBlockIndex uint32
}

func NewProvider(loader *BlockLoader, entries []*TableEntry) *Provider {
	return &Provider{
		loader:   loader,
		entries:  entries,
		curEntry: nil,
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

func (tp *Provider) findEntry(tid uint32) *TableEntry {
	if tp.curEntry != nil && tp.curEntry.checkTIDInBlock(tid) { // fast path
		return tp.curEntry
	}

	entryIndex := sort.Search(len(tp.entries), func(blockIndex int) bool { return tid <= tp.entries[blockIndex].getLastTID() })
	return tp.entries[entryIndex]
}

func (tp *Provider) GetToken(tid uint32) []byte {
	entry := tp.findEntry(tid)
	if entry != tp.curEntry {
		tp.curEntry = entry
		if entry.BlockIndex != tp.curBlockIndex {
			tp.curBlockIndex = entry.BlockIndex
			tp.curBlock = tp.loader.Load(tp.curBlockIndex)
		}
	}
	return tp.curBlock.GetToken(entry.GetIndexInTokensBlock(tid))
}
