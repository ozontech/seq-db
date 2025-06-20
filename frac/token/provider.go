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

func (tp *Provider) findBlock(blockIndex uint32) *Block {
	if tp.curBlockIndex != blockIndex {
		tp.curBlockIndex = blockIndex
		tp.curBlock = tp.loader.Load(blockIndex)
	}
	return tp.curBlock
}

func (tp *Provider) GetToken(tid uint32) []byte {
	entry := tp.findEntry(tid)
	block := tp.findBlock(entry.BlockIndex)
	return block.GetToken(entry.GetIndexInTokensBlock(tid))
}
