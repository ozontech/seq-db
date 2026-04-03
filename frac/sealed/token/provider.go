package token

import (
	"sort"

	"github.com/ozontech/seq-db/pattern"
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

func (tp *Provider) FindContains(firstTID, lastTID uint32, needle []byte) ([]uint32, error) {
	return tp.findInBlocks(firstTID, lastTID, func(b *Block, firstIndex, lastIndex int) ([]int, error) {
		return b.FindContains(firstIndex, lastIndex, needle)
	})
}

func (tp *Provider) FindToken(searcher pattern.Searcher) ([]uint32, error) {
	return tp.findInBlocks(searcher.FirstTID(), searcher.LastTID(), func(b *Block, firstIndex, lastIndex int) ([]int, error) {
		return b.FindToken(firstIndex, lastIndex, searcher)
	})
}

func (tp *Provider) findInBlocks(firstTID, lastTID uint32, search func(*Block, int, int) ([]int, error)) ([]uint32, error) {
	entries := tp.narrowEntries(firstTID, lastTID)
	if len(entries) == 0 {
		return nil, nil
	}

	var tids []uint32

	for _, entry := range entries {
		block := tp.findBlock(entry.BlockIndex)
		firstIndex, lastIndex := tp.narrowTIDs(entry, firstTID, lastTID)
		indices, err := search(block, firstIndex, lastIndex)
		if err != nil {
			return nil, err
		}
		for _, idx := range indices {
			tid := entry.StartTID + uint32(idx-int(entry.StartIndex))
			tids = append(tids, tid)
		}
	}
	return tids, nil
}

func (tp *Provider) narrowTIDs(entry *TableEntry, firstTID, fromTID uint32) (int, int) {
	tidStart := firstTID
	if entry.StartTID > tidStart {
		tidStart = entry.StartTID
	}
	tidEnd := fromTID
	if lastTID := entry.getLastTID(); lastTID < tidEnd {
		tidEnd = lastTID
	}

	firstIndex := entry.GetIndexInTokensBlock(tidStart)
	lastIndex := entry.GetIndexInTokensBlock(tidEnd)
	return firstIndex, lastIndex
}

func (tp *Provider) narrowEntries(firstTID, lastTID uint32) []*TableEntry {
	firstIdx := sort.Search(len(tp.entries), func(i int) bool {
		return tp.entries[i].getLastTID() >= firstTID
	})
	if firstIdx >= len(tp.entries) {
		return nil
	}
	lastIdx := sort.Search(len(tp.entries), func(i int) bool {
		return tp.entries[i].StartTID > lastTID
	})
	lastIdx--
	if lastIdx < firstIdx {
		return nil
	}
	entries := tp.entries[firstIdx : lastIdx+1]
	return entries
}
