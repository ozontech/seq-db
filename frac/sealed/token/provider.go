package token

import (
	"math"
	"sort"

	"github.com/ozontech/seq-db/pattern"
	"github.com/ozontech/seq-db/util"
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
		loader:        loader,
		entries:       entries,
		curBlockIndex: math.MaxUint32, // sentinel: no block loaded yet
	}
}

func (tp *Provider) FirstTID() uint32 {
	return tp.entries[0].StartTID
}

func (tp *Provider) LastTID() uint32 {
	return tp.entries[len(tp.entries)-1].GetLastTID()
}

func (tp *Provider) Ordered() bool {
	return true
}

func (tp *Provider) findEntry(tid uint32) *TableEntry {
	if tp.curEntry != nil && tp.curEntry.checkTIDInBlock(tid) { // fast path
		return tp.curEntry
	}

	entryIndex := sort.Search(len(tp.entries), func(blockIndex int) bool { return tid <= tp.entries[blockIndex].GetLastTID() })
	return tp.entries[entryIndex]
}

func (tp *Provider) findBlock(blockIndex uint32) *Block {
	if tp.curBlockIndex != blockIndex {
		tp.curBlockIndex = blockIndex
		tp.curBlock = tp.loader.GetTokenBlock(blockIndex)
	}
	return tp.curBlock
}

func (tp *Provider) GetToken(tid uint32) []byte {
	entry := tp.findEntry(tid)
	block := tp.findBlock(entry.BlockIndex)
	return block.GetToken(entry.GetIndexInTokensBlock(tid))
}

func (tp *Provider) FindContains(needle []byte) ([]uint32, error) {
	requiredLetters := util.NewLettersBitset(needle)

	return tp.findInBlocks(
		tp.FirstTID(),
		tp.LastTID(),
		func(e *TableEntry) bool {
			return e.Letters.IsNil() || e.Letters.ContainsAll(requiredLetters)
		},
		func(b *Block, firstIndex, lastIndex int) ([]int, error) {
			return b.contains(firstIndex, lastIndex, needle)
		})
}

func (tp *Provider) FindToken(searcher pattern.Searcher) ([]uint32, error) {
	return tp.findInBlocks(
		searcher.FirstTID(),
		searcher.LastTID(),
		func(e *TableEntry) bool {
			return searcher.CheckEntry(e.Letters)
		},
		func(b *Block, firstIndex, lastIndex int) ([]int, error) {
			return b.find(firstIndex, lastIndex, searcher)
		})
}

func (tp *Provider) findInBlocks(
	firstTID,
	lastTID uint32,
	entryFilter func(*TableEntry) bool,
	search func(*Block, int, int) ([]int, error)) ([]uint32, error) {
	var tids []uint32

	for _, entry := range tp.entries {
		if !entry.checkTIDsInBlock(firstTID, lastTID) {
			continue
		}
		if !entryFilter(entry) {
			continue
		}

		block := tp.findBlock(entry.BlockIndex)
		firstIndex, lastIndex := entry.narrowIndexes(firstTID, lastTID)
		indexes, err := search(block, firstIndex, lastIndex)
		if err != nil {
			return nil, err
		}
		for _, idx := range indexes {
			tid := entry.StartTID + uint32(idx-int(entry.StartIndex))
			tids = append(tids, tid)
		}
	}
	return tids, nil
}
