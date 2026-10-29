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

func (tp *Provider) FindSuffix(suffix []byte) ([]uint32, error) {
	requiredLetters := util.NewLettersBitset(suffix)

	return tp.findInBlocks(
		tp.FirstTID(),
		tp.LastTID(),
		func(e *TableEntry) bool {
			return e.Letters.IsNil() || e.Letters.ContainsAll(requiredLetters)
		},
		func(b *Block, firstIndex, lastIndex int) ([]int, error) {
			return b.suffix(firstIndex, lastIndex, suffix)
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

func (tp *Provider) FindIntRange(from, to int64) []uint32 {
	first, last, ok := tp.intTIDRange(from, to)
	if !ok {
		return nil
	}

	tids := make([]uint32, 0, last-first+1)
	for tid := first; tid <= last; tid++ {
		tids = append(tids, tid)
	}
	return tids
}

func (tp *Provider) FindIntToken(ranges []pattern.IntRange, minLen int, searcher pattern.Searcher) ([]uint32, error) {
	var tids []uint32
	for _, r := range ranges {
		first, last, ok := tp.intTIDRange(r.From, r.To)
		if !ok {
			continue
		}

		found, err := tp.findInBlocks(
			first,
			last,
			func(e *TableEntry) bool {
				return e.maxIntLen() >= minLen && searcher.CheckEntry(e.Letters)
			},
			func(b *Block, firstIndex, lastIndex int) ([]int, error) {
				return b.find(firstIndex, lastIndex, searcher)
			})
		if err != nil {
			return nil, err
		}
		tids = append(tids, found...)
	}
	return tids, nil
}

func (tp *Provider) intTIDRange(from, to int64) (uint32, uint32, bool) {
	if from > to {
		return 0, 0, false
	}

	i := sort.Search(len(tp.entries), func(i int) bool { return tp.entries[i].MaxInt >= from })
	j := sort.Search(len(tp.entries), func(j int) bool { return tp.entries[j].MinInt > to }) - 1
	if i == len(tp.entries) || j < i {
		return 0, 0, false
	}

	first := tp.entryLowerBound(tp.entries[i], from)
	last := tp.entryLowerBound(tp.entries[j], to+1) - 1
	if to == math.MaxInt64 {
		last = tp.entries[j].GetLastTID()
	}

	if first > last {
		return 0, 0, false
	}
	return first, last, true
}

func (tp *Provider) entryLowerBound(e *TableEntry, v int64) uint32 {
	block := tp.findBlock(e.BlockIndex)
	ints := block.Ints[e.StartIndex : e.StartIndex+e.ValCount]
	idx := sort.Search(len(ints), func(k int) bool { return ints[k] >= v })
	return e.StartTID + uint32(idx)
}
