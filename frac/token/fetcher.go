package token

import "sort"

type Fetcher struct {
	loader  *BlockLoader
	entires []*TableEntry // continuous monotonic sequence of token table entries

	curBlockIndex  int
	curTokensBlock *Block
}

func NewFetcher(loader *BlockLoader, entries []*TableEntry) *Fetcher {
	b := &Fetcher{
		loader:        loader,
		entires:       entries,
		curBlockIndex: -1,
	}
	return b
}

func (f *Fetcher) getFirstTID() uint32 {
	return f.entires[0].StartTID
}

func (f *Fetcher) getLastTID() uint32 {
	lastIndex := len(f.entires) - 1
	return f.entires[lastIndex].StartTID + f.entires[lastIndex].ValCount - 1
}

func (f *Fetcher) GetTokensCount() int {
	return int(f.getLastTID() - f.getFirstTID() + 1)
}

// getFirstTokenIndex returns a serial number of first token of corresponding block
func (f *Fetcher) getFirstTokenIndex(blockIndex int) int {
	if blockIndex >= len(f.entires) {
		return f.GetTokensCount()
	}
	return int(f.entires[blockIndex].StartTID - f.getFirstTID())
}

// GetTIDFromIndex gets the "index" parameter. It is a serial number of token in this blockFetcher's blocks set
func (f *Fetcher) GetTIDFromIndex(index int) uint32 {
	return f.getFirstTID() + uint32(index)
}

func (f *Fetcher) compareIndex(index, blockIndex int) int {
	if index < f.getFirstTokenIndex(blockIndex) {
		return -1 // the index does not fall into this block, but into one of the previous ones
	}
	if index >= f.getFirstTokenIndex(blockIndex+1) {
		return 1 // the index does not fall into this block, but into one of the following
	}
	return 0 // index falls into this block
}

func (f *Fetcher) GetBlockIndex(index int) int {
	if f.curBlockIndex >= 0 && f.compareIndex(index, f.curBlockIndex) == 0 { // fast path
		return f.curBlockIndex
	}
	return sort.Search(len(f.entires), func(blockIndex int) bool {
		return f.compareIndex(index, blockIndex) <= 0
	})
}

func (f *Fetcher) FetchToken(index int) []byte {
	blockIndex := f.GetBlockIndex(index)
	if blockIndex != f.curBlockIndex {
		f.curBlockIndex = blockIndex
		f.curTokensBlock = f.loader.Load(f.entires[blockIndex])
	}
	return f.curTokensBlock.GetValByTID(f.GetTIDFromIndex(index))
}
