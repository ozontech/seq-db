package docsfilter

import (
	"sort"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

type IteratorAsc Iterator

func (it *IteratorAsc) String() string {
	return "TOMBSTONES_ITERATOR_ASC"
}

func (it *IteratorAsc) Next() (uint32, bool) {
	if it.loader.headers == nil {
		err := it.loader.loadHeaders()
		if err != nil {
			logger.Panic("can't load tombstones headers", zap.Error(err))
		}
		it.blockIndex = len(it.loader.headers) - 1
	}

	for len(it.lids) == 0 {
		if !it.tryNextBlock {
			if err := it.loader.release(); err != nil {
				logger.Panic("error closing loader", zap.Error(err))
			}
			return 0, false
		}

		it.loadNextLIDsBlock()
		it.lids = it.narrowLIDsRange(it.lids)
	}

	i := len(it.lids) - 1
	lid := it.lids[i]
	it.lids = it.lids[:i]
	return lid, true
}

func (it *IteratorAsc) loadNextLIDsBlock() {
	hasLIDsInRange := it.hasLIDsInRange()
	if !hasLIDsInRange {
		it.needTryNextBlock()
		return
	}

	block, err := it.loader.loadBlock(it.blockIndex)
	if err != nil {
		logger.Panic("error loading LIDs block", zap.Error(err))
	}

	it.lids = block
	it.needTryNextBlock()
}

func (it *IteratorAsc) hasLIDsInRange() bool {
	if it.loader.headers[it.blockIndex].MinLID > it.maxLID {
		return false
	}
	if it.loader.headers[it.blockIndex].MaxLID < it.minLID {
		return false
	}

	return true
}

func (it *IteratorAsc) needTryNextBlock() {
	it.tryNextBlock = it.blockIndex > 0
	it.blockIndex--
}

// narrowLIDsRange cuts LIDs between from and to. Returns new lids
func (it *IteratorAsc) narrowLIDsRange(lids []uint32) []uint32 {
	if len(lids) == 0 {
		return lids
	}

	first := lids[0]
	last := lids[len(lids)-1]

	if it.minLID > first {
		left := sort.Search(len(lids), func(i int) bool { return lids[i] >= it.minLID })
		lids = lids[left:]
	}

	if it.maxLID <= last {
		right := sort.Search(len(lids), func(i int) bool { return lids[i] > it.maxLID })
		lids = lids[:right]
	}

	return lids
}
