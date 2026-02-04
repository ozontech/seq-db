package docsfilter

import (
	"sort"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

type Iterator struct {
	loader *loader

	minLID uint32
	maxLID uint32

	blockIndex   int
	tryNextBlock bool

	lids []uint32
}

func NewIterator(
	loader *loader,
	minLID uint32,
	maxLID uint32,
) *Iterator {
	return &Iterator{
		loader:       loader,
		minLID:       minLID,
		maxLID:       maxLID,
		tryNextBlock: true,
	}
}

func (it *Iterator) Next() (uint32, bool) {
	if it.loader.headers == nil {
		err := it.loader.loadHeaders()
		if err != nil {
			logger.Panic("can't load tombstones headers", zap.Error(err))
		}
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

	lid := it.lids[0]
	it.lids = it.lids[1:]
	return lid, true
}

func (it *Iterator) loadNextLIDsBlock() {
	hasLIDsInRange := it.hasLIDsInRange()
	if !hasLIDsInRange {
		it.needTryNextBlock()
		return
	}

	block, err := it.loader.loadBlock(it.blockIndex)
	if err != nil {
		logger.Panic("error loading LIDs block", zap.Error(err))
	}

	// TODO: read lids as uint32 (???)
	uint32s := make([]uint32, 0, len(block))
	for _, lid := range block {
		uint32s = append(uint32s, uint32(lid))
	}

	it.lids = uint32s
	it.needTryNextBlock()
}

func (it *Iterator) hasLIDsInRange() bool {
	if it.loader.headers[it.blockIndex].MinLID > it.maxLID {
		return false
	}
	if it.loader.headers[it.blockIndex].MaxLID < it.minLID {
		return false
	}

	return true
}

func (it *Iterator) needTryNextBlock() {
	it.tryNextBlock = it.blockIndex < len(it.loader.headers)-1
	it.blockIndex++
}

// narrowLIDsRange cuts LIDs between from and to. Returns new lids
func (it *Iterator) narrowLIDsRange(lids []uint32) []uint32 {
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
