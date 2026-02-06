package docsfilter

import (
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

type IteratorDesc Iterator

func (it *IteratorDesc) String() string {
	return "TOMBSTONES_ITERATOR_DESC"
}

func (it *IteratorDesc) Next() (uint32, bool) {
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
		it.lids = (*Iterator)(it).narrowLIDsRange(it.lids)
	}

	lid := it.lids[0]
	it.lids = it.lids[1:]
	return lid, true
}

func (it *IteratorDesc) loadNextLIDsBlock() {
	hasLIDsInRange := (*Iterator)(it).hasLIDsInRange()
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

func (it *IteratorDesc) needTryNextBlock() {
	it.tryNextBlock = it.blockIndex < len(it.loader.headers)-1
	it.blockIndex++
}
