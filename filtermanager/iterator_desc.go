package filtermanager

import (
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/node"
)

type IteratorDesc Iterator

func (it *IteratorDesc) String() string {
	return "TOMBSTONES_ITERATOR_DESC"
}

func (it *IteratorDesc) Next() node.LID {
	if it.loader.headers == nil {
		headers, err := it.loader.getHeaders()
		if err != nil {
			logger.Panic("can't load tombstones headers", zap.Error(err))
		}
		it.loader.headers = headers
	}

	for len(it.lids) == 0 {
		if !it.tryNextBlock {
			if err := it.loader.release(); err != nil {
				logger.Panic("error closing loader", zap.Error(err))
			}
			return node.NullLID()
		}

		it.loadNextLIDsBlock()
		it.lids = (*Iterator)(it).narrowLIDsRange(it.lids)
	}

	lid := it.lids[0]
	it.lids = it.lids[1:]
	return node.NewDescLID(lid)
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
