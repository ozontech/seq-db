package filtermanager

import (
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/node"
)

type IteratorAsc Iterator

func (it *IteratorAsc) String() string {
	return "TOMBSTONES_ITERATOR_ASC"
}

func (it *IteratorAsc) Next() node.LID {
	if it.loader.headers == nil {
		headers, err := it.loader.getHeaders()
		if err != nil {
			logger.Panic("can't load tombstones headers", zap.Error(err))
		}
		it.loader.headers = headers
		it.blockIndex = len(it.loader.headers) - 1
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

	i := len(it.lids) - 1
	lid := it.lids[i]
	it.lids = it.lids[:i]
	return node.NewAscLID(lid)
}

func (it *IteratorAsc) loadNextLIDsBlock() {
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

func (it *IteratorAsc) needTryNextBlock() {
	it.tryNextBlock = it.blockIndex > 0
	it.blockIndex--
}
