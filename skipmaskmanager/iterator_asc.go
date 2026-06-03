package skipmaskmanager

import (
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/node"
)

type IteratorAsc Iterator

func (it *IteratorAsc) String() string {
	return "SKIP_MASK_ITERATOR_ASC"
}

func (it *IteratorAsc) Next() node.LID {
	if it.loader.headers == nil {
		headers, err := it.loader.getHeaders()
		if err != nil {
			logger.Panic("can't load skip mask file headers", zap.Error(err))
		}
		it.loader.headers = headers
		it.blockIndex = len(it.loader.headers) - 1
	}

	for len(it.lids) == 0 {
		if !it.tryNextBlock {
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

func (it *IteratorAsc) NextGeq(nextID node.LID) node.LID {
	// TODO: implement NextGeq
	lid := it.Next()
	for lid.Less(nextID) {
		lid = it.Next()
	}
	return lid
}

func (it *IteratorAsc) loadNextLIDsBlock() {
	hasLIDsInRange := (*Iterator)(it).hasLIDsInRange()
	if !hasLIDsInRange {
		it.needTryNextBlock()
		return
	}

	lids := make([]uint32, 0, it.loader.headers[it.blockIndex].Length)
	err := it.loader.loadBlock(it.blockIndex, func(lid uint32) {
		lids = append(lids, lid)
	})
	if err != nil {
		logger.Panic("error loading LIDs block", zap.Error(err))
	}

	it.lids = lids
	it.needTryNextBlock()
}

func (it *IteratorAsc) needTryNextBlock() {
	it.tryNextBlock = it.blockIndex > 0
	it.blockIndex--
}
