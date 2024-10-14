package frac

import (
	"math"
	"sync"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
)

const (
	docOffsetBits = 30 // this value (but not 32 bits) for historical reasons
	docOffsetMask = 1<<docOffsetBits - 1

	maxDocOffset  = docOffsetMask
	maxBlockIndex = math.MaxUint32

	DocPosNotFound = DocPos(math.MaxUint64)
)

type DocPos uint64

func PackDocPos(blockIndex uint32, offset uint64) DocPos {
	if offset > maxDocOffset {
		logger.Panic("block offset too big",
			zap.Uint64("val", offset),
			zap.Int("max", maxDocOffset),
		)
	}

	pos := DocPos(blockIndex)<<docOffsetBits | DocPos(offset)

	// we add 1 to avoid DocPos = 0 (it was a "not found" sign)
	// this is not relevant now, but we leave it only for backward compatibility
	return pos + 1
}

func (pos DocPos) Unpack() (uint32, uint64) {
	pos-- // rollback "pos + 1" from PackDocPos func
	blockIndex := uint32(pos >> docOffsetBits)
	offset := uint64(pos & docOffsetMask)

	return blockIndex, offset
}

type DocsPositions struct {
	mu        sync.RWMutex
	positions map[seq.ID]DocPos
}

func NewSyncDocsPositions() *DocsPositions {
	return &DocsPositions{
		positions: make(map[seq.ID]DocPos),
	}
}

func (dp *DocsPositions) Get(id seq.ID) DocPos {
	if val, ok := dp.positions[id]; ok {
		return val
	}
	return DocPosNotFound
}

func (dp *DocsPositions) GetSync(id seq.ID) DocPos {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	return dp.Get(id)
}

// SetMultiple returns a slice of added ids
func (dp *DocsPositions) SetMultiple(ids []seq.ID, pos []DocPos) []seq.ID {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	appended := make([]seq.ID, 0)
	for i, id := range ids {
		// Positions may be equal in case of nested index.
		if savedPos, ok := dp.positions[id]; !ok || savedPos == pos[i] {
			dp.positions[id] = pos[i]
			appended = append(appended, id)
		}
	}
	return appended
}
