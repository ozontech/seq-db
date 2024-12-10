package frac

import (
	"sync"

	"github.com/ozontech/seq-db/seq"
)

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
