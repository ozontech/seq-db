package frac

import (
	"sync"

	"github.com/ozontech/seq-db/seq"
)

type DocsPositions struct {
	mu       sync.RWMutex
	idToPos  map[seq.ID]seq.DocPos
	lidToPos []seq.DocPos
}

func NewSyncDocsPositions() *DocsPositions {
	dp := DocsPositions{
		lidToPos: make([]seq.DocPos, 0),
		idToPos:  make(map[seq.ID]seq.DocPos),
	}
	dp.lidToPos = append(dp.lidToPos, 0) // systemID
	return &dp
}

func (dp *DocsPositions) Get(id seq.ID) seq.DocPos {
	if val, ok := dp.idToPos[id]; ok {
		return val
	}
	return seq.DocPosNotFound
}

func (dp *DocsPositions) GetSync(id seq.ID) seq.DocPos {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	return dp.Get(id)
}

// SetMultiple returns a slice of added ids
func (dp *DocsPositions) SetMultiple(ids []seq.ID, pos []seq.DocPos) []seq.ID {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	appended := make([]seq.ID, 0, len(ids))
	for i, id := range ids {
		p, ok := dp.idToPos[id]

		if ok {
			if p != pos[i] {
				// same ID but different position
				// this is a duplicate ID, we can't append it
				continue
			}
		} else {
			dp.idToPos[id] = pos[i]
		}

		dp.lidToPos = append(dp.lidToPos, pos[i])
		appended = append(appended, id)
	}
	return appended
}
