package frac

import (
	"sync"
	"unsafe"

	"github.com/ozontech/seq-db/seq"
)

type DocsPositions struct {
	mu      sync.RWMutex
	idToPos map[seq.ID]seq.DocPos
}

func NewSyncDocsPositions() *DocsPositions {
	dp := DocsPositions{
		idToPos: make(map[seq.ID]seq.DocPos),
	}
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

func (dp *DocsPositions) Size() int {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	const entrySize = int(unsafe.Sizeof(seq.ID{})) +
		int(unsafe.Sizeof(seq.DocPos(0)))

	return len(dp.idToPos) * entrySize
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

		appended = append(appended, id)
	}
	return appended
}
