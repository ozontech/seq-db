package frac

import (
	"sync"
	"unsafe"

	"github.com/ozontech/seq-db/seq"
)

type ActiveLIDs struct {
	mu      *sync.RWMutex
	idToLid map[seq.ID]seq.LID
}

func NewActiveLIDs() *ActiveLIDs {
	al := ActiveLIDs{
		mu:      &sync.RWMutex{},
		idToLid: make(map[seq.ID]seq.LID),
	}
	return &al
}

func (al *ActiveLIDs) Get(id seq.ID) (seq.LID, bool) {
	al.mu.RLock()
	defer al.mu.RUnlock()

	val, ok := al.idToLid[id]
	return val, ok
}

func (al *ActiveLIDs) Size() int {
	al.mu.RLock()
	defer al.mu.RUnlock()

	const entrySize = int(unsafe.Sizeof(seq.ID{})) +
		int(unsafe.Sizeof(seq.LID(0)))

	return len(al.idToLid) * entrySize
}

func (al *ActiveLIDs) SetMultiple(ids []seq.ID, lids []uint32) {
	al.mu.Lock()
	defer al.mu.Unlock()

	for i, id := range ids {
		al.idToLid[id] = seq.LID(lids[i])
	}
}
