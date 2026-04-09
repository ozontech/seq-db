package skipmaskmanager

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

type SkipMaskStatus byte

const (
	StatusCreated SkipMaskStatus = iota
	StatusInProgress
	StatusDone
	StatusError
)

type SkipMaskParams struct {
	Query string
	From  seq.MID
	To    seq.MID
}

type SkipMask struct {
	params SkipMaskParams

	status SkipMaskStatus

	ast parser.SeqQLQuery

	hash    string
	dirPath string

	mu        *sync.RWMutex
	processWg *sync.WaitGroup
}

func NewSkipMask(params SkipMaskParams) *SkipMask {
	return &SkipMask{
		params:    params,
		status:    StatusCreated,
		mu:        &sync.RWMutex{},
		processWg: &sync.WaitGroup{},
	}
}

func (f *SkipMask) String() string {
	return fmt.Sprintf("%s_%d_%d", f.params.Query, f.params.From, f.params.To)
}

func (f *SkipMask) Hash() string {
	if f.hash == "" {
		h := sha256.New()
		h.Write([]byte(f.String()))
		bs := h.Sum(nil)
		f.hash = hex.EncodeToString(bs)
	}
	return f.hash
}

func (f *SkipMask) setStatus(status SkipMaskStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.status = status
}

func (f *SkipMask) getStatus() SkipMaskStatus {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.status
}
