package docsfilter

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/ozontech/seq-db/parser"
)

type FilterStatus byte

const (
	StatusCreated FilterStatus = iota
	StatusInProgress
	StatusDone
	StatusError
)

type Params struct {
	Query string
	From  int64
	To    int64
}

type Filter struct {
	params Params

	status FilterStatus

	ast parser.SeqQLQuery

	hash    string
	dirPath string

	processWg *sync.WaitGroup
}

func NewFilter(params Params) *Filter {
	return &Filter{
		params:    params,
		status:    StatusCreated,
		processWg: &sync.WaitGroup{},
	}
}

func (f *Filter) String() string {
	return fmt.Sprintf("%s_%d_%d", f.params.Query, f.params.From, f.params.To)
}

func (f *Filter) Hash() string {
	if f.hash == "" {
		h := sha256.New()
		h.Write([]byte(f.String()))
		bs := h.Sum(nil)
		f.hash = hex.EncodeToString(bs)
	}
	return f.hash
}

func (f *Filter) markAsDone() {
	f.status = StatusDone
}
