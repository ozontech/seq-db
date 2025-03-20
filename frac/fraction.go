package frac

import (
	"context"

	"github.com/ozontech/seq-db/frac/searcher"
	"github.com/ozontech/seq-db/seq"
)

type SkipIndex interface {
	IsIntersecting(from seq.MID, to seq.MID) bool
	Contains(mid seq.MID) bool
}

type DataProvider interface {
	Fetch([]seq.ID) ([][]byte, error)
	Search(context.Context, searcher.Params) (*seq.QPR, error)
}

type Fraction interface {
	SkipIndex

	Info() *Info
	DataProvider(ctx context.Context) (DataProvider, func())
	FullSize() uint64
	Suicide()
}
