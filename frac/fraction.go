package frac

import (
	"context"

	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/seq"
)

type DataProvider interface {
	Fetch([]seq.ID) ([][]byte, error)
	Search(processor.SearchParams) (*seq.QPR, error)
}

type Fraction interface {
	Info() *Info
	IsIntersecting(from seq.MID, to seq.MID) bool
	Contains(mid seq.MID) bool
	DataProvider(context.Context) (DataProvider, func())
	Suicide()
}
