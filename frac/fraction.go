package frac

import (
	"context"
	"fmt"
	"time"

	"github.com/ozontech/seq-db/consts"
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

func fracToString(f Fraction, fracType string) string {
	info := f.Info()
	s := fmt.Sprintf(
		"%s fraction name=%s, creation time=%s, from=%s, to=%s, %s",
		fracType,
		info.Name(),
		time.UnixMilli(int64(info.CreationTime)).Format(consts.ESTimeFormat),
		info.From,
		info.To,
		info.String(),
	)
	if fracType == "" {
		return s[1:]
	}
	return s
}
