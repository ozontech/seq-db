package frac

import (
	"context"
	"fmt"
	"time"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/seq"
)

type Fraction interface {
	Info() *common.Info
	IsIntersecting(from seq.MID, to seq.MID) bool
	Contains(mid seq.MID) bool
	Fetch(context.Context, []seq.ID) ([][]byte, error)
	Search(context.Context, processor.SearchParams) (*seq.QPR, error)
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
