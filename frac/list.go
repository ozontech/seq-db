package frac

import (
	"sort"

	"github.com/ozontech/seq-db/seq"
)

type List []Fraction

func (fl List) GetTotalSize() uint64 {
	size := uint64(0)
	for _, f := range fl {
		size += f.FullSize()
	}
	return size
}

func (fl List) GetOldestFrac() Fraction {
	if len(fl) == 0 {
		return nil
	}

	byCT := fl[0]
	ct := byCT.Info().CreationTime

	for i := 1; i < len(fl); i++ {
		f := fl[i]
		info := f.Info()
		if ct > info.CreationTime {
			byCT = f
			ct = info.CreationTime
		}
	}

	if ct == 0 {
		byCT = nil
	}

	return byCT
}

func (fl List) Sort(order seq.DocsOrder) {
	if order.IsReverse() {
		sort.Slice(fl, func(i, j int) bool { // ascending order by From
			return fl[i].Info().From < fl[j].Info().From
		})
	} else {
		sort.Slice(fl, func(i, j int) bool { // descending order by To
			return fl[i].Info().To > fl[j].Info().To
		})
	}
}

func (fl List) FilterInRange(from, to seq.MID) List {
	fracs := make(List, 0)
	for _, f := range fl {
		if f.IsIntersecting(from, to) {
			fracs = append(fracs, f)
		}
	}
	return fracs
}

func (fl *List) Pop(n int) []Fraction {
	n = min(n, len(*fl))
	res := (*fl)[:n]
	*fl = (*fl)[n:]
	return res
}
