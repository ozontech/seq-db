package frac

import (
	"sort"

	"github.com/ozontech/seq-db/seq"
)

type List []Fraction

func (l List) GetTotalSize() uint64 {
	size := uint64(0)
	for _, f := range l {
		size += f.FullSize()
	}
	return size
}

func (l List) GetOldestFrac() Fraction {
	if len(l) == 0 {
		return nil
	}

	byCT := l[0]
	ct := byCT.Info().CreationTime

	for i := 1; i < len(l); i++ {
		f := l[i]
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

func (l List) Sort(order seq.DocsOrder) {
	if order.IsNormal() {
		// descending order by To
		// it is not a bug: normal order is descending
		sort.Slice(l, func(i, j int) bool {
			return l[i].Info().To > l[j].Info().To
		})
	} else {
		// ascending order by From
		sort.Slice(l, func(i, j int) bool {
			return l[i].Info().From < l[j].Info().From
		})
	}
}

func (l List) FilterInRange(from, to seq.MID) List {
	res := make(List, 0)
	for _, f := range l {
		if f.IsIntersecting(from, to) {
			res = append(res, f)
		}
	}
	return res
}

func (l *List) Shift(n int) []Fraction {
	n = min(n, len(*l))
	res := (*l)[:n]
	*l = (*l)[n:]
	return res
}
