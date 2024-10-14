package fracmanager

import (
	"github.com/ozontech/seq-db/frac"
)

type FracsList []frac.Fraction

func (fl FracsList) FindByName(name string) frac.Fraction {
	// fractions with newer docs are more likely to be at the end of the list
	for i := len(fl) - 1; i >= 0; i-- {
		if fl[i].Info().Name() == name {
			return fl[i]
		}
	}
	return nil
}

func (fl FracsList) getTotalSize() uint64 {
	size := uint64(0)
	for _, f := range fl {
		size += f.FullSize()
	}
	return size
}

func (fl FracsList) getOldestFrac() frac.Fraction {
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
