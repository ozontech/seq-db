package fetch

import (
	"context"
	"fmt"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

func fetchWithRecover(f frac.Fraction, id seq.ID, docsBuf []byte, midCache, ridCache *frac.UnpackCache) (_, dBuf []byte, err error) {
	defer func() {
		if panicData := recover(); panicData != nil {
			// prevent to return nil instead buf after panic
			dBuf = docsBuf
			err = fmt.Errorf("internal error: fetch panicked on fraction %s: %s", f.Info().Name(), panicData)

			util.Recover(metric.StorePanics, err)
		}
	}()

	dp, release, ok := f.DataProvider(context.Background())
	if !ok {
		return nil, docsBuf, nil
	}
	defer release()
	return dp.Fetch(id, docsBuf, midCache, ridCache)
}
