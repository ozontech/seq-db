package main

import (
	"fmt"
	"io"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/compaction"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/storage"
)

func runMerge(out string, names []string, w io.Writer) error {
	var srcs []compaction.Source
	for _, name := range names {
		f := frac.NewSealed(
			name,
			storage.NewReadLimiter(1, nil),
			frac.NewIndexCache(),
			cache.NewConcurrentCache[[]byte](nil, nil),
			nil,
			&frac.Config{},
			stubSkipMaskProvider{},
		)
		defer f.Release()
		srcs = append(srcs, frac.NewSealedSource(f))
	}

	params := common.SealParams{
		IDsZstdLevel:           3,
		LIDsZstdLevel:          3,
		TokenListZstdLevel:     3,
		DocsPositionsZstdLevel: 3,
		TokenTableZstdLevel:    3,
		LIDBlockSize:           consts.DefaultLIDBlockCap,
		TokenBlockSize:         consts.RegularBlockSize,
		LIDsBitmapThreshold:    consts.DefaultLIDBlockCap,
	}

	preloaded, err := compaction.Merge(out, params, srcs...)
	if err != nil {
		return err
	}

	info := preloaded.Info
	_, err = fmt.Fprintf(w, "merged %d fractions into %s: ver=%d docs=%d index=%d\n",
		len(names), out, info.BinaryDataVer, info.DocsTotal, info.IndexOnDisk)
	return err
}
