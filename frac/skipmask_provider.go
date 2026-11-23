package frac

import (
	"github.com/RoaringBitmap/roaring/v2"

	"github.com/ozontech/seq-db/node"
)

type SkipMaskProvider interface {
	GetIDsIteratorByFrac(fracName string, minLID, maxLID uint32, reverse bool) (node.Node, bool, func() error, error)
	GetIDsBitmapByFrac(fracName string, minLID, maxLID uint32) (*roaring.Bitmap, error)
	RemoveFrac(fracName string)
}
