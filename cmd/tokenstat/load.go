package main

import (
	"github.com/RoaringBitmap/roaring/v2"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/storage"
)

type tokenEntry struct {
	field string
	value string
	freq  uint32
	class tokenClass
}

type fraction struct {
	docsTotal uint32
	tokens    []tokenEntry
}

func loadFraction(name string) (*fraction, error) {
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

	res := &fraction{docsTotal: f.Info().DocsTotal}
	for field, postings := range frac.NewSealedSource(f).TokenTriplets() {
		for triplet, err := range postings {
			if err != nil {
				return nil, err
			}
			value := string(triplet.First)
			res.tokens = append(res.tokens, tokenEntry{
				field: field,
				value: value,
				freq:  uint32(len(triplet.Second)),
				class: classify(value),
			})
		}
	}

	return res, nil
}

type stubSkipMaskProvider struct{}

func (stubSkipMaskProvider) GetIDsIteratorByFrac(_ string, _, _ uint32, reverse bool) (node.Node, bool, func() error, error) {
	return node.NewStatic(nil, reverse), false, func() error { return nil }, nil
}

func (stubSkipMaskProvider) GetIDsBitmapByFrac(_ string, _, _ uint32) (*roaring.Bitmap, error) {
	return nil, nil
}

func (stubSkipMaskProvider) RemoveFrac(_ string) {}
