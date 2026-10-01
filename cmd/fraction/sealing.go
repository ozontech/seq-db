package main

import (
	"fmt"
	"io"
	"os"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/alecthomas/units"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/sealing"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/tokenizer"
)

const (
	docsCompressLevel = 3
	metaCompressLevel = -1

	defaultDocsPerDocBlock = 1000
)

// sealFraction reads JSON documents (one per line) from r, indexes them
// through the same active-fraction pipeline the store uses, and seals the
// result into the fraction files at <frac>. Used to produce fractions for
// decoding in tests and local debugging.
func sealFraction(
	fracName, mappingPath string,
	r io.Reader,
	docsPerDocBlock int,
) error {
	mapping, err := getMapping(mappingPath)
	if err != nil {
		return fmt.Errorf("error while getting mapping: %w", err)
	}

	tokenizers := map[seq.TokenizerType]tokenizer.Tokenizer{
		seq.TokenizerTypeKeyword: tokenizer.NewKeywordTokenizer(20, false, true),
		seq.TokenizerTypeText:    tokenizer.NewTextTokenizer(20, false, true, 100),
		seq.TokenizerTypePath:    tokenizer.NewPathTokenizer(512, false, true),
		seq.TokenizerTypeExists:  tokenizer.NewExistsTokenizer(),
	}

	docs, err := newDocReader(r, mapping, tokenizers, docsPerDocBlock)
	if err != nil {
		return fmt.Errorf("cannot read docs: %w", err)
	}

	activeIndexer, stopIndexer := frac.NewActiveIndexer(4, 10)
	defer stopIndexer()

	base := fracName
	active := frac.NewActive(
		base,
		activeIndexer,
		storage.NewReadLimiter(1, nil),
		cache.NewConcurrentCache[[]byte](nil, nil),
		cache.NewConcurrentCache[[]byte](nil, nil),
		&frac.Config{
			SkipSortDocs: true,
		},
		stubSkipMaskProvider{},
	)
	defer active.Release()

	if err := docs.readDocs(active); err != nil {
		return fmt.Errorf("cannot read docs: %w", err)
	}

	sealParams := common.SealParams{
		IDsZstdLevel:           1,
		LIDsZstdLevel:          1,
		TokenListZstdLevel:     1,
		DocsPositionsZstdLevel: 1,
		TokenTableZstdLevel:    1,
		DocBlocksZstdLevel:     1,
		LIDBlockSize:           256,
		TokenBlockSize:         128,
		DocBlockSize:           128 * int(units.KiB),
		LIDsBitmapThreshold:    25,
	}

	src, err := frac.NewActiveSealingSource(active, sealParams)
	if err != nil {
		return fmt.Errorf("cannot create sealing source: %w", err)
	}

	if _, err := sealing.Seal(src, sealParams); err != nil {
		return fmt.Errorf("cannot seal fraction: %w", err)
	}

	return nil
}

// stubSkipMaskProvider is a no-op provider used because the tool works
// outside the skip mask manager lifecycle.
type stubSkipMaskProvider struct{}

func (stubSkipMaskProvider) GetIDsIteratorByFrac(_ string, _, _ uint32, reverse bool) (node.Node, bool, func() error, error) {
	return node.NewStatic(nil, reverse), false, func() error { return nil }, nil
}

func (stubSkipMaskProvider) GetIDsBitmapByFrac(_ string, _, _ uint32) (*roaring.Bitmap, error) {
	return nil, nil
}

func (stubSkipMaskProvider) RemoveFrac(_ string) {}

func getMapping(path string) (seq.Mapping, error) {
	if path == "" {
		return defaultMapping(), nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read mapping file: %w", err)
	}

	return seq.ReadMapping(data)
}

func defaultMapping() seq.Mapping {
	return seq.Mapping{
		"level":   seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"service": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"message": seq.NewSingleType(seq.TokenizerTypeText, "", 0),
	}
}
