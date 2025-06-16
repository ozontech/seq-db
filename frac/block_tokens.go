package frac

import (
	"math"

	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/packer"
)

type BlockTokens struct {
	field            string
	isStartOfField   bool
	totalSizeOfField int
	startTID         uint32
	tokens           [][]byte
}

func (t *BlockTokens) createTokenTableEntry(startIndex, blockIndex uint32) *token.TableEntry {
	size := len(t.tokens)
	return &token.TableEntry{
		StartIndex: startIndex,
		StartTID:   t.startTID,
		ValCount:   uint32(size),
		BlockIndex: blockIndex,
		MaxVal:     string(t.tokens[size-1]),
	}
}

func (t *BlockTokens) pack(p *packer.BytesPacker) {
	for _, token := range t.tokens {
		p.PutUint32(uint32(len(token)))
		p.PutBytes(token)
	}
	p.PutUint32(math.MaxUint32)
}
